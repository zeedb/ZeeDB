use std::collections::HashMap;

use ast::*;
use catalog_types::CATALOG_KEY;
use chrono::{NaiveDate, TimeZone, Utc};
use context::Context;
use parser::{Parser, PARSER_KEY};

use crate::{unnest::unnest_dependent_joins, BootstrapCatalog};

pub fn rewrite(expr: Expr) -> Expr {
    let expr = top_down_rewrite(expr, rewrite_ddl);
    let expr = top_down_rewrite(expr, rewrite_with);
    let expr = top_down_rewrite(expr, rewrite_scalars);
    let expr = bottom_up_rewrite(expr, unnest_dependent_joins);
    let expr = top_down_rewrite(expr, push_down_predicates);
    let expr = bottom_up_rewrite(expr, remove_dependent_join);
    let expr = bottom_up_rewrite(expr, optimize_join_type);
    let expr = top_down_rewrite(expr, push_down_projections);
    let expr = top_down_rewrite(expr, rewrite_logical_rewrite);
    expr
}

fn rewrite_ddl(expr: Expr) -> Result<Expr, Expr> {
    match expr {
        LogicalCreateDatabase { name } => {
            let mut lines = vec![];
            lines.push(format!(
                "call set_var('parent_catalog_id', {:?});",
                name.catalog_id
            ));
            for catalog_name in &name.path[0..name.path.len() - 1] {
                lines.push(format!("call set_var('parent_catalog_id', (select catalog_id from metadata.catalog where catalog_name = {:?} and parent_catalog_id = get_var('parent_catalog_id')));", catalog_name));
            }
            lines.push("call set_var('catalog_sequence_id', (select sequence_id from metadata.sequence where sequence_name = 'catalog'));".to_string());
            lines
                .push("call set_var('next_catalog_id', metadata.next_val(get_var('catalog_sequence_id')));".to_string());
            lines.push(format!("insert into metadata.catalog (parent_catalog_id, catalog_id, catalog_name) values (get_var('parent_catalog_id'), get_var('next_catalog_id'), {:?});", name.path.last().unwrap()));
            Ok(LogicalRewrite {
                sql: lines.join("\n"),
            })
        }
        LogicalCreateTable { name, columns } => {
            let mut lines = vec![];
            lines.push(format!(
                "call set_var('catalog_id', {:?});",
                name.catalog_id
            ));
            for catalog_name in &name.path[0..name.path.len() - 1] {
                lines.push(format!("call set_var('catalog_id', (select catalog_id from metadata.catalog where catalog_name = {:?} and parent_catalog_id = get_var('catalog_id')));", catalog_name));
            }
            lines.push("call set_var('table_sequence_id', (select sequence_id from metadata.sequence where sequence_name = 'table'));".to_string());
            lines.push(
                "call set_var('next_table_id', metadata.next_val(get_var('table_sequence_id')));"
                    .to_string(),
            );
            lines.push(format!("insert into metadata.table (catalog_id, table_id, table_name) values (get_var('catalog_id'), get_var('next_table_id'), {:?});", name.path.last().unwrap()));
            for (column_id, (column_name, column_type)) in columns.iter().enumerate() {
                let column_type = column_type.to_string();
                lines.push(format!("insert into metadata.column (table_id, column_id, column_name, column_type) values (get_var('next_table_id'), {:?}, {:?}, {:?});", column_id, column_name, column_type));
            }
            lines.push("call metadata.create_table(get_var('next_table_id'));".to_string());
            Ok(LogicalRewrite {
                sql: lines.join("\n"),
            })
        }
        LogicalCreateIndex {
            name,
            table,
            columns,
        } => {
            let mut lines = vec![];
            lines.push(format!(
                "call set_var('index_catalog_id', {:?});",
                name.catalog_id
            ));
            for catalog_name in &name.path[0..name.path.len() - 1] {
                lines.push(format!("call set_var('index_catalog_id', (select catalog_id from metadata.catalog where catalog_name = {:?} and parent_catalog_id = get_var('index_catalog_id')));", catalog_name));
            }
            lines.push(format!(
                "call set_var('table_catalog_id', {:?});",
                table.catalog_id
            ));
            for catalog_name in &table.path[0..table.path.len() - 1] {
                lines.push(format!("call set_var('table_catalog_id', (select catalog_id from metadata.catalog where catalog_name = {:?} and parent_catalog_id = get_var('table_catalog_id')));", catalog_name));
            }
            lines.push("call set_var('index_sequence_id', (select sequence_id from metadata.sequence where sequence_name = 'index'));".to_string());
            lines.push(format!("call set_var('table_id', (select table_id from metadata.table where catalog_id = get_var('table_catalog_id') and table_name = {:?}));", table.path.last().unwrap()));
            lines.push(
                "call set_var('next_index_id', metadata.next_val(get_var('index_sequence_id')));"
                    .to_string(),
            );
            lines.push(format!("insert into metadata.index (catalog_id, index_id, table_id, index_name) values (get_var('index_catalog_id'), get_var('next_index_id'), get_var('table_id'), {:?});", name.path.last().unwrap()));
            for (index_order, column_name) in columns.iter().enumerate() {
                lines.push(format!("call set_var('column_id', (select column_id from metadata.column where table_id = get_var('table_id') and column_name = {:?}));", column_name));
                lines.push(format!("insert into metadata.index_column (index_id, column_id, index_order) values (get_var('next_index_id'), get_var('column_id'), {:?});", index_order));
            }
            lines.push(
                format!("assert (select metadata.is_empty(get_var('table_id'))) as 'Cannot create index because table {} is not empty.';", table.path.last().unwrap().escape_debug()),
            );
            lines.push("call metadata.create_index(get_var('next_index_id'));".to_string());
            let sql = lines.join("\n");
            Ok(LogicalRewrite { sql })
        }
        LogicalDrop { object, name } => {
            let mut lines = vec![];
            match object {
                ObjectType::Database => {
                    lines.push(format!(
                        "call set_var('catalog_id', {:?});",
                        name.catalog_id
                    ));
                    for catalog_name in &name.path[0..name.path.len()] {
                        lines.push(format!("call set_var('catalog_id', (select catalog_id from metadata.catalog where catalog_name = {:?} and parent_catalog_id = get_var('catalog_id')));", catalog_name));
                    }
                    lines.push("call metadata.drop_table((select table_id from metadata.table where catalog_id = get_var('catalog_id')));".to_string());
                    lines.push("delete from metadata.column where table_id in (select table_id from metadata.table where catalog_id = get_var('catalog_id'));".to_string());
                    lines.push(
                        "delete from metadata.table where catalog_id = get_var('catalog_id');"
                            .to_string(),
                    );
                    lines.push(
                        "delete from metadata.catalog where catalog_id = get_var('catalog_id');"
                            .to_string(),
                    );
                }
                ObjectType::Table => {
                    lines.push(format!(
                        "call set_var('catalog_id', {:?});",
                        name.catalog_id
                    ));
                    for catalog_name in &name.path[0..name.path.len() - 1] {
                        lines.push(format!("call set_var('catalog_id', (select catalog_id from metadata.catalog where catalog_name = {:?} and parent_catalog_id = get_var('catalog_id')));", catalog_name));
                    }
                    let table_name = name.path.last().unwrap();
                    lines.push(format!("call set_var('table_id', (select table_id from metadata.table where table_name = {:?} and catalog_id = get_var('catalog_id')));", table_name));
                    lines.push(
                        "delete from metadata.table where table_id = get_var('table_id');"
                            .to_string(),
                    );
                    lines.push("call metadata.drop_table(get_var('table_id'));".to_string());
                }
                ObjectType::Index => {
                    lines.push(format!(
                        "call set_var('catalog_id', {:?});",
                        name.catalog_id
                    ));
                    for catalog_name in &name.path[0..name.path.len() - 1] {
                        lines.push(format!("call set_var('catalog_id', (select catalog_id from metadata.catalog where catalog_name = {:?} and parent_catalog_id = get_var('catalog_id')));", catalog_name));
                    }
                    let index_name = name.path.last().unwrap();
                    lines.push(format!("call set_var('index_id', (select index_id from metadata.index where index_name = {:?} and catalog_id = get_var('catalog_id')));", index_name));
                    lines.push(
                        "delete from metadata.index where index_id = get_var('index_id');"
                            .to_string(),
                    );
                    lines.push("call metadata.drop_index(get_var('index_id'));".to_string());
                }
                ObjectType::Column => todo!(),
            };
            Ok(LogicalRewrite {
                sql: lines.join("\n"),
            })
        }
        LogicalUpdate {
            table,
            tid,
            input,
            columns,
        } => Ok(LogicalInsert {
            table: table.clone(),
            input: Box::new(LogicalDelete { table, tid, input }),
            columns,
        }),
        _ => Err(expr),
    }
}

fn rewrite_with(expr: Expr) -> Result<Expr, Expr> {
    match expr {
        LogicalWith {
            name,
            columns,
            left,
            right,
        } => Ok(LogicalScript {
            statements: vec![
                LogicalCreateTempTable {
                    name: name,
                    columns,
                    input: left,
                },
                *right,
            ],
        }),
        _ => Err(expr),
    }
}

pub fn rewrite_scalars(mut expr: Expr) -> Result<Expr, Expr> {
    fn visit(scalar: &mut Scalar) -> bool {
        let mut did_rewrite = false;
        if let Scalar::Call(f) = scalar {
            match f.as_mut() {
                F::CurrentDate => {
                    *scalar = Scalar::Literal(Value::Date(Some(current_date())));
                    did_rewrite = true;
                }
                F::CurrentTimestamp => {
                    *scalar = Scalar::Literal(Value::Timestamp(Some(current_timestamp())));
                    did_rewrite = true;
                }
                _ => {
                    for i in 0..scalar.len() {
                        did_rewrite = did_rewrite || visit(&mut scalar[i])
                    }
                }
            }
        }
        did_rewrite
    }
    let visit_predicates = |predicates: &mut Vec<Scalar>| {
        let mut did_rewrite = false;
        for p in predicates {
            did_rewrite = did_rewrite || visit(p)
        }
        did_rewrite
    };
    let visit_projects = |projects: &mut Vec<(Scalar, Column)>| {
        let mut did_rewrite = false;
        for (p, _) in projects {
            did_rewrite = did_rewrite || visit(p)
        }
        did_rewrite
    };
    let visit_join = |join: &mut Join| match join {
        Join::Inner(predicates)
        | Join::Right(predicates)
        | Join::Outer(predicates)
        | Join::Semi(predicates)
        | Join::Anti(predicates)
        | Join::Single(predicates)
        | Join::Mark(_, predicates) => visit_predicates(predicates),
    };
    let visit_procedure = |procedure: &mut Procedure| match procedure {
        Procedure::CreateTable(x)
        | Procedure::DropTable(x)
        | Procedure::CreateIndex(x)
        | Procedure::DropIndex(x)
        | Procedure::Assert(x, _) => visit(x),
        Procedure::SetVar(x, y) => visit(x) || visit(y),
    };
    let did_rewrite = match &mut expr {
        Expr::LogicalGet { predicates, .. }
        | Expr::LogicalFilter { predicates, .. }
        | Expr::LogicalDependentJoin { predicates, .. } => visit_predicates(predicates),
        Expr::LogicalMap { projects, .. } => visit_projects(projects),
        Expr::LogicalJoin { join, .. } => visit_join(join),
        Expr::LogicalAssign { value, .. } => visit(value),
        Expr::LogicalCall { procedure, .. } => visit_procedure(procedure),
        Expr::Leaf { .. }
        | Expr::LogicalSingleGet { .. }
        | Expr::LogicalOut { .. }
        | Expr::LogicalWith { .. }
        | Expr::LogicalCreateTempTable { .. }
        | Expr::LogicalGetWith { .. }
        | Expr::LogicalAggregate { .. }
        | Expr::LogicalLimit { .. }
        | Expr::LogicalSort { .. }
        | Expr::LogicalUnion { .. }
        | Expr::LogicalInsert { .. }
        | Expr::LogicalValues { .. }
        | Expr::LogicalUpdate { .. }
        | Expr::LogicalDelete { .. }
        | Expr::LogicalCreateDatabase { .. }
        | Expr::LogicalCreateTable { .. }
        | Expr::LogicalCreateIndex { .. }
        | Expr::LogicalDrop { .. }
        | Expr::LogicalScript { .. }
        | Expr::LogicalExplain { .. }
        | Expr::LogicalRewrite { .. } => false,
        Expr::TableFreeScan
        | Expr::SeqScan { .. }
        | Expr::IndexScan { .. }
        | Expr::Filter { .. }
        | Expr::Out { .. }
        | Expr::Map { .. }
        | Expr::NestedLoop { .. }
        | Expr::HashJoin { .. }
        | Expr::CreateTempTable { .. }
        | Expr::GetTempTable { .. }
        | Expr::Aggregate { .. }
        | Expr::Limit { .. }
        | Expr::Sort { .. }
        | Expr::Union { .. }
        | Expr::Broadcast { .. }
        | Expr::Exchange { .. }
        | Expr::Insert { .. }
        | Expr::Values { .. }
        | Expr::Delete { .. }
        | Expr::Script { .. }
        | Expr::Assign { .. }
        | Expr::Call { .. }
        | Expr::Explain { .. } => panic!(
            "rewrite_scalars is not implemented for physical operator {}",
            expr.name()
        ),
    };
    if did_rewrite {
        Ok(expr)
    } else {
        Err(expr)
    }
}

fn optimize_join_type(expr: Expr) -> Result<Expr, Expr> {
    match expr {
        LogicalFilter {
            predicates: filter_predicates,
            input,
        } => {
            if let LogicalJoin {
                join: Join::Mark(mark, join_predicates),
                left,
                right,
            } = *input
            {
                // Try to turn mark-join into semi-join or anti-join.
                let semi = Scalar::Column(mark.clone());
                let anti = Scalar::Call(Box::new(F::Not(semi.clone())));
                let mut combined_attributes = vec![];
                for c in &right.attributes() {
                    combined_attributes.push((Scalar::Column(c.clone()), c.clone()));
                }
                combined_attributes.push((Scalar::Literal(Value::Bool(Some(true))), mark.clone()));
                combined_attributes.sort_by(|(_, a), (_, b)| a.cmp(b));
                for i in 0..filter_predicates.len() {
                    // WHERE $mark can be turned into a semi-join.
                    if filter_predicates[i] == semi {
                        let mut remaining_predicates = filter_predicates;
                        remaining_predicates.remove(i);
                        return Ok(maybe_filter(
                            remaining_predicates,
                            LogicalMap {
                                include_existing: false,
                                projects: combined_attributes,
                                input: Box::new(LogicalJoin {
                                    join: Join::Semi(join_predicates),
                                    left,
                                    right,
                                }),
                            },
                        ));
                    // WHERE NOT $mark can be turned into an anti-join.
                    } else if filter_predicates[i] == anti {
                        let mut remaining_predicates = filter_predicates;
                        remaining_predicates.remove(i);
                        return Ok(maybe_filter(
                            remaining_predicates,
                            LogicalMap {
                                include_existing: false,
                                projects: combined_attributes,
                                input: Box::new(LogicalJoin {
                                    join: Join::Anti(join_predicates),
                                    left,
                                    right,
                                }),
                            },
                        ));
                    }
                }
                // Give up, re-assemble expr.
                Err(LogicalFilter {
                    predicates: filter_predicates,
                    input: Box::new(LogicalJoin {
                        join: Join::Mark(mark, join_predicates),
                        left,
                        right,
                    }),
                })
            } else {
                Err(LogicalFilter {
                    predicates: filter_predicates,
                    input,
                })
            }
        }
        LogicalJoin {
            join: Join::Single(join_predicates),
            left,
            right,
        } if join_predicates.is_empty() && prove_singleton(left.as_ref()) => Ok(LogicalJoin {
            join: Join::Inner(vec![]),
            left,
            right,
        }),
        LogicalJoin {
            join: Join::Inner(join_predicates),
            left,
            right,
        } => {
            if let Some(projects) = is_table_free_scan(left.as_ref()) {
                Ok(maybe_filter(join_predicates, maybe_map(projects, *right)))
            } else if let Some(projects) = is_table_free_scan(right.as_ref()) {
                Ok(maybe_filter(join_predicates, maybe_map(projects, *left)))
            } else {
                Err(LogicalJoin {
                    join: Join::Inner(join_predicates),
                    left,
                    right,
                })
            }
        }
        _ => Err(expr),
    }
}

fn remove_dependent_join(expr: Expr) -> Result<Expr, Expr> {
    match expr {
        LogicalDependentJoin {
            parameters,
            predicates,
            subquery,
            domain,
        } => {
            assert!(!parameters.is_empty());
            // Check if predicates contains subquery.a = domain.b for every b in domain.
            let subquery_scope = subquery.attributes();
            // TODO this checks for predicates that have been pushed down into the dependent join.
            // The actual rules is more general: anytime there is a condition a=b higher in the tree,
            // it implies an equivalence class a~b, which means we can replace
            //   LogicalJoin [a] subquery domain
            // with
            //   Map a:b subquery
            let match_equals = |x: &Scalar| -> Option<(Column, Column)> {
                if let Scalar::Call(function) = x {
                    if let F::Equal(Scalar::Column(left), Scalar::Column(right)) = function.as_ref()
                    {
                        if subquery_scope.contains(left) && parameters.contains(right) {
                            return Some((left.clone(), right.clone()));
                        } else if subquery_scope.contains(right) && parameters.contains(left) {
                            return Some((right.clone(), left.clone()));
                        }
                    }
                }
                None
            };
            let mut equiv_predicates = HashMap::new();
            let mut filter_predicates = vec![];
            for p in &predicates {
                if let Some((subquery_column, domain_column)) = match_equals(p) {
                    filter_predicates.push(Scalar::Call(Box::new(F::Not(Scalar::Call(Box::new(
                        F::IsNull(Scalar::Column(subquery_column.clone())),
                    ))))));
                    equiv_predicates.insert(domain_column, subquery_column);
                } else {
                    filter_predicates.push(p.clone())
                }
            }
            // If we can't remove the dependent join, turn it into an inner join.
            // Note that in principal, the inner join can be faster if it prunes the subquery early in the query plan.
            // Ideally we would consider both possibilities in the search phase.
            // However, for the time being, we are simply using a heuristic that eliminating the dependent join is probably better.
            if !parameters.iter().all(|c| equiv_predicates.contains_key(c)) {
                return Ok(LogicalJoin {
                    join: Join::Inner(predicates),
                    left: subquery,
                    right: Box::new(LogicalAggregate {
                        group_by: parameters,
                        aggregate: vec![],
                        input: domain,
                    }),
                });
            }
            // Infer domain from subquery using equivalences from the join condition.
            let project_domain: Vec<(Scalar, Column)> = parameters
                .iter()
                .map(|c| (Scalar::Column(equiv_predicates[c].clone()), c.clone()))
                .collect();
            Ok(maybe_filter(
                filter_predicates,
                LogicalMap {
                    include_existing: true,
                    projects: project_domain,
                    input: subquery,
                },
            ))
        }
        _ => Err(expr),
    }
}

fn push_down_predicates(expr: Expr) -> Result<Expr, Expr> {
    match expr {
        LogicalFilter {
            predicates: filter_predicates,
            input,
        } => match *input {
            LogicalJoin { join, left, right } => match join {
                Join::Inner(join_predicates) => Ok(LogicalJoin {
                    join: Join::Inner(combine_predicates(join_predicates, filter_predicates)),
                    left,
                    right,
                }),
                Join::Semi(join_predicates) => Ok(LogicalJoin {
                    join: Join::Semi(combine_predicates(join_predicates, filter_predicates)),
                    left,
                    right,
                }),
                Join::Right(_)
                | Join::Outer(_)
                | Join::Anti(_)
                | Join::Single(_)
                | Join::Mark(_, _) => {
                    let (correlated, uncorrelated) =
                        correlated_predicates(&filter_predicates, right.as_ref());
                    if !uncorrelated.is_empty() {
                        Ok(maybe_filter(
                            correlated,
                            LogicalJoin {
                                join,
                                left,
                                right: Box::new(LogicalFilter {
                                    predicates: uncorrelated,
                                    input: right,
                                }),
                            },
                        ))
                    } else {
                        Err(LogicalFilter {
                            predicates: filter_predicates,
                            input: Box::new(LogicalJoin { join, left, right }),
                        })
                    }
                }
            },
            LogicalDependentJoin {
                parameters,
                predicates: join_predicates,
                subquery,
                domain,
            } => Ok(LogicalDependentJoin {
                parameters,
                predicates: combine_predicates(join_predicates, filter_predicates),
                subquery,
                domain,
            }),
            LogicalMap {
                include_existing,
                projects,
                input,
            } => {
                let (correlated, uncorrelated) =
                    correlated_predicates(&filter_predicates, input.as_ref());
                if !uncorrelated.is_empty() {
                    Ok(maybe_filter(
                        correlated,
                        LogicalMap {
                            include_existing,
                            projects,
                            input: Box::new(LogicalFilter {
                                predicates: uncorrelated,
                                input,
                            }),
                        },
                    ))
                } else {
                    Err(LogicalFilter {
                        predicates: filter_predicates,
                        input: Box::new(LogicalMap {
                            include_existing,
                            projects,
                            input,
                        }),
                    })
                }
            }
            LogicalFilter {
                predicates: inner_predicates,
                input,
            } => Ok(LogicalFilter {
                predicates: combine_predicates(filter_predicates, inner_predicates),
                input,
            }),
            LogicalGet {
                projects,
                predicates: get_predicates,
                table,
            } => Ok(LogicalGet {
                projects,
                predicates: combine_predicates(filter_predicates, get_predicates),
                table,
            }),
            _ => Err(LogicalFilter {
                predicates: filter_predicates,
                input,
            }),
        },
        LogicalJoin { join, left, right } => {
            match join {
                // Push predicates down both sides of inner join.
                Join::Inner(_) | Join::Semi(_) => {
                    // Try to push down left.
                    let (correlated, uncorrelated) =
                        correlated_predicates(join.predicates(), left.as_ref());
                    if !uncorrelated.is_empty() {
                        return Ok(LogicalJoin {
                            join: join.replace(correlated),
                            left: Box::new(LogicalFilter {
                                predicates: uncorrelated,
                                input: left,
                            }),
                            right,
                        });
                    }
                    // Try to push down right.
                    let (correlated, uncorrelated) =
                        correlated_predicates(join.predicates(), right.as_ref());
                    if !uncorrelated.is_empty() {
                        return Ok(LogicalJoin {
                            join: join.replace(correlated),
                            left,
                            right: Box::new(LogicalFilter {
                                predicates: uncorrelated,
                                input: right,
                            }),
                        });
                    }
                }
                // Push predicates down right side of right join.
                Join::Right(_) | Join::Anti(_) | Join::Single(_) | Join::Mark(_, _) => {
                    // Try to push down right.
                    let (correlated, uncorrelated) =
                        correlated_predicates(join.predicates(), right.as_ref());
                    if !uncorrelated.is_empty() {
                        return Ok(LogicalJoin {
                            join: join.replace(correlated),
                            left,
                            right: Box::new(LogicalFilter {
                                predicates: uncorrelated,
                                input: right,
                            }),
                        });
                    }
                }
                Join::Outer(_) => {}
            }
            Err(LogicalJoin { join, left, right })
        }
        LogicalDependentJoin {
            parameters,
            predicates: join_predicates,
            subquery,
            domain,
        } => {
            // Try to push down subquery.
            let (correlated, uncorrelated) =
                correlated_predicates(&join_predicates, subquery.as_ref());
            if !uncorrelated.is_empty() {
                return Ok(LogicalDependentJoin {
                    parameters,
                    predicates: correlated,
                    subquery: Box::new(LogicalFilter {
                        predicates: uncorrelated,
                        input: subquery,
                    }),
                    domain,
                });
            }
            // Try to push down domain.
            let (correlated, uncorrelated) =
                correlated_predicates(&join_predicates, domain.as_ref());
            if !uncorrelated.is_empty() {
                return Ok(LogicalDependentJoin {
                    parameters,
                    predicates: correlated,
                    subquery,
                    domain: Box::new(LogicalFilter {
                        predicates: uncorrelated,
                        input: domain,
                    }),
                });
            }
            // All predicates are correlated with both sides.
            Err(LogicalDependentJoin {
                parameters,
                predicates: join_predicates,
                subquery,
                domain,
            })
        }
        _ => Err(expr),
    }
}

fn push_down_projections(expr: Expr) -> Result<Expr, Expr> {
    match expr {
        LogicalMap {
            include_existing: outer_include_existing,
            projects: outer,
            input,
        } => {
            // If map is a no-op, remove it.
            if outer.len() == input.attributes().len() && outer.iter().all(|(x, c)| x.is_just(c)) {
                return Ok(*input);
            }
            match *input {
                LogicalMap {
                    include_existing: inner_include_existing,
                    projects: inner,
                    input,
                } => Ok(LogicalMap {
                    include_existing: outer_include_existing && inner_include_existing,
                    projects: combine_projects(outer, outer_include_existing, inner),
                    input,
                }),
                LogicalGet {
                    projects: inner,
                    predicates,
                    table,
                } => {
                    let mut combined = inner.clone();
                    for (x, c) in &outer {
                        // TODO if some* mapped items can be embedded, embed them.
                        // TODO if column is renamed, that should be embedd-able, but it's not presently because we use column names as ids.
                        if !x.is_just(c) {
                            return Err(LogicalMap {
                                include_existing: outer_include_existing,
                                projects: outer,
                                input: Box::new(LogicalGet {
                                    projects: inner,
                                    predicates,
                                    table,
                                }),
                            });
                        }
                        if !combined.contains(c) {
                            combined.push(c.clone());
                        }
                    }
                    Ok(LogicalGet {
                        projects: combined,
                        predicates,
                        table,
                    })
                }
                _ => Err(LogicalMap {
                    include_existing: outer_include_existing,
                    projects: outer,
                    input,
                }),
            }
        }
        _ => Err(expr),
    }
}

fn rewrite_logical_rewrite(expr: Expr) -> Result<Expr, Expr> {
    match expr {
        LogicalRewrite { sql } => {
            let expr = analyze_bootstrap(&sql);
            let expr = rewrite(expr);
            Ok(expr)
        }
        _ => Err(expr),
    }
}

fn analyze_bootstrap(sql: &str) -> Expr {
    let mut context = Context::default();
    context.insert(PARSER_KEY, Parser::default());
    context.insert(CATALOG_KEY, Box::new(BootstrapCatalog));
    context[PARSER_KEY]
        .analyze(sql, catalog_types::ROOT_CATALOG_ID, 0, vec![], &context)
        .unwrap()
}

fn bottom_up_rewrite(mut expr: Expr, f: impl Fn(Expr) -> Result<Expr, Expr> + Copy) -> Expr {
    // First, rewrite the children.
    for i in 0..expr.len() {
        expr[i] = bottom_up_rewrite(std::mem::take(&mut expr[i]), f)
    }
    match f(expr) {
        // If f succeeded, we may have new children that need to be rewritten.
        Ok(expr) => bottom_up_rewrite(expr, f),
        // Otherwise, we have reached a fixed point.
        Err(expr) => expr,
    }
}

fn top_down_rewrite(expr: Expr, f: impl Fn(Expr) -> Result<Expr, Expr> + Copy) -> Expr {
    // First, rewrite the parent.
    match f(expr) {
        // If f succeeded, we may have a new parent that needs to be rewritten.
        Ok(expr) => top_down_rewrite(expr, f),
        // Otherwise, proceed with rewriting the children.
        Err(mut expr) => {
            for i in 0..expr.len() {
                expr[i] = top_down_rewrite(std::mem::take(&mut expr[i]), f)
            }
            expr
        }
    }
}

fn correlated_predicates(predicates: &Vec<Scalar>, input: &Expr) -> (Vec<Scalar>, Vec<Scalar>) {
    let scope = input.attributes();
    predicates
        .iter()
        .map(|p| p.clone())
        .partition(|p| !p.references().is_subset(&scope))
}

fn prove_singleton(expr: &Expr) -> bool {
    match expr {
        LogicalMap { input, .. } => prove_singleton(input),
        LogicalAggregate {
            group_by, input, ..
        } => {
            if group_by.is_empty() {
                prove_non_empty(input)
            } else {
                prove_singleton(input)
            }
        }
        LogicalSingleGet => true,
        _ => false,
    }
}

fn prove_non_empty(expr: &Expr) -> bool {
    match expr {
        LogicalMap { input, .. } | LogicalAggregate { input, .. } => prove_non_empty(input),
        LogicalSingleGet => true,
        _ => false,
    }
}

fn prove_at_most_one(expr: &Expr) -> bool {
    match expr {
        LogicalMap { input, .. } => prove_at_most_one(input),
        LogicalAggregate {
            group_by, input, ..
        } => {
            if group_by.is_empty() {
                true
            } else {
                prove_at_most_one(input)
            }
        }
        LogicalSingleGet => true,
        _ => false,
    }
}

fn is_table_free_scan(input: &Expr) -> Option<Vec<(Scalar, Column)>> {
    match input {
        LogicalMap {
            include_existing: true,
            projects: my_projects,
            input,
        } => {
            if let Some(more_projects) = is_table_free_scan(input.as_ref()) {
                let mut projects = vec![];
                projects.extend_from_slice(my_projects);
                projects.extend_from_slice(&more_projects);
                Some(projects)
            } else {
                None
            }
        }
        LogicalMap {
            include_existing: false,
            projects: my_projects,
            input,
        } => {
            if is_table_free_scan(input.as_ref()).is_some() {
                Some(my_projects.clone())
            } else {
                None
            }
        }
        LogicalSingleGet => Some(vec![]),
        _ => None,
    }
}

fn maybe_filter(predicates: Vec<Scalar>, input: Expr) -> Expr {
    if predicates.is_empty() {
        input
    } else {
        LogicalFilter {
            predicates,
            input: Box::new(input),
        }
    }
}

fn maybe_map(projects: Vec<(Scalar, Column)>, input: Expr) -> Expr {
    if projects.is_empty() {
        input
    } else {
        LogicalMap {
            include_existing: true,
            projects,
            input: Box::new(input),
        }
    }
}

fn combine_predicates(outer: Vec<Scalar>, inner: Vec<Scalar>) -> Vec<Scalar> {
    let mut combined = Vec::with_capacity(outer.len() + inner.len());
    for p in outer {
        combined.push(p);
    }
    for p in inner {
        combined.push(p);
    }
    combined
}

fn combine_projects(
    outer: Vec<(Scalar, Column)>,
    include_existing: bool,
    inner: Vec<(Scalar, Column)>,
) -> Vec<(Scalar, Column)> {
    let mut inlined = if include_existing {
        inner.clone()
    } else {
        vec![]
    };
    for (outer_expr, outer_column) in outer {
        let mut outer_expr = outer_expr;
        for (inner_expr, inner_column) in &inner {
            outer_expr = outer_expr.inline(&inner_expr, &inner_column);
        }
        inlined.push((outer_expr, outer_column));
    }
    inlined
}

fn current_timestamp() -> i64 {
    let ts = Utc::now();
    ts.timestamp() * MICROSECONDS + ts.timestamp_subsec_micros() as i64
}

fn current_date() -> i32 {
    let d = Utc::today();
    let epoch = Utc.from_utc_date(&NaiveDate::from_ymd(1970, 1, 1));
    (d - epoch).num_days() as i32
}

/// Number of microseconds in a second
const MICROSECONDS: i64 = 1_000_000;
