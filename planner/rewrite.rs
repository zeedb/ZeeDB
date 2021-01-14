use crate::unnest::unnest_dependent_joins;
use ast::*;
use chrono::{NaiveDate, TimeZone, Utc};
use std::collections::HashMap;
use zetasql::SimpleCatalogProto;

pub(crate) fn rewrite(catalog_id: i64, catalog: &SimpleCatalogProto, expr: Expr) -> Expr {
    let expr = expr.top_down_rewrite(&|e| apply_repeatedly(rewrite_ddl, e));
    let expr = rewrite_scalars(expr);
    let expr = expr.bottom_up_rewrite(&|e| apply_repeatedly(unnest_dependent_joins, e));
    let expr = expr.top_down_rewrite(&|e| apply_repeatedly(push_down_predicates, e));
    let expr = expr.bottom_up_rewrite(&|e| apply_repeatedly(remove_dependent_join, e));
    let expr = expr.bottom_up_rewrite(&|e| apply_repeatedly(optimize_join_type, e));
    let expr = expr.top_down_rewrite(&|e| apply_repeatedly(push_down_projections, e));
    let expr = rewrite_logical_rewrite(catalog_id, catalog, expr);
    expr
}

fn rewrite_ddl(expr: Expr) -> Result<Expr, Expr> {
    match expr {
        LogicalCreateDatabase { name } => {
            let mut lines = vec![];
            lines.push(format!("set parent_catalog_id = {:?};", name.catalog_id));
            for catalog_name in &name.path[0..name.path.len() - 1] {
                lines.push(format!("set parent_catalog_id = (select catalog_id from metadata.catalog where catalog_name = {:?} and parent_catalog_id = @parent_catalog_id);", catalog_name));
            }
            lines.push("set catalog_sequence_id = (select sequence_id from metadata.sequence where sequence_name = 'catalog');".to_string());
            lines
                .push("set next_catalog_id = metadata.next_val(@catalog_sequence_id);".to_string());
            lines.push(format!("insert into metadata.catalog (parent_catalog_id, catalog_id, catalog_name) values (@parent_catalog_id, @next_catalog_id, {:?});", name.path.last().unwrap()));
            Ok(LogicalRewrite {
                sql: lines.join("\n"),
            })
        }
        LogicalCreateTable { name, columns } => {
            let mut lines = vec![];
            lines.push(format!("set catalog_id = {:?};", name.catalog_id));
            for catalog_name in &name.path[0..name.path.len() - 1] {
                lines.push(format!("set catalog_id = (select catalog_id from metadata.catalog where catalog_name = {:?} and parent_catalog_id = @catalog_id);", catalog_name));
            }
            lines.push("set table_sequence_id = (select sequence_id from metadata.sequence where sequence_name = 'table');".to_string());
            lines.push("set next_table_id = metadata.next_val(@table_sequence_id);".to_string());
            lines.push(format!("insert into metadata.table (catalog_id, table_id, table_name) values (@catalog_id, @next_table_id, {:?});", name.path.last().unwrap()));
            for (column_id, (column_name, column_type)) in columns.iter().enumerate() {
                let column_type = column_type.to_string();
                lines.push(format!("insert into metadata.column (table_id, column_id, column_name, column_type) values (@next_table_id, {:?}, {:?}, {:?});", column_id, column_name, column_type));
            }
            lines.push("call metadata.create_table(@next_table_id);".to_string());
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
            lines.push(format!("set index_catalog_id = {:?};", name.catalog_id));
            for catalog_name in &name.path[0..name.path.len() - 1] {
                lines.push(format!("set index_catalog_id = (select catalog_id from metadata.catalog where catalog_name = {:?} and parent_catalog_id = @index_catalog_id);", catalog_name));
            }
            lines.push(format!("set table_catalog_id = {:?};", table.catalog_id));
            for catalog_name in &table.path[0..table.path.len() - 1] {
                lines.push(format!("set table_catalog_id = (select catalog_id from metadata.catalog where catalog_name = {:?} and parent_catalog_id = @table_catalog_id);", catalog_name));
            }
            lines.push("set index_sequence_id = (select sequence_id from metadata.sequence where sequence_name = 'index');".to_string());
            lines.push(format!("set table_id = (select table_id from metadata.table where catalog_id = @table_catalog_id and table_name = {:?});", table.path.last().unwrap()));
            lines.push("set next_index_id = metadata.next_val(@index_sequence_id);".to_string());
            lines.push(format!("insert into metadata.index (catalog_id, index_id, table_id, index_name) values (@index_catalog_id, @next_index_id, @table_id, {:?});", name.path.last().unwrap()));
            for (index_order, column_name) in columns.iter().enumerate() {
                lines.push(format!("set column_id = (select column_id from metadata.column where table_id = @table_id and column_name = {:?});", column_name));
                lines.push(format!("insert into metadata.index_column (index_id, column_id, index_order) values (@next_index_id, @column_id, {:?});", index_order));
            }
            lines.push("call metadata.create_index(@next_index_id);".to_string());
            let sql = lines.join("\n");
            Ok(LogicalRewrite { sql })
        }
        LogicalDrop { object, name } => {
            let mut lines = vec![];
            match object {
                ObjectType::Database => {
                    lines.push(format!("set catalog_id = {:?};", name.catalog_id));
                    for catalog_name in &name.path[0..name.path.len()] {
                        lines.push(format!("set catalog_id = (select catalog_id from metadata.catalog where catalog_name = {:?} and parent_catalog_id = @catalog_id);", catalog_name));
                    }
                    lines.push("call metadata.drop_table((select table_id from metadata.table where catalog_id = @catalog_id));".to_string());
                    lines.push("delete from metadata.column where table_id in (select table_id from metadata.table where catalog_id = @catalog_id);".to_string());
                    lines.push(
                        "delete from metadata.table where catalog_id = @catalog_id;".to_string(),
                    );
                    lines.push(
                        "delete from metadata.catalog where catalog_id = @catalog_id;".to_string(),
                    );
                }
                ObjectType::Table => {
                    lines.push(format!("set catalog_id = {:?};", name.catalog_id));
                    for catalog_name in &name.path[0..name.path.len() - 1] {
                        lines.push(format!("set catalog_id = (select catalog_id from metadata.catalog where catalog_name = {:?} and parent_catalog_id = @catalog_id);", catalog_name));
                    }
                    let table_name = name.path.last().unwrap();
                    lines.push(format!("set table_id = (select table_id from metadata.table where table_name = {:?} and catalog_id = @catalog_id);", table_name));
                    lines
                        .push("delete from metadata.table where table_id = @table_id;".to_string());
                    lines.push("call metadata.drop_table(@table_id);".to_string());
                }
                ObjectType::Index => {
                    lines.push(format!("set catalog_id = {:?};", name.catalog_id));
                    for catalog_name in &name.path[0..name.path.len() - 1] {
                        lines.push(format!("set catalog_id = (select catalog_id from metadata.catalog where catalog_name = {:?} and parent_catalog_id = @catalog_id);", catalog_name));
                    }
                    let index_name = name.path.last().unwrap();
                    lines.push(format!("set index_id = (select index_id from metadata.index where index_name = {:?} and catalog_id = @catalog_id);", index_name));
                    lines
                        .push("delete from metadata.index where index_id = @index_id;".to_string());
                    lines.push("call metadata.drop_index(@index_id);".to_string());
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

fn rewrite_scalars(expr: Expr) -> Expr {
    expr.map_scalar(|scalar| {
        scalar.bottom_up_rewrite(&|scalar| match scalar {
            Scalar::Call(function) => match *function {
                F::CurrentDate => Scalar::Literal(Value::Date(Some(current_date()))),
                F::CurrentTimestamp => Scalar::Literal(Value::Timestamp(Some(current_timestamp()))),
                other => Scalar::Call(Box::new(other)),
            },
            other => other,
        })
    })
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
        LogicalWith {
            name,
            columns,
            left,
            right,
        } => match count_get_with(&name, &right) {
            0 if !left.has_side_effects() => Ok(*right),
            1 => Ok(inline_with(&name, &columns, left.as_ref(), *right)),
            _ => Ok(with_to_script(name, columns, *left, *right)),
        },
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

fn rewrite_logical_rewrite(catalog_id: i64, catalog: &SimpleCatalogProto, expr: Expr) -> Expr {
    expr.bottom_up_rewrite(&|expr| match expr {
        LogicalRewrite { sql } => {
            let expr = parser::analyze(catalog_id, &catalog, &sql).expect(&sql);
            rewrite(catalog_id, catalog, expr)
        }
        other => other,
    })
}

fn apply_repeatedly(f: impl Fn(Expr) -> Result<Expr, Expr>, mut expr: Expr) -> Expr {
    loop {
        expr = match f(expr) {
            Ok(expr) => expr,
            Err(expr) => return expr,
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

fn count_get_with(name: &String, expr: &Expr) -> usize {
    match expr {
        LogicalGetWith { name: get_name, .. } => {
            if name == get_name {
                1
            } else {
                0
            }
        }
        _ if expr.is_logical() => expr.iter().map(|expr| count_get_with(name, expr)).sum(),
        _ => panic!("{} is not logical", expr),
    }
}

fn inline_with(name: &String, columns: &Vec<Column>, left: &Expr, right: Expr) -> Expr {
    match right {
        LogicalGetWith {
            name: get_name,
            columns: get_columns,
        } if name == &get_name => {
            let mut projects = vec![];
            for i in 0..columns.len() {
                projects.push((Scalar::Column(columns[i].clone()), get_columns[i].clone()))
            }
            LogicalMap {
                include_existing: false,
                projects,
                input: Box::new(left.clone()),
            }
        }
        expr => expr.map(|child| inline_with(name, columns, left, child)),
    }
}

fn with_to_script(name: String, columns: Vec<Column>, left: Expr, right: Expr) -> Expr {
    LogicalScript {
        statements: vec![
            LogicalCreateTempTable {
                name: name,
                columns,
                input: Box::new(left),
            },
            right,
        ],
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
