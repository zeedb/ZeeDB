use ast::*;
use std::collections::HashMap;
use zetasql::SimpleCatalogProto;

#[derive(Debug)]
enum RewriteRule {
    // Rewrite DDL:
    CreateDatabaseToScript,
    CreateTableToScript,
    CreateIndexToScript,
    DropToScript,
    UpdateToDeleteThenInsert,
    // Unnest meta rule:
    PushDependentJoin,
    // Unnesting implementation rules:
    PushDependentJoinThroughFilter,
    PushDependentJoinThroughMap,
    PushDependentJoinThroughJoin,
    PushDependentJoinThroughWith,
    PushDependentJoinThroughAggregate,
    PushDependentJoinThroughLimit,
    PushDependentJoinThroughSort,
    PushDependentJoinThroughSetOperation,
    // Optimize joins:
    MarkJoinToSemiJoin,
    SingleJoinToInnerJoin,
    RemoveInnerJoin,
    RemoveWith,
    RemoveTableFreeScan,
    // Predicate pushdown:
    PushExplicitFilterIntoInnerJoin,
    PushImplicitFilterThroughInnerJoin,
    PushExplicitFilterThroughOuterJoin,
    PushFilterThroughMap,
    CombineConsecutiveFilters,
    EmbedFilterIntoGet,
    // Optimize projections:
    CombineConsecutiveMaps,
    EmbedMapIntoGet,
    RemoveMap,
}

impl RewriteRule {
    fn apply(&self, expr: &Expr) -> Option<Expr> {
        match self {
            RewriteRule::CreateDatabaseToScript => {
                if let LogicalCreateDatabase { name } = expr {
                    let mut lines = vec![];
                    lines.push(format!("set parent_catalog_id = {:?};", name.catalog_id));
                    for catalog_name in &name.path[0..name.path.len() - 1] {
                        lines.push(format!("set parent_catalog_id = (select catalog_id from metadata.catalog where catalog_name = {:?} and parent_catalog_id = @parent_catalog_id);", catalog_name));
                    }
                    lines.push("set catalog_sequence_id = (select sequence_id from metadata.sequence where sequence_name = 'catalog');".to_string());
                    lines.push(
                        "set next_catalog_id = metadata.next_val(@catalog_sequence_id);"
                            .to_string(),
                    );
                    lines.push(format!("insert into metadata.catalog (parent_catalog_id, catalog_id, catalog_name) values (@parent_catalog_id, @next_catalog_id, {:?});", name.path.last().unwrap()));
                    return Some(LogicalRewrite {
                        sql: lines.join("\n"),
                    });
                }
            }
            RewriteRule::CreateTableToScript => {
                if let LogicalCreateTable { name, columns } = expr {
                    let mut lines = vec![];
                    lines.push(format!("set catalog_id = {:?};", name.catalog_id));
                    for catalog_name in &name.path[0..name.path.len() - 1] {
                        lines.push(format!("set catalog_id = (select catalog_id from metadata.catalog where catalog_name = {:?} and parent_catalog_id = @catalog_id);", catalog_name));
                    }
                    lines.push("set table_sequence_id = (select sequence_id from metadata.sequence where sequence_name = 'table');".to_string());
                    lines.push(
                        "set next_table_id = metadata.next_val(@table_sequence_id);".to_string(),
                    );
                    lines.push(format!("insert into metadata.table (catalog_id, table_id, table_name) values (@catalog_id, @next_table_id, {:?});", name.path.last().unwrap()));
                    for (column_id, (column_name, column_type)) in columns.iter().enumerate() {
                        let column_type = data_type::to_string(column_type);
                        lines.push(format!("insert into metadata.column (table_id, column_id, column_name, column_type) values (@next_table_id, {:?}, {:?}, {:?});", column_id, column_name, column_type));
                    }
                    lines.push("call metadata.create_table(@next_table_id);".to_string());
                    return Some(LogicalRewrite {
                        sql: lines.join("\n"),
                    });
                }
            }
            RewriteRule::CreateIndexToScript => {
                if let LogicalCreateIndex {
                    name,
                    table,
                    columns,
                } = expr
                {
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
                    lines.push(
                        "set next_index_id = metadata.next_val(@index_sequence_id);".to_string(),
                    );
                    lines.push(format!("insert into metadata.index (catalog_id, index_id, table_id, index_name) values (@index_catalog_id, @next_index_id, @table_id, {:?});", name.path.last().unwrap()));
                    for (index_order, column_name) in columns.iter().enumerate() {
                        lines.push(format!("set column_id = (select column_id from metadata.column where table_id = @table_id and column_name = {:?});", column_name));
                        lines.push(format!("insert into metadata.index_column (index_id, column_id, index_order) values (@next_index_id, @column_id, {:?});", index_order));
                    }
                    lines.push("call metadata.create_index(@next_index_id);".to_string());
                    let sql = lines.join("\n");
                    return Some(LogicalRewrite { sql });
                }
            }
            RewriteRule::DropToScript => {
                if let LogicalDrop { object, name } = expr {
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
                                "delete from metadata.table where catalog_id = @catalog_id;"
                                    .to_string(),
                            );
                            lines.push(
                                "delete from metadata.catalog where catalog_id = @catalog_id;"
                                    .to_string(),
                            );
                        }
                        ObjectType::Table => {
                            lines.push(format!("set catalog_id = {:?};", name.catalog_id));
                            for catalog_name in &name.path[0..name.path.len() - 1] {
                                lines.push(format!("set catalog_id = (select catalog_id from metadata.catalog where catalog_name = {:?} and parent_catalog_id = @catalog_id);", catalog_name));
                            }
                            let table_name = name.path.last().unwrap();
                            lines.push(format!("set table_id = (select table_id from metadata.table where table_name = {:?} and catalog_id = @catalog_id);", table_name));
                            lines.push(
                                "delete from metadata.table where table_id = @table_id;"
                                    .to_string(),
                            );
                            lines.push("call metadata.drop_table(@table_id);".to_string());
                        }
                        ObjectType::Index => {
                            lines.push(format!("set catalog_id = {:?};", name.catalog_id));
                            for catalog_name in &name.path[0..name.path.len() - 1] {
                                lines.push(format!("set catalog_id = (select catalog_id from metadata.catalog where catalog_name = {:?} and parent_catalog_id = @catalog_id);", catalog_name));
                            }
                            let index_name = name.path.last().unwrap();
                            lines.push(format!("set index_id = (select index_id from metadata.index where index_name = {:?} and catalog_id = @catalog_id);", index_name));
                            lines.push(
                                "delete from metadata.index where index_id = @index_id;"
                                    .to_string(),
                            );
                            lines.push("call metadata.drop_index(@index_id);".to_string());
                        }
                        ObjectType::Column => todo!(),
                    };
                    return Some(LogicalRewrite {
                        sql: lines.join("\n"),
                    });
                }
            }
            RewriteRule::UpdateToDeleteThenInsert => {
                if let LogicalUpdate { table, tid, input } = expr {
                    return Some(LogicalInsert {
                        table: table.clone(),
                        input: Box::new(LogicalDelete {
                            table: table.clone(),
                            tid: tid.clone(),
                            input: input.clone(),
                        }),
                    });
                }
            }
            RewriteRule::PushDependentJoin => {
                if let LogicalDependentJoin {
                    parameters,
                    subquery,
                    ..
                } = expr
                {
                    if free_parameters(parameters, subquery).is_empty() {
                        return None;
                    }
                    return Some(expr.clone().top_down_rewrite(&mut |expr| {
                        apply_all(
                            expr,
                            &vec![
                                RewriteRule::PushDependentJoinThroughFilter,
                                RewriteRule::PushDependentJoinThroughMap,
                                RewriteRule::PushDependentJoinThroughJoin,
                                RewriteRule::PushDependentJoinThroughWith,
                                RewriteRule::PushDependentJoinThroughAggregate,
                                RewriteRule::PushDependentJoinThroughLimit,
                                RewriteRule::PushDependentJoinThroughSort,
                                RewriteRule::PushDependentJoinThroughSetOperation,
                            ],
                        )
                    }));
                }
            }
            RewriteRule::PushDependentJoinThroughFilter => {
                if let LogicalDependentJoin {
                    parameters,
                    predicates: join_predicates,
                    subquery,
                    domain,
                } = expr
                {
                    if free_parameters(parameters, subquery).is_empty() {
                        return None;
                    }
                    if let LogicalFilter {
                        predicates: filter_predicates,
                        input: subquery,
                    } = subquery.as_ref()
                    {
                        return Some(LogicalFilter {
                            predicates: filter_predicates.clone(),
                            input: Box::new(LogicalDependentJoin {
                                parameters: parameters.clone(),
                                predicates: join_predicates.clone(),
                                subquery: subquery.clone(),
                                domain: domain.clone(),
                            }),
                        });
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughMap => {
                if let LogicalDependentJoin {
                    parameters,
                    predicates,
                    subquery,
                    domain,
                } = expr
                {
                    if free_parameters(parameters, subquery).is_empty() {
                        return None;
                    }
                    if let LogicalMap {
                        include_existing,
                        projects,
                        input: subquery,
                    } = subquery.as_ref()
                    {
                        let mut projects = projects.clone();
                        for p in parameters {
                            if !projects.iter().any(|(_, c)| c == p) {
                                projects.push((Scalar::Column(p.clone()), p.clone()));
                            }
                        }
                        return Some(LogicalMap {
                            include_existing: *include_existing,
                            projects,
                            input: Box::new(LogicalDependentJoin {
                                parameters: parameters.clone(),
                                predicates: predicates.clone(),
                                subquery: subquery.clone(),
                                domain: domain.clone(),
                            }),
                        });
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughJoin => {
                if let LogicalDependentJoin {
                    parameters,
                    predicates,
                    subquery,
                    domain,
                } = expr
                {
                    // PushExplicitFilterIntoInnerJoin hasn't yet run, so predicates should be empty.
                    // We won't bother substituting fresh column names in parameters.
                    assert!(predicates.is_empty());

                    if free_parameters(parameters, subquery).is_empty() {
                        return None;
                    }
                    if let LogicalJoin {
                        join,
                        left: left_subquery,
                        right: right_subquery,
                    } = subquery.as_ref()
                    {
                        match join {
                            Join::Inner(_) => {
                                if free_parameters(parameters, left_subquery).is_empty() {
                                    return Some(LogicalJoin {
                                        join: join.clone(),
                                        left: left_subquery.clone(),
                                        right: Box::new(LogicalDependentJoin {
                                            parameters: parameters.clone(),
                                            predicates: vec![],
                                            subquery: right_subquery.clone(),
                                            domain: domain.clone(),
                                        }),
                                    });
                                } else if free_parameters(parameters, right_subquery).is_empty() {
                                    return Some(LogicalJoin {
                                        join: join.clone(),
                                        left: Box::new(LogicalDependentJoin {
                                            parameters: parameters.clone(),
                                            predicates: vec![],
                                            subquery: left_subquery.clone(),
                                            domain: domain.clone(),
                                        }),
                                        right: right_subquery.clone(),
                                    });
                                } else {
                                    // Substitute fresh column names for the left side subquery.
                                    let left_parameters: Vec<_> =
                                        parameters.iter().map(Column::fresh).collect();
                                    let left_parameters_map: HashMap<_, _> = (0..parameters.len())
                                        .map(|i| {
                                            (parameters[i].clone(), left_parameters[i].clone())
                                        })
                                        .collect();
                                    let left_subquery =
                                        left_subquery.clone().subst(&left_parameters_map);
                                    let left_domain = domain.clone().subst(&left_parameters_map);
                                    // Add natural-join on domain to the top join predicates.
                                    let mut inner_join_predicates = join.predicates().clone();
                                    for i in 0..parameters.len() {
                                        inner_join_predicates.push(Scalar::Call(Box::new(
                                            Function::Is(
                                                Scalar::Column(left_parameters[i].clone()),
                                                Scalar::Column(parameters[i].clone()),
                                            ),
                                        )));
                                    }
                                    // Push the rewritten dependent join down the left side, and the original dependent join down the right side.
                                    return Some(LogicalJoin {
                                        join: Join::Inner(inner_join_predicates),
                                        left: Box::new(LogicalDependentJoin {
                                            parameters: left_parameters,
                                            predicates: vec![],
                                            subquery: Box::new(left_subquery),
                                            domain: Box::new(left_domain),
                                        }),
                                        right: Box::new(LogicalDependentJoin {
                                            parameters: parameters.clone(),
                                            predicates: vec![],
                                            subquery: right_subquery.clone(),
                                            domain: domain.clone(),
                                        }),
                                    });
                                }
                            }
                            Join::Right(predicates) => {
                                // is left_subquery the right one?
                                if free_parameters(parameters, left_subquery).is_empty() {
                                    todo!()
                                } else {
                                    todo!()
                                }
                            }
                            Join::Outer(predicates) => todo!(),
                            Join::Semi(predicates) => {
                                // is left_subquery the right one?
                                if free_parameters(parameters, left_subquery).is_empty() {
                                    todo!()
                                } else {
                                    todo!()
                                }
                            }
                            Join::Anti(predicates) => {
                                // is left_subquery the right one?
                                if free_parameters(parameters, left_subquery).is_empty() {
                                    todo!()
                                } else {
                                    todo!()
                                }
                            }
                            Join::Single(predicates) => todo!(),
                            Join::Mark(_, predicates) => todo!(),
                        }
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughWith => {
                if let LogicalDependentJoin {
                    parameters,
                    predicates,
                    subquery,
                    domain,
                } = expr
                {
                    if free_parameters(parameters, subquery).is_empty() {
                        return None;
                    }
                    if let LogicalWith {
                        name,
                        columns,
                        left: left_left,
                        right: left_right,
                    } = subquery.as_ref()
                    {
                        todo!()
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughAggregate => {
                if let LogicalDependentJoin {
                    parameters,
                    predicates,
                    subquery,
                    domain,
                } = expr
                {
                    if free_parameters(parameters, subquery).is_empty() {
                        return None;
                    }
                    if let LogicalAggregate {
                        group_by,
                        aggregate,
                        input: subquery,
                    } = subquery.as_ref()
                    {
                        let mut group_by = group_by.clone();
                        for c in parameters {
                            group_by.push(c.clone());
                        }
                        return Some(LogicalAggregate {
                            group_by,
                            aggregate: aggregate.clone(),
                            input: Box::new(LogicalDependentJoin {
                                parameters: parameters.clone(),
                                predicates: predicates.clone(),
                                subquery: subquery.clone(),
                                domain: domain.clone(),
                            }),
                        });
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughLimit => {
                if let LogicalDependentJoin {
                    parameters,
                    predicates,
                    subquery,
                    domain,
                } = expr
                {
                    if free_parameters(parameters, subquery).is_empty() {
                        return None;
                    }
                    if let LogicalLimit {
                        limit,
                        offset,
                        input: subquery,
                    } = subquery.as_ref()
                    {
                        if *offset > 0 {
                            panic!("OFFSET is not supported in correlated subquery");
                        }
                        let input = LogicalDependentJoin {
                            parameters: parameters.clone(),
                            predicates: predicates.clone(),
                            subquery: subquery.clone(),
                            domain: domain.clone(),
                        };
                        if *limit == 0 {
                            return Some(LogicalLimit {
                                limit: *limit,
                                offset: *offset,
                                input: Box::new(input),
                            });
                        } else {
                            return Some(input);
                        }
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughSort => {
                if let LogicalDependentJoin {
                    parameters,
                    predicates,
                    subquery,
                    domain,
                } = expr
                {
                    if free_parameters(parameters, subquery).is_empty() {
                        return None;
                    }
                    if let LogicalSort {
                        order_by,
                        input: subquery,
                    } = subquery.as_ref()
                    {
                        todo!()
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughSetOperation => {
                if let LogicalDependentJoin {
                    parameters,
                    predicates,
                    subquery,
                    domain,
                } = expr
                {
                    if free_parameters(parameters, subquery).is_empty() {
                        return None;
                    }
                    if let LogicalUnion {
                        left: left_subquery,
                        right: right_subquery,
                    } = subquery.as_ref()
                    {
                        return Some(LogicalUnion {
                            left: Box::new(LogicalDependentJoin {
                                parameters: parameters.clone(),
                                predicates: predicates.clone(),
                                subquery: left_subquery.clone(),
                                domain: domain.clone(),
                            }),
                            right: Box::new(LogicalDependentJoin {
                                parameters: parameters.clone(),
                                predicates: predicates.clone(),
                                subquery: right_subquery.clone(),
                                domain: domain.clone(),
                            }),
                        });
                    }
                }
            }
            RewriteRule::MarkJoinToSemiJoin => {
                if let LogicalFilter {
                    predicates: filter_predicates,
                    input,
                } = expr
                {
                    if let LogicalJoin {
                        join: Join::Mark(mark, join_predicates),
                        left,
                        right,
                    } = input.as_ref()
                    {
                        let mut filter_predicates = filter_predicates.clone();
                        let semi = Scalar::Column(mark.clone());
                        let anti = Scalar::Call(Box::new(Function::Not(semi.clone())));
                        let mut combined_attributes = vec![];
                        for c in right.attributes() {
                            combined_attributes.push((Scalar::Column(c.clone()), c));
                        }
                        combined_attributes
                            .push((Scalar::Literal(Value::Boolean(true)), mark.clone()));
                        combined_attributes.sort_by(|(_, a), (_, b)| a.cmp(b));
                        for i in 0..filter_predicates.len() {
                            if filter_predicates[i] == semi {
                                filter_predicates.remove(i);
                                return Some(maybe_filter(
                                    &filter_predicates,
                                    &LogicalMap {
                                        include_existing: false,
                                        projects: combined_attributes,
                                        input: Box::new(LogicalJoin {
                                            join: Join::Semi(join_predicates.clone()),
                                            left: left.clone(),
                                            right: right.clone(),
                                        }),
                                    },
                                ));
                            } else if filter_predicates[i] == anti {
                                filter_predicates.remove(i);
                                return Some(maybe_filter(
                                    &filter_predicates,
                                    &LogicalMap {
                                        include_existing: false,
                                        projects: combined_attributes,
                                        input: Box::new(LogicalJoin {
                                            join: Join::Anti(join_predicates.clone()),
                                            left: left.clone(),
                                            right: right.clone(),
                                        }),
                                    },
                                ));
                            }
                        }
                    }
                }
            }
            RewriteRule::SingleJoinToInnerJoin => {
                if let LogicalJoin {
                    join: Join::Single(join_predicates),
                    left,
                    right,
                } = expr
                {
                    if join_predicates.is_empty() && prove_singleton(left) {
                        return Some(LogicalJoin {
                            join: Join::Inner(vec![]),
                            left: left.clone(),
                            right: right.clone(),
                        });
                    }
                }
            }
            RewriteRule::RemoveInnerJoin => {
                if let LogicalJoin {
                    join: Join::Inner(join_predicates),
                    left,
                    right,
                } = expr
                {
                    if let Some(projects) = is_table_free_scan(left.as_ref()) {
                        return Some(maybe_filter(join_predicates, &maybe_map(&projects, right)));
                    }
                    if let Some(projects) = is_table_free_scan(right.as_ref()) {
                        return Some(maybe_filter(join_predicates, &maybe_map(&projects, left)));
                    }
                }
            }
            RewriteRule::RemoveWith => {
                if let LogicalWith {
                    name,
                    columns,
                    left,
                    right,
                } = expr
                {
                    match count_get_with(name, right) {
                        0 if !left.has_side_effects() => return Some(right.as_ref().clone()),
                        1 => return Some(inline_with(name, columns, left, right.as_ref().clone())),
                        _ => return Some(with_to_script(name, columns, left, right)),
                    }
                }
            }
            RewriteRule::RemoveTableFreeScan => {
                if let LogicalJoin { join, left, right } = expr {
                    if let Join::Inner(join_predicates) = join {
                        if let TableFreeScan = right.as_ref() {
                            return Some(maybe_filter(join_predicates, left));
                        }
                    }
                }
            }
            RewriteRule::PushExplicitFilterIntoInnerJoin => {
                if let LogicalFilter {
                    predicates: filter_predicates,
                    input,
                } = expr
                {
                    if let LogicalJoin { join, left, right } = input.as_ref() {
                        if let Join::Inner(join_predicates) | Join::Semi(join_predicates) = join {
                            let mut combined = join_predicates.clone();
                            for p in filter_predicates {
                                combined.push(p.clone());
                            }
                            return Some(LogicalJoin {
                                join: join.replace(combined),
                                left: left.clone(),
                                right: right.clone(),
                            });
                        }
                    } else if let LogicalDependentJoin {
                        parameters,
                        predicates: join_predicates,
                        subquery,
                        domain,
                    } = input.as_ref()
                    {
                        let mut combined = join_predicates.clone();
                        for p in filter_predicates {
                            combined.push(p.clone());
                        }
                        return Some(LogicalDependentJoin {
                            parameters: parameters.clone(),
                            predicates: combined,
                            subquery: subquery.clone(),
                            domain: domain.clone(),
                        });
                    }
                }
            }
            RewriteRule::PushImplicitFilterThroughInnerJoin => {
                if let LogicalJoin { join, left, right } = expr {
                    if let Join::Inner(join_predicates) | Join::Semi(join_predicates) = join {
                        // Try to push down left.
                        let (correlated, uncorrelated) =
                            correlated_predicates(join_predicates, left);
                        if !uncorrelated.is_empty() {
                            return Some(LogicalJoin {
                                join: join.replace(correlated),
                                left: Box::new(LogicalFilter {
                                    predicates: uncorrelated,
                                    input: left.clone(),
                                }),
                                right: right.clone(),
                            });
                        }
                        // Try to push down right.
                        let (correlated, uncorrelated) =
                            correlated_predicates(join_predicates, right);
                        if !uncorrelated.is_empty() {
                            return Some(LogicalJoin {
                                join: join.replace(correlated),
                                left: left.clone(),
                                right: Box::new(LogicalFilter {
                                    predicates: uncorrelated,
                                    input: right.clone(),
                                }),
                            });
                        }
                    }
                } else if let LogicalDependentJoin {
                    parameters,
                    predicates: join_predicates,
                    subquery,
                    domain,
                } = expr
                {
                    // Try to push down subquery.
                    let (correlated, uncorrelated) =
                        correlated_predicates(join_predicates, subquery);
                    if !uncorrelated.is_empty() {
                        return Some(LogicalDependentJoin {
                            parameters: parameters.clone(),
                            predicates: correlated,
                            subquery: Box::new(LogicalFilter {
                                predicates: uncorrelated,
                                input: subquery.clone(),
                            }),
                            domain: domain.clone(),
                        });
                    }
                    // Try to push down domain.
                    let (correlated, uncorrelated) = correlated_predicates(join_predicates, domain);
                    if !uncorrelated.is_empty() {
                        return Some(LogicalDependentJoin {
                            parameters: parameters.clone(),
                            predicates: correlated,
                            subquery: subquery.clone(),
                            domain: Box::new(LogicalFilter {
                                predicates: uncorrelated,
                                input: domain.clone(),
                            }),
                        });
                    }
                }
            }
            RewriteRule::PushExplicitFilterThroughOuterJoin => {
                if let LogicalFilter {
                    predicates: filter_predicates,
                    input,
                } = expr
                {
                    if let LogicalJoin { join, left, right } = input.as_ref() {
                        let (correlated, uncorrelated) =
                            correlated_predicates(filter_predicates, right);
                        if !uncorrelated.is_empty() {
                            return Some(maybe_filter(
                                &correlated,
                                &LogicalJoin {
                                    join: join.clone(),
                                    left: left.clone(),
                                    right: Box::new(LogicalFilter {
                                        predicates: uncorrelated,
                                        input: right.clone(),
                                    }),
                                },
                            ));
                        }
                    }
                }
            }
            RewriteRule::PushFilterThroughMap => {
                if let LogicalFilter { predicates, input } = expr {
                    if let LogicalMap {
                        include_existing,
                        projects,
                        input,
                    } = input.as_ref()
                    {
                        let (correlated, uncorrelated) = correlated_predicates(predicates, input);
                        if !uncorrelated.is_empty() {
                            return Some(maybe_filter(
                                &correlated,
                                &LogicalMap {
                                    include_existing: *include_existing,
                                    projects: projects.clone(),
                                    input: Box::new(LogicalFilter {
                                        predicates: uncorrelated,
                                        input: input.clone(),
                                    }),
                                },
                            ));
                        }
                    }
                }
            }
            RewriteRule::CombineConsecutiveFilters => {
                if let LogicalFilter {
                    predicates: outer,
                    input,
                } = expr
                {
                    if let LogicalFilter {
                        predicates: inner,
                        input,
                    } = input.as_ref()
                    {
                        return combine_consecutive_filters(outer, inner, input);
                    }
                }
            }
            RewriteRule::EmbedFilterIntoGet => {
                if let LogicalFilter {
                    predicates: filter_predicates,
                    input,
                } = expr
                {
                    if let LogicalGet {
                        projects,
                        predicates,
                        table,
                    } = input.as_ref()
                    {
                        let mut filter_predicates = filter_predicates.clone();
                        let mut predicates = predicates.clone();
                        let projects = projects.clone();
                        let table = table.clone();
                        predicates.append(&mut filter_predicates);
                        return Some(LogicalGet {
                            projects,
                            predicates,
                            table,
                        });
                    }
                }
            }
            RewriteRule::CombineConsecutiveMaps => {
                // Map(x, Map(y, _)) => Map(x & y, _)
                if let LogicalMap {
                    include_existing: outer_include_existing,
                    projects: outer,
                    input,
                } = expr
                {
                    if let LogicalMap {
                        include_existing: inner_include_existing,
                        projects: inner,
                        input,
                    } = input.as_ref()
                    {
                        let mut inlined = if *outer_include_existing {
                            inner.clone()
                        } else {
                            vec![]
                        };
                        for (outer_expr, outer_column) in outer {
                            let mut outer_expr = outer_expr.clone();
                            for (inner_expr, inner_column) in inner {
                                outer_expr = outer_expr.inline(inner_expr, inner_column);
                            }
                            inlined.push((outer_expr, outer_column.clone()));
                        }
                        return Some(LogicalMap {
                            include_existing: *outer_include_existing && *inner_include_existing,
                            projects: inlined,
                            input: input.clone(),
                        });
                    }
                }
            }
            RewriteRule::EmbedMapIntoGet => {
                if let LogicalMap {
                    projects: outer,
                    input,
                    ..
                } = expr
                {
                    if let LogicalGet {
                        projects: inner,
                        predicates,
                        table,
                    } = input.as_ref()
                    {
                        let mut combined = inner.clone();
                        for (x, c) in outer {
                            // TODO if *some* mapped items can be embedded, embed them.
                            // TODO if column is renamed, that should be embedd-able, but it's not presently because we use column names as ids.
                            if !x.is_just(c) {
                                return None;
                            }
                            if !combined.contains(c) {
                                combined.push(c.clone());
                            }
                        }
                        return Some(LogicalGet {
                            projects: combined,
                            predicates: predicates.clone(),
                            table: table.clone(),
                        });
                    }
                }
            }
            RewriteRule::RemoveMap => {
                if let LogicalMap {
                    projects, input, ..
                } = expr
                {
                    if projects.len() == input.attributes().len()
                        && projects.iter().all(|(x, c)| x.is_just(c))
                    {
                        return Some(input.as_ref().clone());
                    }
                }
            }
        };
        None
    }
}

fn correlated_predicates(predicates: &Vec<Scalar>, input: &Expr) -> (Vec<Scalar>, Vec<Scalar>) {
    let scope = input.attributes();
    predicates
        .iter()
        .map(|p| p.clone())
        .partition(|p| !p.references().is_subset(&scope))
}

pub fn free_parameters(parameters: &Vec<Column>, subquery: &Expr) -> Vec<Column> {
    let free = subquery.references();
    parameters
        .iter()
        .map(|p| p.clone())
        .filter(|p| free.contains(p))
        .collect()
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

fn with_to_script(name: &String, columns: &Vec<Column>, left: &Expr, right: &Expr) -> Expr {
    LogicalScript {
        statements: vec![
            LogicalCreateTempTable {
                name: name.clone(),
                columns: columns.clone(),
                input: Box::new(left.clone()),
            },
            right.clone(),
        ],
    }
}

fn maybe_filter(predicates: &Vec<Scalar>, input: &Expr) -> Expr {
    if predicates.is_empty() {
        input.clone()
    } else {
        LogicalFilter {
            predicates: predicates.clone(),
            input: Box::new(input.clone()),
        }
    }
}

fn maybe_map(projects: &Vec<(Scalar, Column)>, input: &Expr) -> Expr {
    if projects.is_empty() {
        input.clone()
    } else {
        LogicalMap {
            include_existing: true,
            projects: projects.clone(),
            input: Box::new(input.clone()),
        }
    }
}

fn combine_consecutive_filters(
    outer: &Vec<Scalar>,
    inner: &Vec<Scalar>,
    input: &Expr,
) -> Option<Expr> {
    let mut combined = vec![];
    for p in outer {
        combined.push(p.clone());
    }
    for p in inner {
        combined.push(p.clone());
    }
    Some(LogicalFilter {
        predicates: combined,
        input: Box::new(input.clone()),
    })
}

fn combine_predicates(outer: &Vec<Scalar>, inner: &Vec<Scalar>) -> Vec<Scalar> {
    let mut combined = Vec::with_capacity(outer.len() + inner.len());
    for p in outer {
        combined.push(p.clone());
    }
    for p in inner {
        combined.push(p.clone());
    }
    combined
}

fn apply_all(before: Expr, rules: &Vec<RewriteRule>) -> Expr {
    for rule in rules {
        match rule.apply(&before) {
            // Abandon previous expr.
            Some(after) => {
                return apply_all(after, rules);
            }
            None => (),
        }
    }
    before
}

pub fn rewrite(catalog_id: i64, catalog: &SimpleCatalogProto, expr: Expr) -> Expr {
    let expr = rewrite_ddl(expr);
    let expr = general_unnest(expr);
    let expr = predicate_push_down(expr);
    let expr = optimize_join_type(expr);
    let expr = projection_push_down(expr);
    let expr = rewrite_logical_rewrite(catalog_id, catalog, expr);
    expr
}

fn rewrite_ddl(expr: Expr) -> Expr {
    expr.top_down_rewrite(&mut |expr| {
        apply_all(
            expr,
            &vec![
                RewriteRule::CreateDatabaseToScript,
                RewriteRule::CreateTableToScript,
                RewriteRule::CreateIndexToScript,
                RewriteRule::DropToScript,
                RewriteRule::UpdateToDeleteThenInsert,
            ],
        )
    })
}

// Unnest all dependent joins, and simplify joins where possible.
fn general_unnest(expr: Expr) -> Expr {
    expr.bottom_up_rewrite(&mut |expr| apply_all(expr, &vec![RewriteRule::PushDependentJoin]))
}

fn rewrite_logical_rewrite(catalog_id: i64, catalog: &SimpleCatalogProto, expr: Expr) -> Expr {
    expr.bottom_up_rewrite(&mut |expr| match expr {
        LogicalRewrite { sql } => {
            let expr = parser::analyze(catalog_id, &catalog, &sql).expect(&sql);
            rewrite(catalog_id, catalog, expr)
        }
        other => other,
    })
}

fn optimize_join_type(expr: Expr) -> Expr {
    expr.bottom_up_rewrite(&mut |expr| {
        apply_all(
            expr,
            &vec![
                RewriteRule::MarkJoinToSemiJoin,
                RewriteRule::SingleJoinToInnerJoin,
                RewriteRule::RemoveInnerJoin,
                RewriteRule::RemoveWith,
                RewriteRule::RemoveTableFreeScan,
            ],
        )
    })
}

// Push predicates into metadata.joins and scans.
fn predicate_push_down(expr: Expr) -> Expr {
    expr.top_down_rewrite(&mut |expr| {
        apply_all(
            expr,
            &vec![
                RewriteRule::PushExplicitFilterIntoInnerJoin,
                RewriteRule::PushImplicitFilterThroughInnerJoin,
                RewriteRule::PushExplicitFilterThroughOuterJoin,
                RewriteRule::PushFilterThroughMap,
                RewriteRule::CombineConsecutiveFilters,
                RewriteRule::EmbedFilterIntoGet,
            ],
        )
    })
}

fn projection_push_down(expr: Expr) -> Expr {
    expr.top_down_rewrite(&mut |expr| {
        apply_all(
            expr,
            &vec![
                RewriteRule::CombineConsecutiveMaps,
                RewriteRule::EmbedMapIntoGet,
                RewriteRule::RemoveMap,
            ],
        )
    })
}
