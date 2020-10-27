use encoding::*;
use node::*;
use std::collections::HashMap;

#[derive(Debug)]
enum RewriteRule {
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
            RewriteRule::PushDependentJoin => {
                if let LogicalDependentJoin {
                    parameters,
                    subquery,
                    ..
                } = expr.as_ref()
                {
                    if free_parameters(parameters, subquery).is_empty() {
                        return None;
                    }
                    return Some(expr.clone().top_down_rewrite(&|expr| {
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
                } = expr.as_ref()
                {
                    if free_parameters(parameters, subquery).is_empty() {
                        return None;
                    }
                    if let LogicalFilter(filter_predicates, subquery) = subquery.as_ref() {
                        return Some(Expr::new(LogicalFilter(
                            filter_predicates.clone(),
                            Expr::new(LogicalDependentJoin {
                                parameters: parameters.clone(),
                                predicates: join_predicates.clone(),
                                subquery: subquery.clone(),
                                domain: domain.clone(),
                            }),
                        )));
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughMap => {
                if let LogicalDependentJoin {
                    parameters,
                    predicates,
                    subquery,
                    domain,
                } = expr.as_ref()
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
                        return Some(Expr::new(LogicalMap {
                            include_existing: *include_existing,
                            projects,
                            input: Expr::new(LogicalDependentJoin {
                                parameters: parameters.clone(),
                                predicates: predicates.clone(),
                                subquery: subquery.clone(),
                                domain: domain.clone(),
                            }),
                        }));
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughJoin => {
                if let LogicalDependentJoin {
                    parameters,
                    predicates,
                    subquery,
                    domain,
                } = expr.as_ref()
                {
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
                                    return Some(Expr::new(LogicalJoin {
                                        join: join.clone(),
                                        left: left_subquery.clone(),
                                        right: Expr::new(LogicalDependentJoin {
                                            parameters: parameters.clone(),
                                            predicates: predicates.clone(),
                                            subquery: right_subquery.clone(),
                                            domain: domain.clone(),
                                        }),
                                    }));
                                } else if free_parameters(parameters, right_subquery).is_empty() {
                                    return Some(Expr::new(LogicalJoin {
                                        join: join.clone(),
                                        left: Expr::new(LogicalDependentJoin {
                                            parameters: parameters.clone(),
                                            predicates: predicates.clone(),
                                            subquery: left_subquery.clone(),
                                            domain: domain.clone(),
                                        }),
                                        right: right_subquery.clone(),
                                    }));
                                } else {
                                    todo!()
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
                } = expr.as_ref()
                {
                    if free_parameters(parameters, subquery).is_empty() {
                        return None;
                    }
                    if let LogicalWith(name, columns, left_left, left_right) = subquery.as_ref() {
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
                } = expr.as_ref()
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
                        return Some(Expr::new(LogicalAggregate {
                            group_by,
                            aggregate: aggregate.clone(),
                            input: Expr::new(LogicalDependentJoin {
                                parameters: parameters.clone(),
                                predicates: predicates.clone(),
                                subquery: subquery.clone(),
                                domain: domain.clone(),
                            }),
                        }));
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughLimit => {
                if let LogicalDependentJoin {
                    parameters,
                    predicates,
                    subquery,
                    domain,
                } = expr.as_ref()
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
                        todo!()
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughSort => {
                if let LogicalDependentJoin {
                    parameters,
                    predicates,
                    subquery,
                    domain,
                } = expr.as_ref()
                {
                    if free_parameters(parameters, subquery).is_empty() {
                        return None;
                    }
                    if let LogicalSort(order_by, subquery) = subquery.as_ref() {
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
                } = expr.as_ref()
                {
                    if free_parameters(parameters, subquery).is_empty() {
                        return None;
                    }
                    match subquery.as_ref() {
                        LogicalUnion(left_subquery, right_subquery) => todo!(),
                        LogicalIntersect(left_subquery, right_subquery) => todo!(),
                        LogicalExcept(left_subquery, right_subquery) => todo!(),
                        _ => {}
                    }
                }
            }
            RewriteRule::MarkJoinToSemiJoin => {
                if let LogicalFilter(filter_predicates, input) = expr.as_ref() {
                    if let LogicalJoin {
                        join: Join::Mark(mark, join_predicates),
                        left,
                        right,
                    } = input.as_ref()
                    {
                        let mut filter_predicates = filter_predicates.clone();
                        let semi = Scalar::Column(mark.clone());
                        let anti = Scalar::Call(Function::Not, vec![semi.clone()], Type::Bool);
                        let mut combined_attributes = vec![];
                        for c in left.attributes() {
                            combined_attributes.push((Scalar::Column(c.clone()), c));
                        }
                        for c in right.attributes() {
                            combined_attributes.push((Scalar::Column(c.clone()), c));
                        }
                        combined_attributes
                            .push((Scalar::Literal(Value::Bool(true), Type::Bool), mark.clone()));
                        combined_attributes.sort_by(|(_, a), (_, b)| a.cmp(b));
                        for i in 0..filter_predicates.len() {
                            if filter_predicates[i] == semi {
                                filter_predicates.remove(i);
                                return Some(maybe_filter(
                                    filter_predicates,
                                    Expr::new(LogicalMap {
                                        include_existing: false,
                                        projects: combined_attributes,
                                        input: Expr::new(LogicalJoin {
                                            join: Join::Semi(join_predicates.clone()),
                                            left: left.clone(),
                                            right: right.clone(),
                                        }),
                                    }),
                                ));
                            } else if filter_predicates[i] == anti {
                                filter_predicates.remove(i);
                                return Some(maybe_filter(
                                    filter_predicates,
                                    Expr::new(LogicalMap {
                                        include_existing: false,
                                        projects: combined_attributes,
                                        input: Expr::new(LogicalJoin {
                                            join: Join::Anti(join_predicates.clone()),
                                            left: left.clone(),
                                            right: right.clone(),
                                        }),
                                    }),
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
                } = expr.as_ref()
                {
                    if join_predicates.is_empty() && prove_singleton(left) {
                        return Some(Expr::new(LogicalJoin {
                            join: Join::Inner(vec![]),
                            left: left.clone(),
                            right: right.clone(),
                        }));
                    }
                }
            }
            RewriteRule::RemoveInnerJoin => {
                if let LogicalJoin {
                    join: Join::Inner(join_predicates),
                    left,
                    right,
                } = expr.as_ref()
                {
                    if let Some(single) = remove_inner_join_left(
                        left.clone(),
                        maybe_filter(join_predicates.clone(), right.clone()),
                    ) {
                        return Some(single);
                    } else if let Some(single) = remove_inner_join_left(
                        right.clone(),
                        maybe_filter(join_predicates.clone(), left.clone()),
                    ) {
                        return Some(single);
                    }
                }
            }
            RewriteRule::RemoveWith => {
                if let LogicalWith(name, columns, left, right) = expr.as_ref() {
                    match count_get_with(name, right) {
                        0 if !has_side_effects(left) => return Some(right.clone()),
                        1 => return Some(inline_with(name, columns, left, right.clone())),
                        _ => (),
                    }
                }
            }
            RewriteRule::PushExplicitFilterIntoInnerJoin => {
                if let LogicalFilter(filter_predicates, input) = expr.as_ref() {
                    if let LogicalJoin { join, left, right } = input.as_ref() {
                        if let Join::Inner(join_predicates) | Join::Semi(join_predicates) = join {
                            let mut combined = join_predicates.clone();
                            for p in filter_predicates {
                                combined.push(p.clone());
                            }
                            return Some(Expr::new(LogicalJoin {
                                join: join.replace(combined),
                                left: left.clone(),
                                right: right.clone(),
                            }));
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
                        return Some(Expr::new(LogicalDependentJoin {
                            parameters: parameters.clone(),
                            predicates: combined,
                            subquery: subquery.clone(),
                            domain: domain.clone(),
                        }));
                    }
                }
            }
            RewriteRule::PushImplicitFilterThroughInnerJoin => {
                if let LogicalJoin { join, left, right } = expr.as_ref() {
                    if let Join::Inner(join_predicates) | Join::Semi(join_predicates) = join {
                        // Try to push down left.
                        let (correlated, uncorrelated) =
                            correlated_predicates(join_predicates, left);
                        if !uncorrelated.is_empty() {
                            return Some(Expr::new(LogicalJoin {
                                join: join.replace(correlated),
                                left: Expr::new(LogicalFilter(uncorrelated, left.clone())),
                                right: right.clone(),
                            }));
                        }
                        // Try to push down right.
                        let (correlated, uncorrelated) =
                            correlated_predicates(join_predicates, right);
                        if !uncorrelated.is_empty() {
                            return Some(Expr::new(LogicalJoin {
                                join: join.replace(correlated),
                                left: left.clone(),
                                right: Expr::new(LogicalFilter(uncorrelated, right.clone())),
                            }));
                        }
                    }
                } else if let LogicalDependentJoin {
                    parameters,
                    predicates: join_predicates,
                    subquery,
                    domain,
                } = expr.as_ref()
                {
                    // Try to push down subquery.
                    let (correlated, uncorrelated) =
                        correlated_predicates(join_predicates, subquery);
                    if !uncorrelated.is_empty() {
                        return Some(Expr::new(LogicalDependentJoin {
                            parameters: parameters.clone(),
                            predicates: correlated,
                            subquery: Expr::new(LogicalFilter(uncorrelated, subquery.clone())),
                            domain: domain.clone(),
                        }));
                    }
                    // Try to push down domain.
                    let (correlated, uncorrelated) = correlated_predicates(join_predicates, domain);
                    if !uncorrelated.is_empty() {
                        return Some(Expr::new(LogicalDependentJoin {
                            parameters: parameters.clone(),
                            predicates: correlated,
                            subquery: subquery.clone(),
                            domain: Expr::new(LogicalFilter(uncorrelated, domain.clone())),
                        }));
                    }
                }
            }
            RewriteRule::PushExplicitFilterThroughOuterJoin => {
                if let LogicalFilter(filter_predicates, input) = expr.as_ref() {
                    if let LogicalJoin { join, left, right } = input.as_ref() {
                        let (correlated, uncorrelated) =
                            correlated_predicates(filter_predicates, right);
                        if !uncorrelated.is_empty() {
                            return Some(maybe_filter(
                                correlated,
                                Expr::new(LogicalJoin {
                                    join: join.clone(),
                                    left: left.clone(),
                                    right: Expr::new(LogicalFilter(uncorrelated, right.clone())),
                                }),
                            ));
                        }
                    }
                }
            }
            RewriteRule::PushFilterThroughMap => {
                if let LogicalFilter(predicates, input) = expr.as_ref() {
                    if let LogicalMap {
                        include_existing,
                        projects,
                        input,
                    } = input.as_ref()
                    {
                        let (correlated, uncorrelated) = correlated_predicates(predicates, input);
                        if !uncorrelated.is_empty() {
                            return Some(maybe_filter(
                                correlated,
                                Expr::new(LogicalMap {
                                    include_existing: *include_existing,
                                    projects: projects.clone(),
                                    input: Expr::new(LogicalFilter(uncorrelated, input.clone())),
                                }),
                            ));
                        }
                    }
                }
            }
            RewriteRule::CombineConsecutiveFilters => {
                if let LogicalFilter(outer, input) = expr.as_ref() {
                    if let LogicalFilter(inner, input) = input.as_ref() {
                        return combine_consecutive_filters(outer, inner, input);
                    }
                }
            }
            RewriteRule::EmbedFilterIntoGet => {
                if let LogicalFilter(filter_predicates, input) = expr.as_ref() {
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
                        return Some(Expr::new(LogicalGet {
                            projects,
                            predicates,
                            table,
                        }));
                    }
                }
            }
            RewriteRule::CombineConsecutiveMaps => {
                // Map(x, Map(y, _)) => Map(x & y, _)
                if let LogicalMap {
                    include_existing: outer_include_existing,
                    projects: outer,
                    input,
                } = expr.as_ref()
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
                        return Some(Expr::new(LogicalMap {
                            include_existing: *outer_include_existing && *inner_include_existing,
                            projects: inlined,
                            input: input.clone(),
                        }));
                    }
                }
            }
            RewriteRule::EmbedMapIntoGet => {
                if let LogicalMap {
                    projects: outer,
                    input,
                    ..
                } = expr.as_ref()
                {
                    if let LogicalGet {
                        projects: inner,
                        predicates,
                        table,
                    } = input.as_ref()
                    {
                        let mut combined = inner.clone();
                        for (x, c) in outer {
                            if !x.is_just(c) {
                                return None;
                            }
                            if !combined.contains(c) {
                                combined.push(c.clone());
                            }
                        }
                        return Some(Expr::new(LogicalGet {
                            projects: combined,
                            predicates: predicates.clone(),
                            table: table.clone(),
                        }));
                    }
                }
            }
            RewriteRule::RemoveMap => {
                if let LogicalMap {
                    projects, input, ..
                } = expr.as_ref()
                {
                    if projects.len() == input.attributes().len()
                        && projects.iter().all(|(x, c)| x.is_just(c))
                    {
                        return Some(input.clone());
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
    match expr.as_ref() {
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
    match expr.as_ref() {
        LogicalMap { input, .. } | LogicalAggregate { input, .. } => prove_non_empty(input),
        LogicalSingleGet => true,
        _ => false,
    }
}

fn remove_inner_join_left(left: Expr, right: Expr) -> Option<Expr> {
    match *left.0 {
        LogicalMap {
            include_existing,
            projects,
            input,
        } => remove_inner_join_left(input, right).map(|input| {
            Expr::new(LogicalMap {
                include_existing,
                projects,
                input,
            })
        }),
        LogicalSingleGet => Some(right),
        _ => None,
    }
}

fn count_get_with(name: &String, expr: &Expr) -> usize {
    let mut count = 0;
    for e in expr.iter() {
        if let LogicalGetWith(get_name, _) = e.as_ref() {
            if name == get_name {
                count += 1
            }
        }
    }
    count
}

fn inline_with(name: &String, columns: &Vec<Column>, left: &Expr, right: Expr) -> Expr {
    match *right.0 {
        LogicalGetWith(get_name, get_columns) if name == &get_name => {
            let mut projects = vec![];
            for i in 0..columns.len() {
                projects.push((Scalar::Column(columns[i].clone()), get_columns[i].clone()))
            }
            Expr::new(LogicalMap {
                include_existing: false,
                projects,
                input: left.clone(),
            })
        }
        expr => Expr::new(expr.map(|child| inline_with(name, columns, left, child))),
    }
}

fn has_side_effects(expr: &Expr) -> bool {
    expr.iter().any(|e| e.0.has_side_effects())
}

fn maybe_filter(predicates: Vec<Scalar>, input: Expr) -> Expr {
    if predicates.is_empty() {
        input
    } else {
        Expr::new(LogicalFilter(predicates, input))
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
    Some(Expr::new(LogicalFilter(combined, input.clone())))
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

fn apply_all(expr: Expr, rules: &Vec<RewriteRule>) -> Expr {
    for rule in rules {
        match rule.apply(&expr) {
            // Abandon previous expr.
            Some(expr) => {
                return apply_all(expr, rules);
            }
            None => (),
        }
    }
    expr
}
pub fn rewrite(expr: Expr) -> Expr {
    // println!("start:\n{}", &expr);
    let expr = general_unnest(expr);
    // println!("general_unnest:\n{}", &expr);
    let expr = predicate_push_down(expr);
    // println!("predicate_push_down:\n{}", &expr);
    let expr = optimize_join_type(expr);
    // println!("optimize_join_type:\n{}", &expr);
    let expr = projection_push_down(expr);
    // println!("projection_push_down:\n{}", &expr);
    expr
}

// Unnest all dependent joins, and simplify joins where possible.
fn general_unnest(expr: Expr) -> Expr {
    expr.bottom_up_rewrite(&|expr| apply_all(expr, &vec![RewriteRule::PushDependentJoin]))
}

fn optimize_join_type(expr: Expr) -> Expr {
    expr.bottom_up_rewrite(&|expr| {
        apply_all(
            expr,
            &vec![
                RewriteRule::MarkJoinToSemiJoin,
                RewriteRule::SingleJoinToInnerJoin,
                RewriteRule::RemoveInnerJoin,
                RewriteRule::RemoveWith,
            ],
        )
    })
}

// Push predicates into joins and scans.
fn predicate_push_down(expr: Expr) -> Expr {
    expr.top_down_rewrite(&|expr| {
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
    expr.top_down_rewrite(&|expr| {
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
