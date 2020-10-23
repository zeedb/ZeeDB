use encoding::*;
use node::*;

#[derive(Debug)]
enum RewriteRule {
    // Simple unnesting:
    PullExplicitFilterIntoInnerJoin,
    PullImplicitFilterThroughInnerJoin,
    PullExplicitFilterThroughOuterJoin,
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
    PushFilterThroughProject,
    CombineConsecutiveFilters,
    EmbedFilterIntoGet,
    // Optimize projections:
    CombineConsecutiveProjects,
    EmbedProjectIntoGet,
    AggregateToProject,
    RemoveMap,
}

impl RewriteRule {
    fn apply(&self, expr: &Expr) -> Option<Expr> {
        match self {
            RewriteRule::PullExplicitFilterIntoInnerJoin => {
                if let LogicalJoin {
                    parameters,
                    join: Join::Inner(join_predicates),
                    left,
                    right,
                } = expr.as_ref()
                {
                    if let LogicalFilter(left_predicates, left) = left.as_ref() {
                        let (correlated, uncorrelated) =
                            correlated_predicates(left_predicates, left);
                        if !correlated.is_empty() {
                            let mut join_predicates = join_predicates.clone();
                            join_predicates.extend(correlated);
                            return Some(Expr::new(LogicalJoin {
                                parameters: free_parameters(parameters, left),
                                join: Join::Inner(join_predicates),
                                left: maybe_filter(uncorrelated, left.clone()),
                                right: right.clone(),
                            }));
                        }
                    }
                    if let LogicalFilter(right_predicates, right) = right.as_ref() {
                        let (correlated, uncorrelated) =
                            correlated_predicates(right_predicates, right);
                        if !correlated.is_empty() {
                            let mut join_predicates = join_predicates.clone();
                            join_predicates.extend(correlated);
                            return Some(Expr::new(LogicalJoin {
                                parameters: parameters.clone(),
                                join: Join::Inner(join_predicates),
                                left: left.clone(),
                                right: maybe_filter(uncorrelated, right.clone()),
                            }));
                        }
                    }
                }
            }
            RewriteRule::PullImplicitFilterThroughInnerJoin => {
                if let LogicalJoin {
                    parameters,
                    join: Join::Inner(join_predicates),
                    left,
                    right,
                } = expr.as_ref()
                {
                    let (correlated, uncorrelated) = correlated_predicates(join_predicates, expr);
                    if !correlated.is_empty() {
                        return Some(Expr::new(LogicalFilter(
                            correlated,
                            Expr::new(LogicalJoin {
                                parameters: free_parameters(parameters, left),
                                join: Join::Inner(uncorrelated),
                                left: left.clone(),
                                right: right.clone(),
                            }),
                        )));
                    }
                }
            }
            RewriteRule::PullExplicitFilterThroughOuterJoin => {
                if let LogicalJoin {
                    parameters,
                    join,
                    left,
                    right,
                } = expr.as_ref()
                {
                    if let LogicalFilter(filter_predicates, right) = right.as_ref() {
                        let (correlated, uncorrelated) =
                            correlated_predicates(filter_predicates, right);
                        if !correlated.is_empty() {
                            return Some(Expr::new(LogicalFilter(
                                correlated,
                                Expr::new(LogicalJoin {
                                    parameters: free_parameters(parameters, left),
                                    join: join.clone(),
                                    left: left.clone(),
                                    right: maybe_filter(uncorrelated, right.clone()),
                                }),
                            )));
                        }
                    }
                }
            }
            RewriteRule::PushDependentJoin => {
                if let LogicalJoin {
                    parameters,
                    join,
                    left,
                    right,
                } = expr.as_ref()
                {
                    if parameters.is_empty() {
                        return None;
                    }
                    return Some(unnest_one(parameters, join, left, right));
                }
            }
            RewriteRule::PushDependentJoinThroughFilter => {
                if let LogicalJoin {
                    parameters,
                    join: Join::Inner(join_predicates),
                    left: subquery,
                    right: domain,
                } = expr.as_ref()
                {
                    if parameters.is_empty() {
                        return None;
                    }
                    if !join_predicates.is_empty() {
                        return None;
                    }
                    if let LogicalFilter(predicates, subquery) = subquery.as_ref() {
                        return Some(Expr::new(LogicalFilter(
                            predicates.clone(),
                            push_dependent_join(parameters, subquery, domain),
                        )));
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughMap => {
                if let LogicalJoin {
                    parameters,
                    join: Join::Inner(join_predicates),
                    left: subquery,
                    right: domain,
                } = expr.as_ref()
                {
                    if parameters.is_empty() {
                        return None;
                    }
                    if !join_predicates.is_empty() {
                        return None;
                    }
                    if let LogicalMap(projects, subquery) = subquery.as_ref() {
                        let mut projects = projects.clone();
                        for p in parameters {
                            if !projects.iter().any(|(_, c)| c == p) {
                                projects.push((Scalar::Column(p.clone()), p.clone()));
                            }
                        }
                        return Some(Expr::new(LogicalMap(
                            projects,
                            push_dependent_join(parameters, subquery, domain),
                        )));
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughJoin => {
                if let LogicalJoin {
                    parameters,
                    join: Join::Inner(join_predicates),
                    left: subquery,
                    right: domain,
                } = expr.as_ref()
                {
                    if parameters.is_empty() {
                        return None;
                    }
                    if !join_predicates.is_empty() {
                        return None;
                    }
                    if let LogicalJoin {
                        parameters: empty_parameters,
                        join,
                        left: left_subquery,
                        right: right_subquery,
                    } = subquery.as_ref()
                    {
                        if !empty_parameters.is_empty() {
                            return None;
                        }
                        match join {
                            Join::Inner(predicates) => {
                                if free_parameters(parameters, left_subquery).is_empty() {
                                    return Some(Expr::new(LogicalJoin {
                                        parameters: vec![],
                                        join: Join::Inner(predicates.clone()),
                                        left: left_subquery.clone(),
                                        right: push_dependent_join(
                                            parameters,
                                            right_subquery,
                                            domain,
                                        ),
                                    }));
                                } else if free_parameters(parameters, right_subquery).is_empty() {
                                    return Some(Expr::new(LogicalJoin {
                                        parameters: vec![],
                                        join: Join::Inner(predicates.clone()),
                                        left: push_dependent_join(
                                            parameters,
                                            left_subquery,
                                            domain,
                                        ),
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
                if let LogicalJoin {
                    parameters,
                    join: Join::Inner(join_predicates),
                    left: subquery,
                    right: domain,
                } = expr.as_ref()
                {
                    if parameters.is_empty() {
                        return None;
                    }
                    if !join_predicates.is_empty() {
                        return None;
                    }
                    if let LogicalWith(name, columns, left_left, left_right) = subquery.as_ref() {
                        todo!()
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughAggregate => {
                if let LogicalJoin {
                    parameters,
                    join: Join::Inner(join_predicates),
                    left: subquery,
                    right: domain,
                } = expr.as_ref()
                {
                    if parameters.is_empty() {
                        return None;
                    }
                    if !join_predicates.is_empty() {
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
                            input: push_dependent_join(parameters, subquery, domain),
                        }));
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughLimit => {
                if let LogicalJoin {
                    parameters,
                    join: Join::Inner(join_predicates),
                    left: subquery,
                    right: domain,
                } = expr.as_ref()
                {
                    if parameters.is_empty() {
                        return None;
                    }
                    if !join_predicates.is_empty() {
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
                if let LogicalJoin {
                    parameters,
                    join: Join::Inner(join_predicates),
                    left: subquery,
                    right: domain,
                } = expr.as_ref()
                {
                    if parameters.is_empty() {
                        return None;
                    }
                    if !join_predicates.is_empty() {
                        return None;
                    }
                    if let LogicalSort(order_by, subquery) = subquery.as_ref() {
                        todo!()
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughSetOperation => {
                if let LogicalJoin {
                    parameters,
                    join: Join::Inner(join_predicates),
                    left: subquery,
                    right: domain,
                } = expr.as_ref()
                {
                    if parameters.is_empty() {
                        return None;
                    }
                    if !join_predicates.is_empty() {
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
                        parameters,
                        join: Join::Mark(mark, join_predicates),
                        left,
                        right,
                    } = input.as_ref()
                    {
                        if !parameters.is_empty() {
                            return None;
                        }
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
                                    Expr::new(LogicalMap(
                                        combined_attributes,
                                        Expr::new(LogicalJoin {
                                            parameters: vec![],
                                            join: Join::Semi(join_predicates.clone()),
                                            left: left.clone(),
                                            right: right.clone(),
                                        }),
                                    )),
                                ));
                            } else if filter_predicates[i] == anti {
                                filter_predicates.remove(i);
                                return Some(maybe_filter(
                                    filter_predicates,
                                    Expr::new(LogicalMap(
                                        combined_attributes,
                                        Expr::new(LogicalJoin {
                                            parameters: vec![],
                                            join: Join::Anti(join_predicates.clone()),
                                            left: left.clone(),
                                            right: right.clone(),
                                        }),
                                    )),
                                ));
                            }
                        }
                    }
                }
            }
            RewriteRule::SingleJoinToInnerJoin => {
                if let LogicalJoin {
                    parameters,
                    join: Join::Single(join_predicates),
                    left,
                    right,
                } = expr.as_ref()
                {
                    if !parameters.is_empty() {
                        return None;
                    }
                    if join_predicates.is_empty() && prove_singleton(left) {
                        return Some(Expr::new(LogicalJoin {
                            parameters: vec![],
                            join: Join::Inner(vec![]),
                            left: left.clone(),
                            right: right.clone(),
                        }));
                    }
                }
            }
            RewriteRule::RemoveInnerJoin => {
                if let LogicalJoin {
                    parameters,
                    join: Join::Inner(join_predicates),
                    left,
                    right,
                } = expr.as_ref()
                {
                    if !parameters.is_empty() {
                        return None;
                    }
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
                    if let LogicalJoin {
                        parameters,
                        join: Join::Inner(join_predicates),
                        left,
                        right,
                    } = input.as_ref()
                    {
                        if !parameters.is_empty() {
                            return None;
                        }
                        let mut combined = join_predicates.clone();
                        for p in filter_predicates {
                            combined.push(p.clone());
                        }
                        return Some(Expr::new(LogicalJoin {
                            parameters: vec![],
                            join: Join::Inner(combined),
                            left: left.clone(),
                            right: right.clone(),
                        }));
                    }
                }
            }
            RewriteRule::PushImplicitFilterThroughInnerJoin => {
                if let LogicalJoin {
                    parameters,
                    join: Join::Inner(join_predicates),
                    left,
                    right,
                } = expr.as_ref()
                {
                    if !parameters.is_empty() {
                        return None;
                    }
                    let (correlated, uncorrelated) = correlated_predicates(join_predicates, left);
                    if !uncorrelated.is_empty() {
                        return Some(Expr::new(LogicalJoin {
                            parameters: vec![],
                            join: Join::Inner(correlated),
                            left: Expr::new(LogicalFilter(uncorrelated, left.clone())),
                            right: right.clone(),
                        }));
                    }
                    let (correlated, uncorrelated) = correlated_predicates(join_predicates, right);
                    if !uncorrelated.is_empty() {
                        return Some(Expr::new(LogicalJoin {
                            parameters: vec![],
                            join: Join::Inner(correlated),
                            left: left.clone(),
                            right: Expr::new(LogicalFilter(uncorrelated, right.clone())),
                        }));
                    }
                }
            }
            RewriteRule::PushExplicitFilterThroughOuterJoin => {
                if let LogicalFilter(filter_predicates, input) = expr.as_ref() {
                    if let LogicalJoin {
                        parameters,
                        join,
                        left,
                        right,
                    } = input.as_ref()
                    {
                        if !parameters.is_empty() {
                            return None;
                        }
                        let (correlated, uncorrelated) =
                            correlated_predicates(filter_predicates, right);
                        if !uncorrelated.is_empty() {
                            return Some(maybe_filter(
                                correlated,
                                Expr::new(LogicalJoin {
                                    parameters: vec![],
                                    join: join.clone(),
                                    left: left.clone(),
                                    right: Expr::new(LogicalFilter(uncorrelated, right.clone())),
                                }),
                            ));
                        }
                    }
                }
            }
            RewriteRule::PushFilterThroughProject => {
                if let LogicalFilter(predicates, input) = expr.as_ref() {
                    if let LogicalMap(projects, input) = input.as_ref() {
                        return push_filter_through_project(predicates, projects, input);
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
            RewriteRule::CombineConsecutiveProjects => {
                if let LogicalMap(outer, input) = expr.as_ref() {
                    // Map(x, Map(y, _)) => Map(x & y, _)
                    if let LogicalMap(inner, input) = input.as_ref() {
                        let mut inlined = vec![];
                        for (outer_expr, outer_column) in outer {
                            let mut outer_expr = outer_expr.clone();
                            for (inner_expr, inner_column) in inner {
                                outer_expr = outer_expr.inline(inner_expr, inner_column);
                            }
                            inlined.push((outer_expr, outer_column.clone()));
                        }
                        return Some(Expr::new(LogicalMap(inlined, input.clone())));
                    }
                }
                if let LogicalProject(outer, input) = expr.as_ref() {
                    // Project(x, Map(y, _)) => Project(x, _) if y in x
                    if let LogicalMap(inner, input) = input.as_ref() {
                        if inner
                            .iter()
                            .all(|(x, c)| x.is_just(c) || !outer.contains(c))
                        {
                            return Some(Expr::new(LogicalProject(outer.clone(), input.clone())));
                        }
                    }
                    // Project(x, Project(y, _)) => Project(x, _)
                    if let LogicalProject(_, input) = input.as_ref() {
                        return Some(Expr::new(LogicalProject(outer.clone(), input.clone())));
                    }
                }
            }
            RewriteRule::EmbedProjectIntoGet => {
                if let LogicalMap(outer, input) = expr.as_ref() {
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
            RewriteRule::AggregateToProject => {
                if let LogicalAggregate {
                    group_by,
                    aggregate,
                    input,
                } = expr.as_ref()
                {
                    if aggregate.is_empty() {
                        return Some(Expr::new(LogicalProject(group_by.clone(), input.clone())));
                    }
                }
            }
            RewriteRule::RemoveMap => {
                if let LogicalMap(projects, input) = expr.as_ref() {
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
        .clone()
        .iter()
        .map(|c| c.clone())
        .partition(|c| !c.free().is_subset(&scope))
}

fn push_dependent_join(parameters: &Vec<Column>, subquery: &Expr, domain: &Expr) -> Expr {
    Expr::new(LogicalJoin {
        parameters: free_parameters(parameters, subquery),
        join: Join::Inner(vec![]),
        left: subquery.clone(),
        right: domain.clone(),
    })
}

pub fn free_parameters(parameters: &Vec<Column>, subquery: &Expr) -> Vec<Column> {
    let free = subquery.free();
    parameters
        .iter()
        .map(|p| p.clone())
        .filter(|p| free.contains(p))
        .collect()
}

fn prove_singleton(expr: &Expr) -> bool {
    match expr.as_ref() {
        LogicalMap(_, input) => prove_singleton(input),
        LogicalProject(projects, input) => {
            if projects.is_empty() {
                prove_non_empty(input)
            } else {
                prove_singleton(input)
            }
        }
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
        LogicalMap(_, input) | LogicalProject(_, input) | LogicalAggregate { input, .. } => {
            prove_non_empty(input)
        }
        LogicalSingleGet => true,
        _ => false,
    }
}

fn remove_inner_join_left(left: Expr, right: Expr) -> Option<Expr> {
    match *left.0 {
        LogicalMap(projects, left) => {
            remove_inner_join_left(left, right).map(|left| Expr::new(LogicalMap(projects, left)))
        }
        LogicalProject(projects, left) => remove_inner_join_left(left, right)
            .map(|left| Expr::new(LogicalProject(projects, left))),
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
            Expr::new(LogicalMap(projects, left.clone()))
        }
        expr => Expr::new(expr.map(|child| inline_with(name, columns, left, child))),
    }
}

fn has_side_effects(expr: &Expr) -> bool {
    expr.iter().any(|e| e.0.has_side_effects())
}

fn push_filter_through_project(
    predicates: &Vec<Scalar>,
    projects: &Vec<(Scalar, Column)>,
    input: &Expr,
) -> Option<Expr> {
    let mut outer = vec![];
    let mut inner = vec![];
    for p in predicates {
        if p.free().is_subset(&input.attributes()) {
            inner.push(p.clone());
        } else {
            outer.push(p.clone());
        }
    }
    if inner.is_empty() {
        return None;
    }
    Some(maybe_filter(
        outer,
        Expr::new(LogicalMap(
            projects.clone(),
            Expr::new(LogicalFilter(inner, input.clone())),
        )),
    ))
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
    let expr = simple_unnest(expr);
    let expr = general_unnest(expr);
    let expr = predicate_push_down(expr);
    let expr = optimize_join_type(expr);
    let expr = projection_push_down(expr);
    expr
}

fn simple_unnest(expr: Expr) -> Expr {
    expr.bottom_up_rewrite(&|expr| {
        apply_all(
            expr,
            &vec![
                RewriteRule::PullExplicitFilterIntoInnerJoin,
                RewriteRule::PullImplicitFilterThroughInnerJoin,
                RewriteRule::PullExplicitFilterThroughOuterJoin,
                RewriteRule::CombineConsecutiveFilters,
            ],
        )
    })
}

// Unnest all dependent joins, and simplify joins where possible.
fn general_unnest(expr: Expr) -> Expr {
    expr.bottom_up_rewrite(&|expr| apply_all(expr, &vec![RewriteRule::PushDependentJoin]))
}

// Unnest one dependent join, assuming there are no dependent joins under it.
fn unnest_one(
    subquery_parameters: &Vec<Column>,
    join: &Join,
    subquery: &Expr,
    outer: &Expr,
) -> Expr {
    // A correlated subquery can be interpreted as a dependent join
    // that executes the subquery once for every tuple in outer:
    //
    //        DependentJoin
    //         +         +
    //         +         +
    //    subquery      outer
    //
    // As a first step in eliminating the dependent join, we rewrite it as dependent join
    // that executes the subquery once for every *distinct combination of parameters* in outer,
    // and then an equi-join that looks up the appropriate row for every tuple in outer:
    //
    //             LogicalJoin
    //              +       +
    //              +       +
    //   DependentJoin     outer
    //    +         +
    //    +         +
    // subquery  Project
    //              +
    //              +
    //            outer
    //
    // This is a slight improvement because the number of distinct combinations of parameters in outer
    // is likely much less than the number of tuples in outer,
    // and it makes the dependent join much easier to manipulate because Project is a set rather than a multiset.
    // During the rewrite phase, we will take advantage of this to push the dependent join down
    // until it can be eliminated or converted to an inner join.
    //
    //  Project
    //     +
    //     +
    //   outer
    let project = LogicalProject(subquery_parameters.clone(), outer.clone());
    //   DependentJoin
    //    +         +
    //    +         +
    // subquery  Project
    //              +
    let fresh_column = subquery.free().iter().map(|c| c.id).max().unwrap_or(0) + 1;
    let rename_subquery_parameters: Vec<Column> = (0..subquery_parameters.len())
        .map(|i| Column {
            id: fresh_column + i as i64,
            name: subquery_parameters[i].name.clone(),
            table: subquery_parameters[i].table.clone(),
            typ: subquery_parameters[i].typ.clone(),
        })
        .collect();
    let dependent_join = LogicalJoin {
        parameters: subquery_parameters.clone(),
        join: Join::Inner(vec![]),
        left: subquery.clone(),
        right: Expr::new(project),
    };
    // Rename attributes of DependentJoin so LogicalJoin can reference both sides.
    let mut map_names: Vec<(Scalar, Column)> = dependent_join
        .attributes()
        .iter()
        .map(|c| {
            for i in 0..subquery_parameters.len() {
                if &subquery_parameters[i] == c {
                    return (
                        Scalar::Column(subquery_parameters[i].clone()),
                        rename_subquery_parameters[i].clone(),
                    );
                }
            }
            (Scalar::Column(c.clone()), c.clone())
        })
        .collect();
    map_names.sort_by(|(_, a), (_, b)| a.cmp(b));
    let map_dependent_join = LogicalMap(map_names, Expr::new(dependent_join));
    //             LogicalJoin
    //              +       +
    //              +       +
    //   DependentJoin     outer
    //    +         +
    let mut join_predicates: Vec<Scalar> = (0..subquery_parameters.len())
        .map(|i| {
            Scalar::Call(
                Function::Equal,
                vec![
                    Scalar::Column(subquery_parameters[i].clone()),
                    Scalar::Column(rename_subquery_parameters[i].clone()),
                ],
                Type::Bool,
            )
        })
        .collect();
    let join = match join {
        Join::Single(additional_predicates) => {
            for p in additional_predicates {
                join_predicates.push(p.clone());
            }
            Join::Single(join_predicates)
        }
        Join::Mark(mark, additional_predicates) => {
            for p in additional_predicates {
                join_predicates.push(p.clone());
            }
            Join::Mark(mark.clone(), join_predicates)
        }
        _ => panic!("{}", join),
    };
    let equi_join = Expr::new(LogicalJoin {
        join,
        parameters: vec![],
        left: Expr::new(map_dependent_join),
        right: outer.clone(),
    });
    // Push down dependent join.
    equi_join.top_down_rewrite(&|expr| {
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
    })
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
                RewriteRule::PushFilterThroughProject,
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
                RewriteRule::CombineConsecutiveProjects,
                RewriteRule::EmbedProjectIntoGet,
                RewriteRule::AggregateToProject,
                RewriteRule::RemoveMap,
            ],
        )
    })
}
