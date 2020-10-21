use encoding::*;
use node::*;

#[derive(Debug)]
enum RewriteRule {
    // Unnest meta rule:
    PushDependentJoin,
    MarkJoinToSemiJoin,
    SingleJoinToInnerJoin,
    RemoveInnerJoin,
    RemoveWith,
    // Unnesting implementation rules:
    PushDependentJoinThroughFilter,
    PushDependentJoinThroughMap,
    PushDependentJoinThroughJoin,
    PushDependentJoinThroughWith,
    PushDependentJoinThroughAggregate,
    PushDependentJoinThroughLimit,
    PushDependentJoinThroughSort,
    PushDependentJoinThroughSetOperation,
    DependentJoinToInnerJoin,
    // Join simplification:
    // Predicate pushdown:
    PushExplicitFilterThroughInnerJoin,
    PushImplicitFilterThroughInnerJoin,
    PushExplicitFilterThroughRightJoin,
    PushImplicitFilterThroughRightJoin,
    PushFilterThroughProject,
    CombineConsecutiveFilters,
    EmbedFilterIntoGet,
    // Projection simplification:
    CombineConsecutiveProjects,
    EmbedProjectIntoGet,
}

impl RewriteRule {
    fn apply(&self, expr: &Expr) -> Option<Expr> {
        match self {
            RewriteRule::PushDependentJoin => {
                if let LogicalDependentJoin { .. } = expr.as_ref() {
                    return Some(unnest_one(expr.clone()));
                }
            }
            RewriteRule::PushDependentJoinThroughFilter => {
                if let LogicalDependentJoin {
                    parameters,
                    left: subquery,
                    right: domain,
                } = expr.as_ref()
                {
                    if let LogicalFilter(predicates, subquery) = subquery.as_ref() {
                        return Some(Expr::new(LogicalFilter(
                            predicates.clone(),
                            push_dependent_join(parameters, subquery, domain),
                        )));
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughMap => {
                if let LogicalDependentJoin {
                    parameters,
                    left: subquery,
                    right: domain,
                } = expr.as_ref()
                {
                    if let LogicalMap(projects, subquery) = subquery.as_ref() {
                        return Some(Expr::new(LogicalMap(
                            projects.clone(),
                            push_dependent_join(parameters, subquery, domain),
                        )));
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughJoin => {
                if let LogicalDependentJoin {
                    parameters,
                    left: subquery,
                    right: domain,
                } = expr.as_ref()
                {
                    if let LogicalJoin(join, left_subquery, right_subquery) = subquery.as_ref() {
                        match join {
                            Join::Inner(predicates) => {
                                if free_parameters(parameters, left_subquery).is_empty() {
                                    return Some(Expr::new(LogicalJoin(
                                        Join::Inner(predicates.clone()),
                                        left_subquery.clone(),
                                        push_dependent_join(parameters, right_subquery, domain),
                                    )));
                                } else if free_parameters(parameters, right_subquery).is_empty() {
                                    return Some(Expr::new(LogicalJoin(
                                        Join::Inner(predicates.clone()),
                                        push_dependent_join(parameters, left_subquery, domain),
                                        right_subquery.clone(),
                                    )));
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
            // TODO what happens when a dependent join sits on top of another dependent join? Need a bottom-up transform?
            RewriteRule::PushDependentJoinThroughWith => {
                if let LogicalDependentJoin {
                    parameters,
                    left: subquery,
                    right: domain,
                } = expr.as_ref()
                {
                    if let LogicalWith(name, columns, left_left, left_right) = subquery.as_ref() {
                        todo!()
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughAggregate => {
                if let LogicalDependentJoin {
                    parameters,
                    left: subquery,
                    right: domain,
                } = expr.as_ref()
                {
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
                if let LogicalDependentJoin {
                    parameters,
                    left: subquery,
                    right: domain,
                } = expr.as_ref()
                {
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
                    left: subquery,
                    right: domain,
                } = expr.as_ref()
                {
                    if let LogicalSort(order_by, subquery) = subquery.as_ref() {
                        todo!()
                    }
                }
            }
            RewriteRule::PushDependentJoinThroughSetOperation => {
                if let LogicalDependentJoin {
                    parameters,
                    left: subquery,
                    right: domain,
                } = expr.as_ref()
                {
                    match subquery.as_ref() {
                        LogicalUnion(left_subquery, right_subquery) => todo!(),
                        LogicalIntersect(left_subquery, right_subquery) => todo!(),
                        LogicalExcept(left_subquery, right_subquery) => todo!(),
                        _ => {}
                    }
                }
            }
            RewriteRule::DependentJoinToInnerJoin => {
                if let LogicalDependentJoin {
                    parameters,
                    left: subquery,
                    right: domain,
                } = expr.as_ref()
                {
                    if parameters.is_empty() {
                        return Some(Expr::new(LogicalJoin(
                            Join::Inner(vec![]),
                            subquery.clone(),
                            domain.clone(),
                        )));
                    }
                }
            }
            RewriteRule::MarkJoinToSemiJoin => {
                if let LogicalFilter(filter_predicates, input) = expr.as_ref() {
                    if let LogicalJoin(Join::Mark(mark, join_predicates), left, right) =
                        input.as_ref()
                    {
                        let mut filter_predicates = filter_predicates.clone();
                        let semi = Scalar::Column(mark.clone());
                        let anti = Scalar::Call(Function::Not, vec![semi.clone()], Type::Bool);
                        for i in 0..filter_predicates.len() {
                            if filter_predicates[i] == semi {
                                filter_predicates.remove(i);
                                return Some(maybe_filter(
                                    filter_predicates,
                                    Expr::new(LogicalMap(
                                        vec![(
                                            Scalar::Literal(Value::Bool(true), Type::Bool),
                                            mark.clone(),
                                        )],
                                        Expr::new(LogicalJoin(
                                            Join::Semi(join_predicates.clone()),
                                            left.clone(),
                                            right.clone(),
                                        )),
                                    )),
                                ));
                            } else if filter_predicates[i] == anti {
                                filter_predicates.remove(i);
                                return Some(maybe_filter(
                                    filter_predicates,
                                    Expr::new(LogicalMap(
                                        vec![(
                                            Scalar::Literal(Value::Bool(true), Type::Bool),
                                            mark.clone(),
                                        )],
                                        Expr::new(LogicalJoin(
                                            Join::Anti(join_predicates.clone()),
                                            left.clone(),
                                            right.clone(),
                                        )),
                                    )),
                                ));
                            }
                        }
                    }
                }
            }
            RewriteRule::SingleJoinToInnerJoin => {
                if let LogicalJoin(Join::Single(join_predicates), left, right) = expr.as_ref() {
                    if join_predicates.is_empty() && prove_singleton(left) {
                        return Some(Expr::new(LogicalJoin(
                            Join::Inner(vec![]),
                            left.clone(),
                            right.clone(),
                        )));
                    }
                }
            }
            RewriteRule::RemoveInnerJoin => {
                if let LogicalJoin(Join::Inner(join_predicates), left, right) = expr.as_ref() {
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
            RewriteRule::PushExplicitFilterThroughInnerJoin => {
                if let LogicalFilter(filter_predicates, input) = expr.as_ref() {
                    if let LogicalJoin(Join::Inner(join_predicates), left, right) = input.as_ref() {
                        return Some(push_explicit_filter_through_inner_join(
                            filter_predicates,
                            join_predicates,
                            left,
                            right,
                        ));
                    }
                }
            }
            RewriteRule::PushImplicitFilterThroughInnerJoin => {
                if let LogicalJoin(Join::Inner(join_predicates), left, right) = expr.as_ref() {
                    return push_implicit_filter_through_inner_join(join_predicates, left, right);
                }
            }
            RewriteRule::PushExplicitFilterThroughRightJoin => {
                if let LogicalFilter(filter_predicates, input) = expr.as_ref() {
                    if let LogicalJoin(Join::Right(join_predicates), left, right) = input.as_ref() {
                        return push_explicit_filter_through_right_join(
                            filter_predicates,
                            join_predicates,
                            left,
                            right,
                        );
                    }
                }
            }
            RewriteRule::PushImplicitFilterThroughRightJoin => {
                if let LogicalJoin(Join::Right(join_predicates), left, right) = expr.as_ref() {
                    return push_implicit_filter_through_right_join(join_predicates, left, right);
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
                    if let LogicalMap(inner, input) = input.as_ref() {
                        let combined = inline_all(outer, inner);
                        return Some(Expr::new(LogicalMap(combined, input.clone())));
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
                        let mut stuck = vec![];
                        let mut inner = inner.clone();
                        for (x, c) in outer {
                            // TODO in theory we can do renaming in the Get operator
                            if x.is_just(c) {
                                if !inner.contains(c) {
                                    inner.push(c.clone());
                                }
                            } else {
                                stuck.push((x.clone(), c.clone()));
                            }
                        }
                        if stuck.is_empty() {
                            return Some(Expr::new(LogicalGet {
                                projects: inner,
                                predicates: predicates.clone(),
                                table: table.clone(),
                            }));
                        } else if stuck.len() < outer.len() {
                            return Some(Expr::new(LogicalMap(
                                stuck,
                                Expr::new(LogicalGet {
                                    projects: inner,
                                    predicates: predicates.clone(),
                                    table: table.clone(),
                                }),
                            )));
                        }
                    }
                }
            }
        };
        None
    }
}

fn push_dependent_join(parameters: &Vec<Column>, subquery: &Expr, domain: &Expr) -> Expr {
    Expr::new(LogicalDependentJoin {
        parameters: free_parameters(parameters, subquery),
        left: subquery.clone(),
        right: domain.clone(),
    })
}

pub fn free_parameters(parameters: &Vec<Column>, subquery: &Expr) -> Vec<Column> {
    let free = subquery.free(parameters);
    parameters
        .iter()
        .filter(|p| free.contains(p))
        .map(|p| p.clone())
        .collect()
}

fn prove_singleton(expr: &Expr) -> bool {
    match expr.as_ref() {
        LogicalMap(_, input) => prove_singleton(input),
        LogicalSingleGet => true,
        LogicalAggregate { group_by, .. } => group_by.is_empty(),
        _ => false,
    }
}

fn remove_inner_join_left(left: Expr, right: Expr) -> Option<Expr> {
    match *left.0 {
        LogicalMap(projects, left) => {
            remove_inner_join_left(left, Expr::new(LogicalMap(projects, right)))
        }
        LogicalProject(projects, left) => {
            remove_inner_join_left(left, Expr::new(LogicalProject(projects, right)))
        }
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

fn pull_filter_through_join(
    join: &Join,
    left_predicates: &Vec<Scalar>,
    left: &Expr,
    right: &Expr,
) -> Option<Expr> {
    let (corr, uncorr) = split_correlated_predicates(left_predicates, left);
    // If there are no correlated predicates, pulling the filter upwards is pointless.
    // (pulling filters upwards is generally a bad idea, we only want to do it when we're trying to decorrelate a subquery)
    if corr.is_empty() {
        return None;
    }
    if uncorr.is_empty() {
        return Some(Expr::new(LogicalFilter(
            corr,
            Expr::new(LogicalJoin(join.clone(), left.clone(), right.clone())),
        )));
    }
    Some(Expr::new(LogicalFilter(
        corr,
        Expr::new(LogicalJoin(
            join.clone(),
            Expr::new(LogicalFilter(uncorr, left.clone())),
            right.clone(),
        )),
    )))
}

fn pull_filter_through_project(
    projects: &Vec<(Scalar, Column)>,
    predicates: &Vec<Scalar>,
    input: &Expr,
) -> Option<Expr> {
    let (corr, uncorr) = split_correlated_predicates(predicates, input);
    // If there are no correlated predicates, pulling the filter upwards is pointless.
    // (pulling filters upwards is generally a bad idea, we only want to do it when we're trying to decorrelate a subquery)
    if corr.is_empty() {
        return None;
    }
    if uncorr.is_empty() {
        return Some(Expr::new(LogicalFilter(
            corr,
            Expr::new(LogicalMap(projects.clone(), input.clone())),
        )));
    }
    Some(Expr::new(LogicalFilter(
        corr,
        Expr::new(LogicalMap(
            projects.clone(),
            Expr::new(LogicalFilter(uncorr, input.clone())),
        )),
    )))
}

fn push_explicit_filter_through_inner_join(
    filter_predicates: &Vec<Scalar>,
    join_predicates: &Vec<Scalar>,
    left: &Expr,
    right: &Expr,
) -> Expr {
    let mut left_predicates = vec![];
    let mut right_predicates = vec![];
    let mut remaining_predicates = vec![];
    let mut distribute_predicate = |p: Scalar| {
        if p.free().is_subset(&left.attributes()) {
            left_predicates.push(p);
        } else if p.free().is_subset(&right.attributes()) {
            right_predicates.push(p);
        } else {
            remaining_predicates.push(p);
        }
    };
    for p in filter_predicates.clone() {
        distribute_predicate(p);
    }
    for p in join_predicates.clone() {
        distribute_predicate(p);
    }
    let mut left = left.clone();
    if !left_predicates.is_empty() {
        left = Expr::new(LogicalFilter(left_predicates, left));
    }
    let mut right = right.clone();
    if !right_predicates.is_empty() {
        right = Expr::new(LogicalFilter(right_predicates, right));
    }
    Expr::new(LogicalJoin(Join::Inner(remaining_predicates), left, right))
}

fn push_implicit_filter_through_inner_join(
    join_predicates: &Vec<Scalar>,
    left: &Expr,
    right: &Expr,
) -> Option<Expr> {
    let mut left_predicates = vec![];
    let mut right_predicates = vec![];
    let mut remaining_predicates = vec![];
    for p in join_predicates.clone() {
        if p.free().is_subset(&left.attributes()) {
            left_predicates.push(p);
        } else if p.free().is_subset(&right.attributes()) {
            right_predicates.push(p);
        } else {
            remaining_predicates.push(p);
        }
    }
    if left_predicates.is_empty() && right_predicates.is_empty() {
        return None;
    }
    let mut left = left.clone();
    if !left_predicates.is_empty() {
        left = Expr::new(LogicalFilter(left_predicates, left));
    }
    let mut right = right.clone();
    if !right_predicates.is_empty() {
        right = Expr::new(LogicalFilter(right_predicates, right));
    }
    Some(Expr::new(LogicalJoin(
        Join::Inner(remaining_predicates.clone()),
        left,
        right,
    )))
}

fn push_explicit_filter_through_right_join(
    filter_predicates: &Vec<Scalar>,
    join_predicates: &Vec<Scalar>,
    left: &Expr,
    right: &Expr,
) -> Option<Expr> {
    let mut remaining_filter_predicates = vec![];
    let mut remaining_join_predicates = vec![];
    let mut pushed_predicates = vec![];
    let can_push = |p: &Scalar| p.free().is_subset(&right.attributes());
    for p in filter_predicates {
        if can_push(p) {
            pushed_predicates.push(p.clone());
        } else {
            remaining_filter_predicates.push(p.clone());
        }
    }
    for p in join_predicates {
        if can_push(p) {
            pushed_predicates.push(p.clone());
        } else {
            remaining_join_predicates.push(p.clone());
        }
    }
    if pushed_predicates.is_empty() {
        return None;
    }
    Some(maybe_filter(
        remaining_filter_predicates,
        Expr::new(LogicalJoin(
            Join::Right(remaining_join_predicates),
            left.clone(),
            Expr::new(LogicalFilter(pushed_predicates, right.clone())),
        )),
    ))
}

fn push_implicit_filter_through_right_join(
    join_predicates: &Vec<Scalar>,
    left: &Expr,
    right: &Expr,
) -> Option<Expr> {
    let mut remaining_join_predicates = vec![];
    let mut pushed_predicates = vec![];
    let can_push = |p: &Scalar| p.free().is_subset(&right.attributes());
    for p in join_predicates {
        if can_push(p) {
            pushed_predicates.push(p.clone());
        } else {
            remaining_join_predicates.push(p.clone());
        }
    }
    if pushed_predicates.is_empty() {
        return None;
    }
    Some(Expr::new(LogicalJoin(
        Join::Right(remaining_join_predicates),
        left.clone(),
        Expr::new(LogicalFilter(pushed_predicates, right.clone())),
    )))
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

fn inline_all(
    outer: &Vec<(Scalar, Column)>,
    inner: &Vec<(Scalar, Column)>,
) -> Vec<(Scalar, Column)> {
    let mut combined = vec![];
    for (outer_expr, outer_column) in outer {
        let mut outer_expr = outer_expr.clone();
        for (inner_expr, inner_column) in inner {
            outer_expr = outer_expr.inline(inner_expr, inner_column);
        }
        combined.push((outer_expr, outer_column.clone()));
    }
    combined
}

fn split_correlated_predicates(
    predicates: &Vec<Scalar>,
    input: &Expr,
) -> (Vec<Scalar>, Vec<Scalar>) {
    let mut corr = vec![];
    let mut uncorr = vec![];
    for p in predicates {
        if p.free().is_subset(&input.attributes()) {
            uncorr.push(p.clone());
        } else {
            corr.push(p.clone());
        }
    }
    (corr, uncorr)
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
    let expr = unnest_all(expr);
    let expr = optimize_join_type(expr);
    let expr = predicate_push_down(expr);
    let expr = projection_push_down(expr);
    expr
}

// Unnest all dependent joins, and simplify joins where possible.
fn unnest_all(expr: Expr) -> Expr {
    expr.bottom_up_rewrite(&|expr| apply_all(expr, &vec![RewriteRule::PushDependentJoin]))
}

// Unnest one dependent join, assuming there are no dependent joins under it.
fn unnest_one(expr: Expr) -> Expr {
    expr.top_down_rewrite(&|expr| {
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
                RewriteRule::DependentJoinToInnerJoin,
            ],
        )
    })
}

fn optimize_join_type(expr: Expr) -> Expr {
    expr.top_down_rewrite(&|expr| {
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
                RewriteRule::PushExplicitFilterThroughInnerJoin,
                RewriteRule::PushImplicitFilterThroughInnerJoin,
                RewriteRule::PushExplicitFilterThroughRightJoin,
                RewriteRule::PushImplicitFilterThroughRightJoin,
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
            ],
        )
    })
}
