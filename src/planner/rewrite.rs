use encoding::*;
use node::*;

#[derive(Debug)]
enum RewriteRule {
    // Bottom-up unnest:
    MarkJoinToSemiJoin,
    SingleJoinToInnerJoin,
    RemoveInnerJoin,
    // Top-down predicate pushdown:
    PushExplicitFilterThroughInnerJoin,
    PushImplicitFilterThroughInnerJoin,
    PushExplicitFilterThroughRightJoin,
    PushImplicitFilterThroughRightJoin,
    PushFilterThroughProject,
    CombineConsecutiveFilters,
    // Top-down simplification:
    CombineConsecutiveProjects,
}

impl RewriteRule {
    fn apply(&self, expr: &Expr) -> Option<Expr> {
        match self {
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
                                    Expr::new(LogicalProject(
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
                                    Expr::new(LogicalProject(
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
                    if join_predicates.is_empty() {
                        if let LogicalSingleGet = left.as_ref() {
                            return Some(right.clone());
                        }
                        if let LogicalSingleGet = right.as_ref() {
                            return Some(left.clone());
                        }
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
                    if let LogicalProject(projects, input) = input.as_ref() {
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
            RewriteRule::CombineConsecutiveProjects => {
                if let LogicalProject(outer, input) = expr.as_ref() {
                    if let LogicalProject(inner, input) = input.as_ref() {
                        return combine_consecutive_projects(outer, inner, input);
                    }
                }
            }
        };
        None
    }
}

fn prove_singleton(expr: &Expr) -> bool {
    match expr.as_ref() {
        LogicalProject(_, input) => prove_singleton(input),
        LogicalSingleGet => true,
        LogicalAggregate { group_by, .. } => group_by.is_empty(),
        _ => false,
    }
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
            Expr::new(LogicalProject(projects.clone(), input.clone())),
        )));
    }
    Some(Expr::new(LogicalFilter(
        corr,
        Expr::new(LogicalProject(
            projects.clone(),
            Expr::new(LogicalFilter(uncorr, input.clone())),
        )),
    )))
}

fn pull_filter_through_aggregate(
    group_by: &Vec<Column>,
    aggregate: &Vec<(AggregateFn, Column)>,
    predicates: &Vec<Scalar>,
    input: &Expr,
) -> Option<Expr> {
    let (corr, uncorr) = split_correlated_predicates(predicates, input);
    // If there are no correlated predicates, pulling the filter upwards is pointless.
    // (pulling filters upwards is generally a bad idea, we only want to do it when we're trying to decorrelate a subquery)
    if corr.is_empty() {
        return None;
    }
    let mut group_by = group_by.clone();
    let aggregate = aggregate.clone();
    // We may need to project some additional attributes used by filter
    for x in &corr {
        for c in x.columns() {
            if !input.correlated(&c) && !group_by.contains(&c) {
                group_by.push(c.clone());
            }
        }
    }
    // If there are no uncorrelated predicates, remove the inner filter.
    if uncorr.is_empty() {
        return Some(Expr::new(LogicalFilter(
            corr,
            Expr::new(LogicalAggregate {
                group_by,
                aggregate,
                input: input.clone(),
            }),
        )));
    }
    // If there are correlated predicates, return both inner and outer filters.
    Some(Expr::new(LogicalFilter(
        corr,
        Expr::new(LogicalAggregate {
            group_by,
            aggregate,
            input: (Expr::new(LogicalFilter(uncorr, input.clone()))),
        }),
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
        if !p.columns().any(|c| left.correlated(c)) {
            left_predicates.push(p);
        } else if !p.columns().any(|c| right.correlated(c)) {
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

    let mut top = Expr::new(LogicalJoin(Join::Inner(remaining_predicates), left, right));
    top
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
        if !p.columns().any(|c| left.correlated(c)) {
            left_predicates.push(p);
        } else if !p.columns().any(|c| right.correlated(c)) {
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
    let can_push = |p: &Scalar| !p.columns().any(|c| right.correlated(c));
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
    let can_push = |p: &Scalar| !p.columns().any(|c| right.correlated(c));
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
        if p.columns().any(|c| input.correlated(c)) {
            outer.push(p.clone());
        } else {
            inner.push(p.clone());
        }
    }
    if inner.is_empty() {
        return None;
    }
    Some(maybe_filter(
        outer,
        Expr::new(LogicalProject(
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

fn combine_consecutive_projects(
    outer: &Vec<(Scalar, Column)>,
    inner: &Vec<(Scalar, Column)>,
    input: &Expr,
) -> Option<Expr> {
    if !inner.iter().all(|(x, _)| x.can_inline()) {
        return None;
    }
    let mut combined = vec![];
    for (outer_expr, outer_column) in outer {
        let mut outer_expr = outer_expr.clone();
        for (inner_expr, inner_column) in inner {
            outer_expr = outer_expr.inline(inner_expr, inner_column);
        }
        combined.push((outer_expr, outer_column.clone()));
    }
    Some(Expr::new(LogicalProject(combined, input.clone())))
}

fn split_correlated_predicates(
    predicates: &Vec<Scalar>,
    input: &Expr,
) -> (Vec<Scalar>, Vec<Scalar>) {
    let mut corr = vec![];
    let mut uncorr = vec![];
    for p in predicates {
        if p.columns().any(|c| input.correlated(c)) {
            corr.push(p.clone());
        } else {
            uncorr.push(p.clone());
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

pub fn rewrite(expr: Expr) -> Expr {
    let unnest = vec![
        RewriteRule::MarkJoinToSemiJoin,
        RewriteRule::SingleJoinToInnerJoin,
        RewriteRule::RemoveInnerJoin,
    ];
    let predicate_push_down = vec![
        RewriteRule::PushExplicitFilterThroughInnerJoin,
        RewriteRule::PushImplicitFilterThroughInnerJoin,
        RewriteRule::PushExplicitFilterThroughRightJoin,
        RewriteRule::PushImplicitFilterThroughRightJoin,
        RewriteRule::PushFilterThroughProject,
        RewriteRule::CombineConsecutiveFilters,
    ];
    let projection_push_down = vec![RewriteRule::CombineConsecutiveProjects];
    fn rewrite_all(expr: Expr, rules: &Vec<RewriteRule>) -> Expr {
        for rule in rules {
            match rule.apply(&expr) {
                // Abandon previous expr.
                Some(expr) => {
                    return rewrite_all(expr, rules);
                }
                None => (),
            }
        }
        expr
    }
    let expr = expr.bottom_up_rewrite(&|expr| rewrite_all(expr, &unnest));
    let expr = expr.top_down_rewrite(&|expr| rewrite_all(expr, &predicate_push_down));
    let expr = expr.top_down_rewrite(&|expr| rewrite_all(expr, &projection_push_down));
    expr
}
