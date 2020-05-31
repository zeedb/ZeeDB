use node::*;

#[derive(Debug)]
pub enum RewriteRule {
    // Bottom-up rewrite rules:
    PullFilterThroughJoin,
    PullFilterThroughProject,
    PullFilterThroughAggregate,
    RemoveSingleJoin,
    // Top-down rewrite rules:
    PushExplicitFilterThroughJoin,
    PushImplicitFilterThroughJoin,
    PushFilterThroughProject,
    CombineConsecutiveFilters,
    CombineConsecutiveProjects,
}

impl RewriteRule {
    pub fn call(&self, plan: &Expr) -> Option<Expr> {
        // TODO all the .0 / .1 are hard to follow, use pattern matching
        match self {
            RewriteRule::PullFilterThroughJoin => {
                if let LogicalJoin {
                    join,
                    predicates: join_predicates,
                    mark,
                } = &plan.0
                {
                    if let LogicalFilter(left_predicates) = &plan.1[0].0 {
                        return pull_filter_through_join(
                            join,
                            join_predicates,
                            mark,
                            left_predicates,
                            &plan.1[0].1[0],
                            &plan.1[1],
                        );
                    }
                }
            }
            RewriteRule::PullFilterThroughProject => {
                if let LogicalProject(projects) = &plan.0 {
                    if let LogicalFilter(predicates) = &plan.1[0].0 {
                        return pull_filter_through_project(projects, predicates, &plan.1[0].1[0]);
                    }
                }
            }
            RewriteRule::PullFilterThroughAggregate => {
                if let LogicalAggregate {
                    group_by,
                    aggregate,
                } = &plan.0
                {
                    if let LogicalFilter(predicates) = &plan.1[0].0 {
                        return pull_filter_through_aggregate(
                            group_by,
                            aggregate,
                            predicates,
                            &plan.1[0].1[0],
                        );
                    }
                }
            }
            RewriteRule::RemoveSingleJoin => {
                if let LogicalJoin {
                    join: Join::Single,
                    predicates,
                    mark: None,
                } = &plan.0
                {
                    if let LogicalProject(projects) = &plan.1[0].0 {
                        if let LogicalSingleGet = &plan.1[0].1[0].0 {
                            if predicates.is_empty() {
                                return Some(Expr(
                                    LogicalProject(projects.clone()),
                                    vec![plan.1[1].clone()],
                                ));
                            }
                        }
                    }
                }
            }
            RewriteRule::PushExplicitFilterThroughJoin => {
                if let LogicalFilter(filter_predicates) = &plan.0 {
                    if let LogicalJoin {
                        join,
                        predicates: join_predicates,
                        mark,
                    } = &plan.1[0].0
                    {
                        return push_explicit_filter_through_join(
                            filter_predicates,
                            join,
                            join_predicates,
                            mark,
                            &plan.1[0].1[0],
                            &plan.1[0].1[1],
                        );
                    }
                }
            }
            RewriteRule::PushImplicitFilterThroughJoin => {
                if let LogicalJoin {
                    join,
                    predicates,
                    mark,
                    ..
                } = &plan.0
                {
                    if predicates.len() > 0 {
                        return push_implicit_filter_through_join(
                            join, predicates, mark, &plan.1[0], &plan.1[1],
                        );
                    }
                }
            }
            RewriteRule::PushFilterThroughProject => {
                if let LogicalFilter(predicates) = &plan.0 {
                    if let LogicalProject(projects) = &plan.1[0].0 {
                        return push_filter_through_project(predicates, projects, &plan.1[0].1[0]);
                    }
                }
            }
            RewriteRule::CombineConsecutiveFilters => {
                if let LogicalFilter(predicates1) = &plan.0 {
                    if let LogicalFilter(predicates2) = &plan.1[0].0 {
                        return combine_consecutive_filters(
                            predicates1,
                            predicates2,
                            &plan.1[0].1[0],
                        );
                    }
                }
            }
            RewriteRule::CombineConsecutiveProjects => {
                if let LogicalProject(outer) = &plan.0 {
                    if let LogicalProject(inner) = &plan.1[0].0 {
                        return combine_consecutive_projects(outer, inner, &plan.1[0].1[0]);
                    }
                }
            }
        };
        None
    }
}

fn pull_filter_through_join(
    join: &Join,
    join_predicates: &Vec<Scalar>,
    mark: &Option<Column>,
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
    let predicates = combine_predicates(join_predicates, &corr);
    let join = LogicalJoin {
        join: join.clone(),
        predicates,
        mark: mark.clone(),
    };
    if uncorr.is_empty() {
        return Some(Expr(join, vec![left.clone(), right.clone()]));
    }
    Some(Expr(
        join,
        vec![
            Expr(LogicalFilter(uncorr), vec![left.clone()]),
            right.clone(),
        ],
    ))
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
        return Some(Expr(
            LogicalFilter(corr),
            vec![Expr(LogicalProject(projects.clone()), vec![input.clone()])],
        ));
    }
    return Some(Expr(
        LogicalFilter(corr),
        vec![Expr(
            LogicalProject(projects.clone()),
            vec![Expr(LogicalFilter(uncorr), vec![input.clone()])],
        )],
    ));
}

fn pull_filter_through_aggregate(
    group_by: &Vec<Column>,
    aggregate: &Vec<(Aggregate, Column)>,
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
                group_by.push(c);
            }
        }
    }
    // If there are no uncorrelated predicates, remove the inner filter.
    if uncorr.is_empty() {
        return Some(Expr(
            LogicalFilter(corr),
            vec![Expr(
                LogicalAggregate {
                    group_by,
                    aggregate,
                },
                vec![input.clone()],
            )],
        ));
    }
    // If there are correlated predicates, return both inner and outer filters.
    Some(Expr(
        LogicalFilter(corr),
        vec![Expr(
            LogicalAggregate {
                group_by,
                aggregate,
            },
            vec![Expr(LogicalFilter(uncorr), vec![input.clone()])],
        )],
    ))
}

fn push_explicit_filter_through_join(
    filter_predicates: &Vec<Scalar>,
    join: &Join,
    join_predicates: &Vec<Scalar>,
    mark: &Option<Column>,
    left: &Expr,
    right: &Expr,
) -> Option<Expr> {
    Some(Expr(
        LogicalJoin {
            join: join.clone(),
            predicates: combine_predicates(filter_predicates, join_predicates),
            mark: mark.clone(),
        },
        vec![left.clone(), right.clone()],
    ))
}

fn push_implicit_filter_through_join(
    join: &Join,
    predicates: &Vec<Scalar>,
    mark: &Option<Column>,
    left: &Expr,
    right: &Expr,
) -> Option<Expr> {
    let mut left_predicates = vec![];
    let mut right_predicates = vec![];
    let mut remaining_predicates = predicates.clone();
    for p in predicates {
        if !p.correlated(left) {
            left_predicates.push(p.clone());
        } else if !p.correlated(right) {
            right_predicates.push(p.clone());
        } else {
            remaining_predicates.push(p.clone());
        }
    }
    if left_predicates.is_empty() && right_predicates.is_empty() {
        return None;
    }
    let mut left = left.clone();
    if !left_predicates.is_empty() {
        left = Expr(LogicalFilter(left_predicates), vec![left]);
    }
    let mut right = right.clone();
    if !right_predicates.is_empty() {
        right = Expr(LogicalFilter(right_predicates), vec![right]);
    }
    Some(Expr(
        LogicalJoin {
            join: join.clone(),
            predicates: remaining_predicates,
            mark: mark.clone(),
        },
        vec![left, right],
    ))
}

fn push_filter_through_project(
    predicates: &Vec<Scalar>,
    projects: &Vec<(Scalar, Column)>,
    input: &Expr,
) -> Option<Expr> {
    let mut outer = vec![];
    let mut inner = vec![];
    for p in predicates {
        if p.correlated(input) {
            outer.push(p.clone());
        } else {
            inner.push(p.clone());
        }
    }
    if inner.is_empty() {
        None
    } else if outer.is_empty() {
        Some(Expr(
            LogicalProject(projects.clone()),
            vec![Expr(LogicalFilter(inner), vec![input.clone()])],
        ))
    } else {
        Some(Expr(
            LogicalFilter(outer),
            vec![Expr(
                LogicalProject(projects.clone()),
                vec![Expr(LogicalFilter(inner), vec![input.clone()])],
            )],
        ))
    }
}

fn combine_consecutive_filters(
    predicates1: &Vec<Scalar>,
    predicates2: &Vec<Scalar>,
    input: &Expr,
) -> Option<Expr> {
    let mut combined = vec![];
    for p in predicates1 {
        combined.push(p.clone());
    }
    for p in predicates2 {
        combined.push(p.clone());
    }
    Some(Expr(LogicalFilter(combined), vec![input.clone()]))
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
    for (inner_expr, inner_column) in inner {
        combined.push((inner_expr.clone(), inner_column.clone()));
    }
    Some(Expr(LogicalProject(combined), vec![input.clone()]))
}

fn split_correlated_predicates(
    predicates: &Vec<Scalar>,
    input: &Expr,
) -> (Vec<Scalar>, Vec<Scalar>) {
    let mut corr = vec![];
    let mut uncorr = vec![];
    for p in predicates {
        if p.correlated(input) {
            corr.push(p.clone());
        } else {
            uncorr.push(p.clone());
        }
    }
    (corr, uncorr)
}

fn combine_predicates(predicates1: &Vec<Scalar>, predicates2: &Vec<Scalar>) -> Vec<Scalar> {
    let mut combined = Vec::with_capacity(predicates1.len() + predicates2.len());
    for p in predicates1 {
        combined.push(p.clone());
    }
    for p in predicates2 {
        combined.push(p.clone());
    }
    combined
}

pub fn bottom_up_rules() -> Vec<RewriteRule> {
    vec![
        RewriteRule::PullFilterThroughJoin,
        RewriteRule::PullFilterThroughProject,
        RewriteRule::PullFilterThroughAggregate,
        RewriteRule::RemoveSingleJoin,
    ]
}

pub fn top_down_rules() -> Vec<RewriteRule> {
    vec![
        RewriteRule::PushExplicitFilterThroughJoin,
        RewriteRule::PushImplicitFilterThroughJoin,
        RewriteRule::PushFilterThroughProject,
        RewriteRule::CombineConsecutiveFilters,
        RewriteRule::CombineConsecutiveProjects,
    ]
}

pub fn bottom_up(plan: &Expr, rules: &Vec<RewriteRule>) -> Expr {
    // Optimize inputs first.
    let mut inputs = Vec::with_capacity(plan.1.len());
    for input in &plan.1 {
        inputs.push(bottom_up(input, rules));
    }
    let plan = Expr(plan.0.clone(), inputs);
    // Optimize operator.
    for rule in rules {
        match rule.call(&plan) {
            // Abandon previous plan.
            Some(plan) => return bottom_up(&plan, rules),
            None => (),
        }
    }
    plan
}

pub fn top_down(plan: &Expr, rules: &Vec<RewriteRule>) -> Expr {
    // Optimize operator.
    for rule in rules {
        match rule.call(&plan) {
            // Abandon previous plan.
            Some(plan) => {
                return top_down(&plan, rules);
            }
            None => (),
        }
    }
    // Optimize inputs.
    let mut inputs = Vec::with_capacity(plan.1.len());
    for input in &plan.1 {
        inputs.push(top_down(input, rules));
    }
    Expr(plan.0.clone(), inputs)
}
