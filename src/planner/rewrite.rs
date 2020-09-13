use node::*;

#[derive(Debug)]
enum RewriteRule {
    // Bottom-up rewrite rules:
    PullFilterThroughJoin,
    PullFilterThroughProject,
    PullFilterThroughAggregate,
    RemoveSingleJoin,
    // Top-down rewrite rules:
    PushFilterThroughJoin,
    PushFilterThroughProject,
    CombineConsecutiveFilters,
    CombineConsecutiveProjects,
}

impl RewriteRule {
    fn call(&self, expr: &Expr) -> Option<Expr> {
        match self {
            RewriteRule::PullFilterThroughJoin => {
                if let Expr(LogicalJoin(join, left, right)) = expr {
                    if let Expr(LogicalFilter(left_predicates, left)) = left.as_ref() {
                        return pull_filter_through_join(join, left_predicates, left, right);
                    }
                }
            }
            RewriteRule::PullFilterThroughProject => {
                if let Expr(LogicalProject(projects, input)) = expr {
                    if let Expr(LogicalFilter(predicates, input)) = input.as_ref() {
                        return pull_filter_through_project(projects, predicates, input);
                    }
                }
            }
            RewriteRule::PullFilterThroughAggregate => {
                if let Expr(LogicalAggregate {
                    group_by,
                    aggregate,
                    input,
                }) = expr
                {
                    if let Expr(LogicalFilter(predicates, input)) = input.as_ref() {
                        return pull_filter_through_aggregate(
                            group_by, aggregate, predicates, input,
                        );
                    }
                }
            }
            RewriteRule::RemoveSingleJoin => {
                if let Expr(LogicalJoin(Join::Single, left, right)) = expr {
                    if let Expr(LogicalProject(projects, left)) = left.as_ref() {
                        if let Expr(LogicalSingleGet) = left.as_ref() {
                            return Some(Expr(LogicalProject(projects.clone(), right.clone())));
                        }
                    }
                }
            }
            RewriteRule::PushFilterThroughJoin => {
                if let Expr(LogicalFilter(filter_predicates, input)) = expr {
                    if let Expr(LogicalJoin(join, left, right)) = input.as_ref() {
                        return push_filter_through_join(filter_predicates, join, left, right);
                    }
                }
            }
            RewriteRule::PushFilterThroughProject => {
                if let Expr(LogicalFilter(predicates, input)) = expr {
                    if let Expr(LogicalProject(projects, input)) = input.as_ref() {
                        return push_filter_through_project(predicates, projects, input);
                    }
                }
            }
            RewriteRule::CombineConsecutiveFilters => {
                if let Expr(LogicalFilter(outer, input)) = expr {
                    if let Expr(LogicalFilter(inner, input)) = input.as_ref() {
                        return combine_consecutive_filters(outer, inner, input);
                    }
                }
            }
            RewriteRule::CombineConsecutiveProjects => {
                if let Expr(LogicalProject(outer, input)) = expr {
                    if let Expr(LogicalProject(inner, input)) = input.as_ref() {
                        return combine_consecutive_projects(outer, inner, input);
                    }
                }
            }
        };
        None
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
        return Some(Expr(LogicalFilter(
            corr,
            Box::new(Expr(LogicalJoin(
                join.clone(),
                Box::new(left.clone()),
                Box::new(right.clone()),
            ))),
        )));
    }
    Some(Expr(LogicalFilter(
        corr,
        Box::new(Expr(LogicalJoin(
            join.clone(),
            Box::new(Expr(LogicalFilter(uncorr, Box::new(left.clone())))),
            Box::new(right.clone()),
        ))),
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
        return Some(Expr(LogicalFilter(
            corr,
            Box::new(Expr(LogicalProject(
                projects.clone(),
                Box::new(input.clone()),
            ))),
        )));
    }
    Some(Expr(LogicalFilter(
        corr,
        Box::new(Expr(LogicalProject(
            projects.clone(),
            Box::new(Expr(LogicalFilter(uncorr, Box::new(input.clone())))),
        ))),
    )))
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
                group_by.push(c.clone());
            }
        }
    }
    // If there are no uncorrelated predicates, remove the inner filter.
    if uncorr.is_empty() {
        return Some(Expr(LogicalFilter(
            corr,
            Box::new(Expr(LogicalAggregate {
                group_by,
                aggregate,
                input: Box::new(input.clone()),
            })),
        )));
    }
    // If there are correlated predicates, return both inner and outer filters.
    Some(Expr(LogicalFilter(
        corr,
        Box::new(Expr(LogicalAggregate {
            group_by,
            aggregate,
            input: Box::new(Expr(LogicalFilter(uncorr, Box::new(input.clone())))),
        })),
    )))
}

fn push_filter_through_join(
    predicates: &Vec<Scalar>,
    join: &Join,
    left: &Expr,
    right: &Expr,
) -> Option<Expr> {
    let mut left_predicates = vec![];
    let mut right_predicates = vec![];
    let mut remaining_predicates = predicates.clone();
    for p in predicates {
        if !p.columns().any(|c| left.correlated(c)) {
            left_predicates.push(p.clone());
        } else if !p.columns().any(|c| right.correlated(c)) {
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
        left = Expr(LogicalFilter(left_predicates, Box::new(left)));
    }
    let mut right = right.clone();
    if !right_predicates.is_empty() {
        right = Expr(LogicalFilter(right_predicates, Box::new(right)));
    }
    let mut top = Expr(LogicalJoin(join.clone(), Box::new(left), Box::new(right)));
    if !remaining_predicates.is_empty() {
        top = Expr(LogicalFilter(remaining_predicates, Box::new(top)));
    }
    Some(top)
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
        None
    } else if outer.is_empty() {
        Some(Expr(LogicalProject(
            projects.clone(),
            Box::new(Expr(LogicalFilter(inner, Box::new(input.clone())))),
        )))
    } else {
        Some(Expr(LogicalFilter(
            outer,
            Box::new(Expr(LogicalProject(
                projects.clone(),
                Box::new(Expr(LogicalFilter(inner, Box::new(input.clone())))),
            ))),
        )))
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
    Some(Expr(LogicalFilter(combined, Box::new(input.clone()))))
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
    Some(Expr(LogicalProject(combined, Box::new(input.clone()))))
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
    let bottom_up_rules = vec![
        RewriteRule::PullFilterThroughJoin,
        RewriteRule::PullFilterThroughProject,
        RewriteRule::PullFilterThroughAggregate,
        RewriteRule::RemoveSingleJoin,
    ];
    let top_down_rules = vec![
        RewriteRule::PushFilterThroughJoin,
        RewriteRule::PushFilterThroughProject,
        RewriteRule::CombineConsecutiveFilters,
        RewriteRule::CombineConsecutiveProjects,
    ];
    fn rewrite_all(expr: Expr, rules: &Vec<RewriteRule>) -> Expr {
        for rule in rules {
            match rule.call(&expr) {
                // Abandon previous expr.
                Some(expr) => {
                    return rewrite_all(expr, rules);
                }
                None => (),
            }
        }
        expr
    }
    let expr = expr.bottom_up_rewrite(&|expr| rewrite_all(expr, &bottom_up_rules));
    let expr = expr.top_down_rewrite(&|expr| rewrite_all(expr, &top_down_rules));
    expr
}
