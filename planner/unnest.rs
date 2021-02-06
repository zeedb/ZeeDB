use std::collections::{HashMap, HashSet};

use ast::*;

/// Unnest arbitrary subqueries using the "general unnesting" strategy described in
///   "Unnesting Arbitrary Queries" Neumann and Kemper
///   http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Neumann-Unnesting_Arbitrary_Querie.pdf
/// Some additional details are given in Mark Raasveldt's talk:
///   https://www.youtube.com/watch?v=ajpg_pMX620
///   Slides https://drive.google.com/file/d/17_sVIwwxFM5RZB5McQZ8dzT8JvOHZuAq/view
/// And in the implementation of DuckDB:
///   https://github.com/cwida/duckdb/blob/master/src/planner/subquery/flatten_dependent_join.cpp
pub(crate) fn unnest_dependent_joins(expr: Expr) -> Result<Expr, Expr> {
    match &expr {
        LogicalDependentJoin {
            parameters,
            subquery,
            ..
        } if !free_parameters(parameters, subquery).is_empty() => {
            Ok(expr.top_down_rewrite(&|expr| push_dependent_join(expr)))
        }
        _ => Err(expr),
    }
}

fn push_dependent_join(expr: Expr) -> Expr {
    match expr {
        LogicalDependentJoin {
            parameters,
            predicates: join_predicates,
            subquery,
            domain,
        } if !free_parameters(&parameters, &subquery).is_empty() => {
            // PushExplicitFilterIntoInnerJoin hasn't yet run, so predicates should be empty.
            // We won't bother substituting fresh column names in parameters.
            assert!(join_predicates.is_empty());

            match *subquery {
                LogicalFilter {
                    predicates: filter_predicates,
                    input: subquery,
                } => LogicalFilter {
                    predicates: filter_predicates,
                    input: Box::new(LogicalDependentJoin {
                        parameters,
                        predicates: join_predicates,
                        subquery,
                        domain,
                    }),
                },
                LogicalMap {
                    include_existing,
                    mut projects,
                    input: subquery,
                } => {
                    for p in &parameters {
                        if !projects.iter().any(|(_, c)| c == p) {
                            projects.push((Scalar::Column(p.clone()), p.clone()));
                        }
                    }
                    LogicalMap {
                        include_existing,
                        projects,
                        input: Box::new(LogicalDependentJoin {
                            parameters,
                            predicates: join_predicates,
                            subquery,
                            domain,
                        }),
                    }
                }
                LogicalJoin {
                    join,
                    left: left_subquery,
                    right: right_subquery,
                } => match join {
                    Join::Inner(_) => {
                        if free_parameters(&parameters, &left_subquery).is_empty() {
                            push_right(parameters, join, left_subquery, right_subquery, domain)
                        } else if free_parameters(&parameters, &right_subquery).is_empty() {
                            push_left(parameters, join, left_subquery, right_subquery, domain)
                        } else {
                            push_both(parameters, join, left_subquery, right_subquery, domain)
                        }
                    }
                    Join::Right(_)
                    | Join::Semi(_)
                    | Join::Anti(_)
                    | Join::Single(_)
                    | Join::Mark(_, _) => {
                        if free_parameters(&parameters, &left_subquery).is_empty() {
                            push_right(parameters, join, left_subquery, right_subquery, domain)
                        } else {
                            push_both(parameters, join, left_subquery, right_subquery, domain)
                        }
                    }
                    Join::Outer(_) => {
                        push_both(parameters, join, left_subquery, right_subquery, domain)
                    }
                },
                LogicalWith { .. } => panic!("WITH is not supported in correlated subqueries"),
                LogicalAggregate {
                    group_by,
                    aggregate,
                    input: subquery,
                } => {
                    let mut group_by = group_by;
                    for c in &parameters {
                        group_by.push(c.clone());
                    }
                    LogicalAggregate {
                        group_by,
                        aggregate,
                        input: Box::new(LogicalDependentJoin {
                            parameters,
                            predicates: join_predicates,
                            subquery,
                            domain,
                        }),
                    }
                }
                LogicalLimit {
                    limit,
                    offset,
                    input: subquery,
                } => {
                    if offset > 0 {
                        panic!("OFFSET is not supported in correlated subquery");
                    }
                    // Limit 0 means we ignore the result of the subquery.
                    if limit == 0 {
                        LogicalLimit {
                            limit: 0,
                            offset: 0,
                            input: Box::new(LogicalDependentJoin {
                                parameters,
                                predicates: join_predicates,
                                subquery,
                                domain,
                            }),
                        }
                    // Limit 1 can be converted to ANY_VALUE, which can be unnested:
                    } else if limit == 1 {
                        LogicalDependentJoin {
                            parameters,
                            predicates: join_predicates,
                            subquery: Box::new(LogicalAggregate {
                                group_by: vec![],
                                aggregate: any_value(subquery.attributes()),
                                input: subquery,
                            }),
                            domain,
                        }
                    } else {
                        // LIMIT N may actually be a no-op.
                        // Either way, it's best to just make the user take it out.
                        panic!("Only LIMIT 0 and LIMIT 1 are supported in subqueries");
                    }
                }
                LogicalSort { .. } => {
                    panic!("ORDER BY is not supported in correlated subqueries");
                }
                LogicalUnion {
                    left: left_subquery,
                    right: right_subquery,
                } => LogicalUnion {
                    left: Box::new(LogicalDependentJoin {
                        parameters: parameters.clone(),
                        predicates: join_predicates.clone(),
                        subquery: left_subquery,
                        domain: domain.clone(),
                    }),
                    right: Box::new(LogicalDependentJoin {
                        parameters: parameters.clone(),
                        predicates: join_predicates.clone(),
                        subquery: right_subquery,
                        domain: domain.clone(),
                    }),
                },
                _ => LogicalDependentJoin {
                    parameters,
                    predicates: join_predicates,
                    subquery,
                    domain,
                },
            }
        }
        _ => expr,
    }
}

pub fn free_parameters(parameters: &Vec<Column>, subquery: &Expr) -> Vec<Column> {
    let free = subquery.references();
    parameters
        .iter()
        .map(|p| p.clone())
        .filter(|p| free.contains(p))
        .collect()
}

fn push_right(
    parameters: Vec<Column>,
    join: Join,
    left_subquery: Box<Expr>,
    right_subquery: Box<Expr>,
    domain: Box<Expr>,
) -> Expr {
    LogicalJoin {
        join,
        left: left_subquery,
        right: Box::new(LogicalDependentJoin {
            parameters,
            predicates: vec![],
            subquery: right_subquery,
            domain,
        }),
    }
}

fn push_left(
    parameters: Vec<Column>,
    join: Join,
    left_subquery: Box<Expr>,
    right_subquery: Box<Expr>,
    domain: Box<Expr>,
) -> Expr {
    LogicalJoin {
        join,
        left: Box::new(LogicalDependentJoin {
            parameters,
            predicates: vec![],
            subquery: left_subquery,
            domain,
        }),
        right: right_subquery,
    }
}

fn push_both(
    parameters: Vec<Column>,
    join: Join,
    left_subquery: Box<Expr>,
    right_subquery: Box<Expr>,
    domain: Box<Expr>,
) -> Expr {
    // Substitute fresh column names for the left side subquery.
    let left_parameters: Vec<_> = parameters.iter().map(Column::fresh).collect();
    let left_parameters_map: HashMap<_, _> = (0..parameters.len())
        .map(|i| (parameters[i].clone(), left_parameters[i].clone()))
        .collect();
    let left_subquery = left_subquery.subst(&left_parameters_map);
    let left_domain = domain.clone().subst(&left_parameters_map);
    // Add natural-join on domain to the top join predicates.
    let mut join_predicates = join.predicates().clone();
    for i in 0..parameters.len() {
        join_predicates.push(Scalar::Call(Box::new(F::Is(
            Scalar::Column(left_parameters[i].clone()),
            Scalar::Column(parameters[i].clone()),
        ))));
    }
    // Push the rewritten dependent join down the left side, and the original dependent join down the right side.
    LogicalJoin {
        join: join.replace(join_predicates),
        left: Box::new(LogicalDependentJoin {
            parameters: left_parameters,
            predicates: vec![],
            subquery: Box::new(left_subquery),
            domain: Box::new(left_domain),
        }),
        right: Box::new(LogicalDependentJoin {
            parameters,
            predicates: vec![],
            subquery: right_subquery,
            domain,
        }),
    }
}

fn any_value(attributes: HashSet<Column>) -> Vec<AggregateExpr> {
    let mut attributes: Vec<_> = attributes
        .iter()
        .map(|column| AggregateExpr {
            function: AggregateFunction::AnyValue,
            distinct: false,
            input: column.clone(),
            output: column.clone(),
        })
        .collect();
    attributes.sort_by(|a, b| a.input.cmp(&b.input));
    attributes
}
