use ast::{Expr, *};

use crate::{cost::*, rule::*, search_space::*};

const TRACE: bool = false;

#[log::trace]
pub fn optimize(expr: Expr, txn: i64) -> Expr {
    let expr = crate::rewrite::rewrite_plan(expr);
    let mut expr = search_for_best_plan(expr, txn);
    crate::distribution::set_hash_columns(&mut expr);
    crate::distribution::set_stages(&mut expr);
    expr
}

#[log::trace]
fn search_for_best_plan(mut expr: Expr, txn: i64) -> Expr {
    let mut ss = SearchSpace::empty(txn);
    copy_in_new(&mut ss, &mut expr);
    let gid = match expr {
        Leaf { gid } => GroupID(gid),
        _ => panic!("copy_in_new did not replace expr with Leaf"),
    };
    optimize_group(&mut ss, gid, PhysicalProp::None);
    winner(&mut ss, gid, PhysicalProp::None)
}

// Our implementation of tasks differs from Columbia/Cascades:
// we use ordinary functions and recursion rather than task objects and a stack of pending tasks.
// However, the logic and the order of invocation should be exactly the same.

/// optimize_group optimizes the entire expression tree represented by gid in a top-down manner.
fn optimize_group(ss: &mut SearchSpace, gid: GroupID, require: PhysicalProp) {
    if TRACE {
        println!("optimize_group @{:?} {:?}", gid.0, require)
    }
    // If the lower bound of the group is greater than the upper bound for the required property, give up.
    if ss[gid].lower_bound >= ss[gid].upper_bound[require].unwrap_or(f64::MAX) {
        return;
    }
    // If there is already a winner for the required property, stop early.
    if ss[gid].winners[require].is_some() {
        return;
    }
    // Cost all the physical mexprs with the same required property.
    for mid in ss[gid].physical.clone() {
        optimize_inputs_and_cost(ss, mid, require);
    }
    // Optimize all logical mexprs with the same required property.
    for mid in ss[gid].logical.clone() {
        optimize_expr(ss, mid, false, require);
    }
}

/// optimize_expr ensures that every matching rule has been applied to mexpr.
fn optimize_expr(ss: &mut SearchSpace, mid: MultiExprID, explore: bool, require: PhysicalProp) {
    if TRACE {
        println!(
            "optimize_expr{} ({:?}) {:?}",
            if explore { " (exploring)" } else { "" },
            &ss[mid],
            require
        )
    }
    for rule in Rule::all() {
        // Have we already applied this rule to this multi-expression?
        if ss[mid].fired.contains(&rule) {
            continue;
        }
        // If we are exploring, rather than optimizing, skip physical expressions:
        if explore && rule.output_is_physical() {
            continue;
        }
        // Does the pattern match the multi-expression?
        if rule.matches_fast(&ss[mid], require) {
            // Explore inputs recursively:
            for i in 0..ss[mid].expr.len() {
                // If the i'th child of the LHS of the rule is not a leaf node, explore it recursively:
                if rule.non_leaf(i) {
                    explore_group(ss, leaf(&ss[mid].expr[i]), require)
                }
            }
            // Apply the rule, potentially adding another MultiExpr to the Group:
            apply_rule(ss, rule, mid, explore, require);
            ss[mid].fired.insert(rule);
        }
    }
}

/// apply_rule applies rule to mexpr.
/// If the result is a logical expr, optimize it recursively.
/// If the result is a physical expr, evaluate its cost and potentially declare it the current winner.
fn apply_rule(
    ss: &mut SearchSpace,
    rule: Rule,
    mid: MultiExprID,
    explore: bool,
    require: PhysicalProp,
) {
    if TRACE {
        println!(
            "apply_rule{} {:?} ({:?}) {:?}",
            if explore { " (exploring)" } else { "" },
            rule,
            &ss[mid],
            require
        )
    }
    for expr in rule.bind(&ss, mid) {
        for expr in rule.apply(expr, ss) {
            // Add mexpr if it isn't already present in the group.
            if let Some(mid) = copy_in(ss, expr, ss[mid].parent) {
                if TRACE {
                    println!("{:?}", &ss);
                }
                if !ss[mid].expr.is_logical() {
                    // If rule produced a physical implementation, cost the implementation:
                    optimize_inputs_and_cost(ss, mid, require);
                } else {
                    // If rule produced a new new logical expression, optimize it:
                    optimize_expr(ss, mid, explore, require)
                }
            }
        }
    }
}

/// explore_group ensures that a non-leaf input to a complex rule has been logically explored,
/// to make sure the logical expression that matches the non-leaf input has a chance to be discovered.
fn explore_group(ss: &mut SearchSpace, gid: GroupID, require: PhysicalProp) {
    if TRACE {
        println!("explore_group @{:?} {:?}", gid.0, require)
    }
    if !ss[gid].explored {
        for mid in ss[gid].logical.clone() {
            optimize_expr(ss, mid, true, require)
        }
        ss[gid].explored = true;
    }
}

/// optimize_inputs_and_cost takes a physical expr, recursively optimizes all of its inputs,
/// estimates its cost, and potentially declares it the winning physical expr of the group.
fn optimize_inputs_and_cost(ss: &mut SearchSpace, mid: MultiExprID, require: PhysicalProp) {
    if TRACE {
        println!("optimize_inputs_and_cost ({:?}) {:?}", &ss[mid], require)
    }
    // If this expression doesn't meet the required property, abandon it.
    if !require.met(&ss[mid].expr) {
        return;
    }
    // Identify the maximum cost we are willing to pay for the logical plan that is implemented by mid.
    let parent = ss[mid].parent;
    let upper_bound = ss[parent].upper_bound[require].unwrap_or(f64::MAX);
    let physical_cost = physical_cost(mid, ss);
    // If we can find a winning strategy for each input and an associated cost,
    // try to declare the current MultiExpr as the winner of its group.
    if optimize_inputs(ss, mid, physical_cost, upper_bound) {
        try_to_declare_winner(ss, mid, physical_cost, require);
    }
}

fn optimize_inputs(
    ss: &mut SearchSpace,
    mid: MultiExprID,
    physical_cost: Cost,
    upper_bound: Cost,
) -> bool {
    // Initially, physicalCost is the actual physical cost of the operator at the head of the multi-expression,
    // and inputCosts are the total physical cost of the winning strategy for each input group.
    // If we don't yet have a winner for an inputGroup, we use the lower bound.
    // Thus, physicalCost + sum(inputCosts) = a lower-bound for the total cost of the best strategy for this group.
    let mut input_costs = init_costs_using_lower_bound(ss, mid);
    for i in 0..ss[mid].expr.len() {
        // If we can prove the cost of this MultiExpr exceeds the upper_bound of the Group, give up:
        let lower_bound = cost_so_far(physical_cost, &input_costs);
        if lower_bound >= upper_bound {
            return false;
        }
        // Propagate the cost upper_bound downwards to the input group,
        // using the best available estimate of the cost of the other inputs:
        let input = leaf(&ss[mid].expr[i]);
        let require = PhysicalProp::required(&ss[mid].expr, i);
        let others_lower_bound = lower_bound - input_costs[i];
        let input_upper_bound = upper_bound - others_lower_bound;
        ss[input].upper_bound[require] = Some(input_upper_bound);
        // Optimize input group:
        optimize_group(ss, leaf(&ss[mid].expr[i]), require);
        // If we failed to declare a winner, give up:
        if ss[input].winners[require].is_none() {
            return false;
        }
        input_costs[i] = ss[input].winners[require].as_ref().unwrap().cost
    }
    true
}

fn try_to_declare_winner(
    ss: &mut SearchSpace,
    mid: MultiExprID,
    physical_cost: Cost,
    require: PhysicalProp,
) {
    if TRACE {
        println!(
            "try_to_declare_winner ({:?}) ${:?} {:?}",
            &ss[mid], physical_cost, require
        )
    }
    let mut total_cost = physical_cost;
    for i in 0..ss[mid].expr.len() {
        let input = leaf(&ss[mid].expr[i]);
        let require_input = PhysicalProp::required(&ss[mid].expr, i);
        match ss[input].winners[require_input].as_ref() {
            Some(winner) => {
                total_cost += winner.cost;
            }
            None => {
                return;
            }
        }
    }
    let gid = ss[mid].parent;
    let current_cost = ss[gid].winners[require].map(|w| w.cost).unwrap_or(f64::MAX);
    if total_cost < current_cost {
        ss[gid].winners[require] = Some(Winner {
            plan: mid,
            cost: total_cost,
        });
        if TRACE {
            println!("{:?}", &ss);
        }
    }
}

fn copy_in(ss: &mut SearchSpace, mut expr: Expr, gid: GroupID) -> Option<MultiExprID> {
    // Recursively copy in the children.
    for i in 0..expr.len() {
        copy_in_new(ss, &mut expr[i]);
    }
    // If this is the first time we observe expr as a member of gid, add it to the group.
    if let Some(mid) = ss.add_mexpr(MultiExpr::new(gid, expr)) {
        // Add expr to group.
        if ss[mid].expr.is_logical() {
            ss[gid].logical.push(mid);
        } else {
            ss[gid].physical.push(mid);
        }
        Some(mid)
    } else {
        None
    }
}

fn copy_in_new(ss: &mut SearchSpace, expr: &mut Expr) {
    if let Leaf { .. } = expr {
        // Nothing to do.
    } else if let Some(mid) = ss.find_dup(&expr) {
        let gid = ss[mid].parent;
        *expr = Leaf { gid: gid.0 };
    } else {
        // Recursively copy in the children.
        for i in 0..expr.len() {
            copy_in_new(ss, &mut expr[i]);
        }
        // Record temp tables.
        if let LogicalCreateTempTable { name, input, .. } = expr {
            ss.temp_tables
                .insert(name.clone(), ss[leaf(input)].props.clone());
        }
        // Replace expr with a Leaf node.
        let gid = ss.reserve();
        let removed = std::mem::replace(expr, Leaf { gid: gid.0 });
        // Initialize a new MultiExpr.
        let mexpr = MultiExpr::new(gid, removed);
        let mid = ss.add_mexpr(mexpr).unwrap();
        // Initialize a new Group.
        let props = crate::cardinality_estimation::compute_logical_props(mid, &ss);
        let lower_bound = compute_lower_bound(&ss[mid], &props, &ss);
        let group = Group {
            logical: vec![mid],
            physical: vec![],
            props,
            lower_bound,
            upper_bound: PerPhysicalProp::default(),
            winners: PerPhysicalProp::default(),
            explored: false,
        };
        ss.add_group(gid, group);
    }
}

fn init_costs_using_lower_bound(ss: &SearchSpace, mid: MultiExprID) -> Vec<Cost> {
    let mut costs = Vec::with_capacity(ss[mid].expr.len());
    for i in 0..ss[mid].expr.len() {
        let input = leaf(&ss[mid].expr[i]);
        let require = PhysicalProp::required(&ss[mid].expr, i);
        let cost = match ss[input].winners[require].as_ref() {
            Some(winner) => winner.cost,
            None => ss[input].lower_bound,
        };
        costs.push(cost);
    }
    costs
}

fn winner(ss: &SearchSpace, gid: GroupID, require: PhysicalProp) -> Expr {
    let mid = ss[gid].winners[require]
        .unwrap_or_else(|| panic!("group @{} has no winner for {:?}\n{:?}", gid.0, require, ss))
        .plan;
    let mut expr = ss[mid].expr.clone();
    for i in 0..expr.len() {
        let require = PhysicalProp::required(&expr, i);
        expr[i] = winner(ss, leaf(&expr[i]), require);
    }
    expr
}

fn cost_so_far(physical_cost: Cost, input_costs: &Vec<Cost>) -> Cost {
    let mut cost = physical_cost;
    if physical_cost == f64::MAX {
        return f64::MAX;
    }
    for input_cost in input_costs {
        if *input_cost == f64::MAX {
            return f64::MAX;
        }
        cost += input_cost
    }
    cost
}
