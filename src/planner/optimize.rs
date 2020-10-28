use crate::cost::*;
use crate::rule::*;
use crate::search_space::*;
use node::*;
use std::collections::{HashMap, HashSet};

// Our implementation of tasks differs from Columbia/Cascades:
// we use ordinary functions and recursion rather than task objects and a stack of pending tasks.
// However, the logic and the order of invocation should be exactly the same.

pub fn optimize(expr: Expr) -> Expr {
    let mut ss = SearchSpace::new();
    let expr = crate::rewrite::rewrite(expr);
    let gid = group_from_expr(&mut ss, expr);
    optimize_group(&mut ss, gid);
    winner(&mut ss, gid)
}

fn optimize_group(ss: &mut SearchSpace, gid: GroupID) {
    if ss[gid].lower_bound >= ss[gid].upper_bound || ss[gid].winner.is_some() {
        return;
    }
    for mid in ss[gid].physical.clone() {
        optimize_inputs(ss, mid);
    }
    for mid in ss[gid].logical.clone() {
        optimize_expr(ss, mid, false);
    }
}

// optimize_expr ensures that every matching rule has been applied to mexpr.
fn optimize_expr(ss: &mut SearchSpace, mid: MultiExprID, explore: bool) {
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
        if rule.matches_fast(&ss[mid]) {
            // Explore inputs recursively:
            for i in 0..ss[mid].op.len() {
                // If the i'th child of the LHS of the rule is not a leaf node, explore it recursively:
                if rule.non_leaf(i) {
                    explore_group(ss, ss[mid].op[i])
                }
            }
            // Apply the rule, potentially adding another MultiExpr to the Group:
            apply_rule(ss, rule, mid, explore);
            ss[mid].fired.insert(rule);
        }
    }
}

// apply_rule applies rule to mexpr.
// If the result is a logical expr, optimize it recursively.
// If the result is a physical expr, evaluate its cost and potentially declare it the current winner.
fn apply_rule(ss: &mut SearchSpace, rule: Rule, mid: MultiExprID, explore: bool) {
    for bind in rule.bind(ss, mid) {
        if let Some(bind) = rule.apply(ss, bind) {
            // Add mexpr if it isn't already present in the group:
            if let Some(mid) = add_binding_to_group(ss, ss[mid].parent, bind) {
                if !ss[mid].op.is_logical() {
                    // If rule produced a physical implementation, cost the implementation:
                    optimize_inputs(ss, mid);
                } else {
                    // If rule produced a new new logical expression, optimize it:
                    optimize_expr(ss, mid, explore)
                }
            }
        }
    }
}

// explore_group ensures that a non-leaf input to a complex rule has been logically explored,
// to make sure the logical expression that matches the non-leaf input has a chance to be discovered.
fn explore_group(ss: &mut SearchSpace, gid: GroupID) {
    if !ss[gid].explored {
        for mid in ss[gid].logical.clone() {
            optimize_expr(ss, mid, true)
        }
        ss[gid].explored = true;
    }
}

// optimize_inputs takes a physical expr, recursively optimizes all of its inputs,
// estimates its cost, and potentially declares it the winning physical expr of the group.
fn optimize_inputs(ss: &mut SearchSpace, mid: MultiExprID) {
    // Initially, physicalCost is the actual physical cost of the operator at the head of the multi-expression,
    // and inputCosts are the total physical cost of the winning strategy for each input group.
    // If we don't yet have a winner for an inputGroup, we use the lower bound.
    // Thus, physicalCost + sum(inputCosts) = a lower-bound for the total cost of the best strategy for this group.
    let physical_cost = physical_cost(ss, mid);
    let mut input_costs = init_costs_using_lower_bound(ss, mid);
    for i in 0..ss[mid].op.len() {
        let input = ss[mid].op[i];
        // If we can prove the cost of this MultiExpr exceeds the upper_bound of the Group, give up:
        if stop_early(ss, ss[mid].parent, physical_cost, &input_costs) {
            return;
        }
        // Propagate the cost upper_bound downwards to the input group,
        // using the best available estimate of the cost of the other inputs:
        let total_cost = cost_so_far(physical_cost, &input_costs);
        let input_upper_bound = ss[ss[mid].parent].upper_bound - (total_cost - input_costs[i]);
        ss[input].upper_bound = input_upper_bound;
        // Optimize input group:
        optimize_group(ss, ss[mid].op[i]);
        // If we failed to declare a winner, give up:
        if ss[input].winner.is_none() {
            return;
        }
        input_costs[i] = ss[input].winner.as_ref().unwrap().cost
    }
    // Now that we have a winning strategy for each input and an associated cost,
    // try to declare the current MultiExpr as the winner of its Group:
    try_to_declare_winner(ss, mid, physical_cost);
}

fn stop_early(
    ss: &SearchSpace,
    gid: GroupID,
    physical_cost: Cost,
    input_costs: &Vec<Cost>,
) -> bool {
    let lower_bound = cost_so_far(physical_cost, input_costs);
    let upper_bound = ss[gid].upper_bound;
    lower_bound >= upper_bound
}

fn try_to_declare_winner(ss: &mut SearchSpace, mid: MultiExprID, physical_cost: Cost) {
    let mut total_cost = physical_cost;
    for i in 0..ss[mid].op.len() {
        let input = ss[mid].op[i];
        match ss[input].winner.as_ref() {
            Some(winner) => {
                total_cost += winner.cost;
            }
            None => {
                return;
            }
        }
    }
    let gid = ss[mid].parent;
    let current_cost = ss[gid].winner.map(|w| w.cost).unwrap_or(f64::MAX);
    if total_cost < current_cost {
        ss[gid].winner = Some(Winner {
            plan: mid,
            cost: total_cost,
        })
    }
}

fn group_from_expr(ss: &mut SearchSpace, expr: Expr) -> GroupID {
    // recursively create new groups for each input to expr
    let mexpr = MultiExpr {
        parent: UNLINKED,
        op: expr.0.map(|child| group_from_expr(ss, child)),
        fired: HashSet::new(),
    };
    group_from_unlinked_mexpr(ss, mexpr)
}

fn add_binding_to_group(
    ss: &mut SearchSpace,
    gid: GroupID,
    bind: Operator<Bind>,
) -> Option<MultiExprID> {
    let visitor = |child| match child {
        Bind::Group(group) => group,
        Bind::Operator(bind) => group_from_bind(ss, *bind),
    };
    let op = bind.map(visitor);
    let mexpr = MultiExpr {
        parent: gid,
        op,
        fired: HashSet::new(),
    };
    if let Some(mid) = ss.intern(mexpr) {
        if ss[mid].op.is_logical() {
            ss[gid].logical.push(mid);
        } else {
            ss[gid].physical.push(mid);
        }
        Some(mid)
    } else {
        None
    }
}

fn group_from_bind(ss: &mut SearchSpace, bind: Operator<Bind>) -> GroupID {
    let visitor = |child| match child {
        Bind::Group(group) => group,
        Bind::Operator(bind) => group_from_bind(ss, *bind),
    };
    let mexpr = MultiExpr {
        parent: UNLINKED,
        op: bind.map(visitor),
        fired: HashSet::new(),
    };
    group_from_unlinked_mexpr(ss, mexpr)
}

fn group_from_unlinked_mexpr(ss: &mut SearchSpace, mut mexpr: MultiExpr) -> GroupID {
    if mexpr.parent != UNLINKED {
        panic!("mexpr is already linked to group {}", mexpr.parent.0);
    }
    // link mexpr to the new group
    let gid = GroupID(ss.groups.len());
    mexpr.parent = gid;
    // initialize a new group
    let props = compute_logical_props(ss, &mexpr);
    let lower_bound = compute_lower_bound(&props.column_unique_cardinality);
    let mid = ss.intern(mexpr).unwrap();
    ss.groups.push(Group {
        logical: vec![mid],
        physical: vec![],
        props,
        lower_bound,
        upper_bound: f64::MAX,
        winner: None,
        explored: false,
    });
    gid
}

fn compute_logical_props(ss: &SearchSpace, mexpr: &MultiExpr) -> LogicalProps {
    let mut cardinality = 0 as usize;
    let mut column_unique_cardinality: HashMap<Column, usize> = HashMap::new();
    match &mexpr.op {
        LogicalSingleGet => cardinality = 1,
        LogicalGet {
            projects,
            predicates,
            table,
        } => {
            // Scan
            cardinality = table_cardinality(table);
            for c in projects {
                if c.name.ends_with("id") {
                    column_unique_cardinality.insert(c.clone(), cardinality);
                } else {
                    column_unique_cardinality.insert(c.clone(), cardinality / 10);
                }
            }
            // Filter
            let selectivity = total_selectivity(predicates, &column_unique_cardinality);
            cardinality = apply_selectivity(cardinality, selectivity);
            for (_, n) in column_unique_cardinality.iter_mut() {
                *n = cardinality.min(*n);
            }
        }
        LogicalFilter(predicates, input) => {
            let scope = &ss[*input].props.column_unique_cardinality;
            let selectivity = total_selectivity(predicates, scope);
            cardinality = apply_selectivity(ss[*input].props.cardinality, selectivity);
            for (c, n) in &ss[*input].props.column_unique_cardinality {
                column_unique_cardinality.insert(c.clone(), cardinality.min(*n));
            }
        }
        LogicalMap {
            include_existing,
            projects,
            input,
        } => {
            cardinality = ss[*input].props.cardinality;
            if *include_existing {
                for (c, n) in &ss[*input].props.column_unique_cardinality {
                    column_unique_cardinality.insert(c.clone(), *n);
                }
            }
            for (x, c) in projects {
                let n = scalar_unique_cardinality(&x, &ss[*input].props.column_unique_cardinality);
                column_unique_cardinality.insert(c.clone(), n);
            }
        }
        LogicalJoin {
            join, left, right, ..
        } => {
            cardinality = ss[*left].props.cardinality * ss[*right].props.cardinality;
            for (c, n) in &ss[*left].props.column_unique_cardinality {
                column_unique_cardinality.insert(c.clone(), *n);
            }
            for (c, n) in &ss[*right].props.column_unique_cardinality {
                column_unique_cardinality.insert(c.clone(), *n);
            }
            // Mark join projects the $mark attribute
            if let Join::Mark(mark, _) = join {
                column_unique_cardinality.insert(mark.clone(), 2);
            }
            // Apply predicates
            for p in join.predicates() {
                let selectivity = predicate_selectivity(p, &column_unique_cardinality);
                cardinality = apply_selectivity(cardinality, selectivity);
            }
            // We want (SemiJoin _ _) to have the same selectivity as (Filter $mark.$in (MarkJoin _ _))
            if let Join::Semi(_) | Join::Anti(_) = join {
                cardinality = apply_selectivity(cardinality, 0.5);
                for (_, n) in column_unique_cardinality.iter_mut() {
                    *n = cardinality.min(*n);
                }
            }
        }
        LogicalDependentJoin {
            parameters,
            predicates,
            subquery,
            domain,
        } => {
            // Figure out the cardinality of domain after projection.
            let mut domain_cardinality = 1;
            for c in parameters {
                let n = ss[*domain].props.column_unique_cardinality[c];
                column_unique_cardinality.insert(c.clone(), n);
                domain_cardinality *= n;
            }
            domain_cardinality = ss[*domain].props.cardinality.min(domain_cardinality);
            // Figure out the cardinality of the join before filtering.
            cardinality = ss[*subquery].props.cardinality * domain_cardinality;
            for (c, n) in &ss[*subquery].props.column_unique_cardinality {
                column_unique_cardinality.insert(c.clone(), *n);
            }
            // Apply predicates
            for p in predicates {
                let selectivity = predicate_selectivity(p, &column_unique_cardinality);
                cardinality = apply_selectivity(cardinality, selectivity);
            }
        }
        LogicalWith(_, _, _, right) => {
            cardinality = ss[*right].props.cardinality;
            column_unique_cardinality = ss[*right].props.column_unique_cardinality.clone();
        }
        LogicalGetWith(_, columns) => {
            cardinality = 1000; // TODO get from catalog somehow
            for c in columns {
                column_unique_cardinality.insert(c.clone(), cardinality);
            }
        }
        LogicalAggregate {
            group_by,
            aggregate,
            input,
        } => {
            cardinality = 1;
            for c in group_by {
                let n = ss[*input].props.column_unique_cardinality[c];
                column_unique_cardinality.insert(c.clone(), n);
                cardinality *= n;
            }
            cardinality = ss[*input].props.cardinality.min(cardinality);
            for (_, c) in aggregate {
                column_unique_cardinality.insert(c.clone(), cardinality);
            }
        }
        LogicalLimit { limit, input, .. } => {
            cardinality = *limit;
            for (c, n) in &ss[*input].props.column_unique_cardinality {
                if *limit < *n {
                    column_unique_cardinality.insert(c.clone(), *limit);
                } else {
                    column_unique_cardinality.insert(c.clone(), *n);
                }
            }
        }
        LogicalSort(_, input) => {
            cardinality = ss[*input].props.cardinality;
            column_unique_cardinality = ss[*input].props.column_unique_cardinality.clone();
        }
        LogicalUnion(left, right) => {
            cardinality = ss[*left].props.cardinality + ss[*right].props.cardinality;
            column_unique_cardinality = max_cuc(
                &ss[*left].props.column_unique_cardinality,
                &ss[*right].props.column_unique_cardinality,
            );
        }
        LogicalIntersect(left, right) => todo!("intersect"),
        LogicalExcept(left, _) => {
            cardinality = ss[*left].props.cardinality;
            column_unique_cardinality = ss[*left].props.column_unique_cardinality.clone();
        }
        LogicalInsert(_, _, _)
        | LogicalValues(_, _, _)
        | LogicalUpdate(_, _)
        | LogicalDelete(_, _)
        | LogicalCreateDatabase(_)
        | LogicalCreateTable { .. }
        | LogicalCreateIndex { .. }
        | LogicalAlterTable { .. }
        | LogicalDrop { .. }
        | LogicalRename { .. } => {}
        op if !op.is_logical() => panic!(
            "tried to compute logical props of physical operator {}",
            op.name()
        ),
        op => panic!("tried to compute logical props of {}", op.name()),
    };
    LogicalProps {
        cardinality,
        column_unique_cardinality,
    }
}

pub fn table_cardinality(table: &Table) -> usize {
    match table.name.as_str() {
        "store" => 1000,
        "customer" => 100000,
        "person" => 10000000,
        _ => 1000,
    }
}

fn init_costs_using_lower_bound(ss: &SearchSpace, mid: MultiExprID) -> Vec<Cost> {
    let mut costs = Vec::with_capacity(ss[mid].op.len());
    for i in 0..ss[mid].op.len() {
        let input = ss[mid].op[i];
        let cost = match ss[input].winner.as_ref() {
            Some(winner) => winner.cost,
            None => ss[input].lower_bound,
        };
        costs.push(cost);
    }
    costs
}

fn winner(ss: &SearchSpace, gid: GroupID) -> Expr {
    let mid = ss[gid]
        .winner
        .unwrap_or_else(|| panic!("group {} has no winner {:?}", gid.0, ss))
        .plan;
    Expr(Box::new(ss[mid].op.clone().map(|gid| winner(ss, gid))))
}

fn total_selectivity(predicates: &Vec<Scalar>, scope: &HashMap<Column, usize>) -> f64 {
    let mut selectivity = 1.0;
    for p in predicates {
        selectivity *= predicate_selectivity(p, scope);
    }
    selectivity
}

fn predicate_selectivity(predicate: &Scalar, scope: &HashMap<Column, usize>) -> f64 {
    match predicate {
        Scalar::Literal(Value::Bool(true), _) => 1.0,
        Scalar::Literal(Value::Bool(false), _) => 0.0,
        Scalar::Literal(value, _) => panic!("{} is not bool", value),
        Scalar::Column(_) => 0.5,
        Scalar::Call(Function::Equal, args, _) => {
            let left = scalar_unique_cardinality(&args[0], scope) as f64;
            let right = scalar_unique_cardinality(&args[1], scope) as f64;
            1.0 / left.max(right).max(1.0)
        }
        Scalar::Call(Function::Or, args, _) => {
            let left = predicate_selectivity(&args[0], scope);
            let right = predicate_selectivity(&args[1], scope);
            1.0 - (1.0 - left) * (1.0 - right)
        }
        Scalar::Call(Function::NotEqual, _, _)
        | Scalar::Call(Function::Less, _, _)
        | Scalar::Call(Function::LessOrEqual, _, _)
        | Scalar::Call(Function::Greater, _, _)
        | Scalar::Call(Function::GreaterOrEqual, _, _) => 1.0,
        Scalar::Call(Function::Like, _, _) => 0.5,
        Scalar::Call(_, _, _) => todo!("{}", predicate),
        Scalar::Cast(_, _) => 0.5,
    }
}

fn apply_selectivity(cardinality: usize, selectivity: f64) -> usize {
    match (cardinality as f64 * selectivity) as usize {
        0 => 1,
        n => n,
    }
}

fn max_cuc(
    left: &HashMap<Column, usize>,
    right: &HashMap<Column, usize>,
) -> HashMap<Column, usize> {
    let mut max = left.clone();
    for (k, v) in right {
        if v > &left[k] {
            max.insert(k.clone(), *v);
        }
    }
    max
}

fn scalar_unique_cardinality(expr: &Scalar, scope: &HashMap<Column, usize>) -> usize {
    match expr {
        Scalar::Literal(_, _) => 1,
        Scalar::Column(column) => *scope
            .get(column)
            .unwrap_or_else(|| panic!("no key {:?} in {:?}", column, scope)),
        Scalar::Call(_, _, _) => 1, // TODO
        Scalar::Cast(value, _) => scalar_unique_cardinality(value, scope),
    }
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
