use crate::rule::*;
use crate::search_space::*;
use encoding::*;
use node::*;
use std::collections::{HashMap, HashSet};

// Our implementation of tasks differs from Columbia/Cascades:
// we use ordinary functions and recursion rather than task objects and a stack of pending tasks.
// However, the logic and the order of invocation should be exactly the same.

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
                if rule.has_inputs(i) {
                    explore_group(ss, ss[mid].op[i])
                }
            }
            // Apply the rule, potentially adding another MultiExpr to the Group:
            apply_rule(ss, &rule, mid, explore);
            ss[mid].fired.insert(rule);
        }
    }
}

// apply_rule applies rule to mexpr.
// If the result is a logical expr, optimize it recursively.
// If the result is a physical expr, evaluate its cost and potentially declare it the current winner.
fn apply_rule(ss: &mut SearchSpace, rule: &Rule, mid: MultiExprID, explore: bool) {
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

// explore_group ensures that optimize_expr is called on every group at least once.
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
    let current_cost = ss[gid].winner.as_ref().map(|w| w.cost).unwrap_or(f64::MAX);
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
        LogicalGet(table) => {
            cardinality = 1000; // TODO get from LogicalGet or Tabl
            for c in &table.columns {
                column_unique_cardinality.insert(c.clone(), cardinality);
            }
        }
        LogicalFilter(predicates, input) => {
            let scope = &ss[*input].props.column_unique_cardinality;
            let selectivity = total_selectivity(predicates, scope);
            cardinality = apply_selectivity(ss[*input].props.cardinality, selectivity);
            for (c, n) in &ss[*input].props.column_unique_cardinality {
                column_unique_cardinality.insert(c.clone(), apply_selectivity(*n, selectivity));
            }
        }
        LogicalProject(projects, input) => {
            cardinality = ss[*input].props.cardinality;
            for (x, c) in projects {
                let n = scalar_unique_cardinality(&x, &ss[*input].props.column_unique_cardinality);
                column_unique_cardinality.insert(c.clone(), n);
            }
        }
        LogicalJoin(join, left, right) => {
            let mut scope = HashMap::new();
            for (c, n) in &ss[*left].props.column_unique_cardinality {
                scope.insert(c.clone(), *n);
            }
            for (c, n) in &ss[*right].props.column_unique_cardinality {
                scope.insert(c.clone(), *n);
            }
            let product = ss[*left].props.cardinality * ss[*right].props.cardinality;
            for (c, n) in &ss[*left].props.column_unique_cardinality {
                column_unique_cardinality.insert(c.clone(), *n);
            }
            for (c, n) in &ss[*right].props.column_unique_cardinality {
                column_unique_cardinality.insert(c.clone(), *n);
            }
            // We want (SemiJoin _ _) to have the same selectivity as (Filter $mark.$in (MarkJoin _ _))
            match join {
                Join::Semi(_) | Join::Anti(_) => {
                    cardinality = apply_selectivity(cardinality, 0.5);
                    for (_, n) in column_unique_cardinality.iter_mut() {
                        *n = apply_selectivity(*n, 0.5);
                    }
                }
                _ => {}
            }
        }
        LogicalWith(name, _, _) => todo!("with"),
        LogicalGetWith(name) => todo!("get_with"),
        LogicalAggregate {
            group_by,
            aggregate,
            input,
        } => {
            cardinality = 1;
            for c in group_by {
                let n = ss[*input].props.column_unique_cardinality[&c];
                column_unique_cardinality.insert(c.clone(), n);
                cardinality *= n;
            }
            for (_, c) in aggregate {
                column_unique_cardinality.insert(c.clone(), cardinality);
            }
        }
        LogicalLimit {
            limit,
            offset,
            input,
        } => {
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
        LogicalExcept(left, right) => {
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
        _ => panic!(),
    };
    LogicalProps {
        cardinality,
        column_unique_cardinality,
    }
}

// physicalCost computes the local cost of the physical operator at the head of a multi-expression tree.
// To compute the total physical cost of an expression, you need to choose a single physical expression
// at every node of the tree and add up the local costs.
fn physical_cost(ss: &SearchSpace, mid: MultiExprID) -> Cost {
    let parent = ss[mid].parent;
    match &ss[mid].op {
        TableFreeScan { .. } => 0.0,
        SeqScan { .. } => {
            let output = ss[parent].props.cardinality as f64;
            let blocks = f64::max(1.0, output * TUPLE_SIZE / BLOCK_SIZE);
            blocks * COST_READ_BLOCK
        }
        IndexScan { .. } => {
            let blocks = ss[parent].props.cardinality as f64;
            blocks * COST_READ_BLOCK
        }
        Filter(predicates, input) => {
            let input = ss[*input].props.cardinality as f64;
            let columns = predicates.len() as f64;
            input * columns * COST_CPU_PRED
        }
        Project(compute, input) => {
            let output = ss[*input].props.cardinality as f64;
            let columns = compute.len() as f64;
            output * columns * COST_CPU_EVAL
        }
        NestedLoop(join, left, right) => {
            let build = ss[*left].props.cardinality as f64;
            let probe = ss[*right].props.cardinality as f64;
            let iterations = build * probe;
            build * COST_ARRAY_BUILD + iterations * COST_ARRAY_PROBE
        }
        HashJoin(join, equals, left, right) => {
            let build = ss[*left].props.cardinality as f64;
            let probe = ss[*right].props.cardinality as f64;
            build * COST_HASH_BUILD + probe * COST_HASH_PROBE
        }
        CreateTempTable { .. } => todo!("CreateTempTable"),
        GetTempTable { .. } => todo!("GetTempTable"),
        Aggregate {
            group_by,
            aggregate,
            input,
        } => {
            let n = ss[*input].props.cardinality as f64;
            let n_group_by = n * group_by.len() as f64;
            let n_aggregate = n * aggregate.len() as f64;
            n_group_by * COST_HASH_BUILD + n_aggregate * COST_CPU_APPLY
        }
        Limit { .. } => 0.0,
        Sort { .. } => {
            let card = ss[parent].props.cardinality.max(1) as f64;
            let log = 2.0 * card * f64::log2(card);
            log * COST_CPU_COMP_MOVE
        }
        Union(_, _) | Intersect(_, _) | Except(_, _) => 0.0,
        Values(_, _) => 0.0,
        Insert(_, _, input) | Update(_, input) | Delete(_, input) => {
            let length = ss[*input].props.cardinality as f64;
            let blocks = f64::max(1.0, length * TUPLE_SIZE / BLOCK_SIZE);
            blocks * COST_WRITE_BLOCK
        }
        CreateDatabase { .. }
        | CreateTable { .. }
        | CreateIndex { .. }
        | AlterTable { .. }
        | Drop { .. }
        | Rename { .. } => 0.0,
        _ => panic!(),
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
    let mid = ss[gid].winner.unwrap().plan;
    Expr(Box::new(ss[mid].op.clone().map(|gid| winner(ss, gid))))
}

fn total_selectivity(predicates: &Vec<Scalar>, scope: &HashMap<Column, usize>) -> f64 {
    let mut selectivity = 0.0;
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
        Scalar::Call(_, _, _) => todo!("call"),
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
    todo!("max_cuc")
}

fn scalar_unique_cardinality(expr: &Scalar, scope: &HashMap<Column, usize>) -> usize {
    match expr {
        Scalar::Literal(_, _) => 1,
        Scalar::Column(column) => scope[column],
        Scalar::Call(_, _, _) => 1, // TODO
        Scalar::Cast(value, _) => scalar_unique_cardinality(value, scope),
    }
}

const BLOCK_SIZE: Cost = 4096.0;
const TUPLE_SIZE: Cost = 100.0;
const COST_READ_BLOCK: Cost = 1.0;
const COST_WRITE_BLOCK: Cost = COST_READ_BLOCK;
const COST_CPU_PRED: Cost = 0.0001;
const COST_CPU_EVAL: Cost = COST_CPU_PRED;
const COST_CPU_APPLY: Cost = COST_CPU_PRED * 2.0;
const COST_CPU_COMP_MOVE: Cost = COST_CPU_PRED * 3.0;
const COST_HASH_BUILD: Cost = COST_CPU_PRED;
const COST_HASH_PROBE: Cost = COST_CPU_PRED;
const COST_ARRAY_BUILD: Cost = COST_CPU_PRED;
const COST_ARRAY_PROBE: Cost = COST_CPU_PRED;

// compute_lower_bound estimates a minimum possible physical cost for mexpr,
// based on a hypothetical idealized query plan that only has to pay
// the cost of joins and reading from disk.
fn compute_lower_bound(column_unique_cardinality: &HashMap<Column, usize>) -> Cost {
    // TODO estimate a lower-bound for joins
    fetching_cost(column_unique_cardinality)
}

fn fetching_cost(column_unique_cardinality: &HashMap<Column, usize>) -> Cost {
    let mut total = 0.0;
    for (_, cost) in table_max_cu_cards(column_unique_cardinality) {
        total += cost as Cost * COST_READ_BLOCK;
    }
    total
}

fn table_max_cu_cards(
    column_unique_cardinality: &HashMap<Column, usize>,
) -> HashMap<String, usize> {
    let mut max = HashMap::new();
    for (column, cost) in column_unique_cardinality {
        if let Some(table) = &column.table {
            if cost > max.get(table).unwrap_or(&0) {
                max.insert(table.clone(), *cost);
            }
        }
    }
    max
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

pub fn optimize(expr: Expr) -> Expr {
    let mut ss = SearchSpace::new();
    let expr = crate::rewrite::rewrite(expr);
    let gid = group_from_expr(&mut ss, expr);
    optimize_group(&mut ss, gid);
    winner(&mut ss, gid)
}
