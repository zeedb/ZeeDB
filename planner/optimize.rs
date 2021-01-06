use crate::{cost::*, rewrite::rewrite, rule::*, search_space::*};
use ast::*;
use catalog::Index;
use std::collections::{HashMap, HashSet};
use storage::Storage;
use zetasql::SimpleCatalogProto;

// Our implementation of tasks differs from Columbia/Cascades:
// we use ordinary functions and recursion rather than task objects and a stack of pending tasks.
// However, the logic and the order of invocation should be exactly the same.

pub fn optimize(
    catalog_id: i64,
    catalog: &SimpleCatalogProto,
    indexes: &HashMap<i64, Vec<Index>>,
    statistics: &Storage,
    expr: Expr,
) -> Expr {
    let mut optimizer = Optimizer {
        indexes,
        statistics,
        ss: SearchSpace::new(),
    };
    let expr = rewrite(catalog_id, catalog, expr);
    let gid = optimizer.copy_in_new(expr);
    optimizer.optimize_group(gid);
    optimizer.winner(gid)
}

struct Optimizer<'a> {
    indexes: &'a HashMap<i64, Vec<Index>>,
    statistics: &'a Storage,
    ss: SearchSpace,
}

impl<'a> Optimizer<'a> {
    fn optimize_group(&mut self, gid: GroupID) {
        if self.ss[gid].lower_bound >= self.ss[gid].upper_bound || self.ss[gid].winner.is_some() {
            return;
        }
        for mid in self.ss[gid].physical.clone() {
            self.optimize_inputs(mid);
        }
        for mid in self.ss[gid].logical.clone() {
            self.optimize_expr(mid, false);
        }
    }

    // optimize_expr ensures that every matching rule has been applied to mexpr.
    fn optimize_expr(&mut self, mid: MultiExprID, explore: bool) {
        for rule in Rule::all() {
            // Have we already applied this rule to this multi-expression?
            if self.ss[mid].fired.contains(&rule) {
                continue;
            }
            // If we are exploring, rather than optimizing, skip physical expressions:
            if explore && rule.output_is_physical() {
                continue;
            }
            // Does the pattern match the multi-expression?
            if rule.matches_fast(&self.ss[mid]) {
                // Explore inputs recursively:
                for i in 0..self.ss[mid].expr.len() {
                    // If the i'th child of the LHS of the rule is not a leaf node, explore it recursively:
                    if rule.non_leaf(i) {
                        self.explore_group(leaf(&self.ss[mid].expr[i]))
                    }
                }
                // Apply the rule, potentially adding another MultiExpr to the Group:
                self.apply_rule(rule, mid, explore);
                self.ss[mid].fired.insert(rule);
            }
        }
    }

    // apply_rule applies rule to mexpr.
    // If the result is a logical expr, optimize it recursively.
    // If the result is a physical expr, evaluate its cost and potentially declare it the current winner.
    fn apply_rule(&mut self, rule: Rule, mid: MultiExprID, explore: bool) {
        for expr in rule.bind(&self.ss, mid) {
            if let Some(expr) = rule.apply(&self.ss, &self.indexes, expr) {
                // Add mexpr if it isn't already present in the group.
                if let Some(mid) = self.copy_in(expr, self.ss[mid].parent) {
                    if !self.ss[mid].expr.is_logical() {
                        // If rule produced a physical implementation, cost the implementation:
                        self.optimize_inputs(mid);
                    } else {
                        // If rule produced a new new logical expression, optimize it:
                        self.optimize_expr(mid, explore)
                    }
                }
            }
        }
    }

    // explore_group ensures that a non-leaf input to a complex rule has been logically explored,
    // to make sure the logical expression that matches the non-leaf input has a chance to be discovered.
    fn explore_group(&mut self, gid: GroupID) {
        if !self.ss[gid].explored {
            for mid in self.ss[gid].logical.clone() {
                self.optimize_expr(mid, true)
            }
            self.ss[gid].explored = true;
        }
    }

    // optimize_inputs takes a physical expr, recursively optimizes all of its inputs,
    // estimates its cost, and potentially declares it the winning physical expr of the group.
    fn optimize_inputs(&mut self, mid: MultiExprID) {
        // Initially, physicalCost is the actual physical cost of the operator at the head of the multi-expression,
        // and inputCosts are the total physical cost of the winning strategy for each input group.
        // If we don't yet have a winner for an inputGroup, we use the lower bound.
        // Thus, physicalCost + sum(inputCosts) = a lower-bound for the total cost of the best strategy for this group.
        let physical_cost = physical_cost(&self.ss, &self.statistics, mid);
        let mut input_costs = self.init_costs_using_lower_bound(mid);
        for i in 0..self.ss[mid].expr.len() {
            let input = leaf(&self.ss[mid].expr[i]);
            // If we can prove the cost of this MultiExpr exceeds the upper_bound of the Group, give up:
            if self.stop_early(self.ss[mid].parent, physical_cost, &input_costs) {
                return;
            }
            // Propagate the cost upper_bound downwards to the input group,
            // using the best available estimate of the cost of the other inputs:
            let total_cost = cost_so_far(physical_cost, &input_costs);
            let input_upper_bound =
                self.ss[self.ss[mid].parent].upper_bound - (total_cost - input_costs[i]);
            self.ss[input].upper_bound = input_upper_bound;
            // Optimize input group:
            self.optimize_group(leaf(&self.ss[mid].expr[i]));
            // If we failed to declare a winner, give up:
            if self.ss[input].winner.is_none() {
                return;
            }
            input_costs[i] = self.ss[input].winner.as_ref().unwrap().cost
        }
        // Now that we have a winning strategy for each input and an associated cost,
        // try to declare the current MultiExpr as the winner of its Group:
        self.try_to_declare_winner(mid, physical_cost);
    }

    fn stop_early(&self, gid: GroupID, physical_cost: Cost, input_costs: &Vec<Cost>) -> bool {
        let lower_bound = cost_so_far(physical_cost, input_costs);
        let upper_bound = self.ss[gid].upper_bound;
        lower_bound >= upper_bound
    }

    fn try_to_declare_winner(&mut self, mid: MultiExprID, physical_cost: Cost) {
        let mut total_cost = physical_cost;
        for i in 0..self.ss[mid].expr.len() {
            let input = leaf(&self.ss[mid].expr[i]);
            match self.ss[input].winner.as_ref() {
                Some(winner) => {
                    total_cost += winner.cost;
                }
                None => {
                    return;
                }
            }
        }
        let gid = self.ss[mid].parent;
        let current_cost = self.ss[gid].winner.map(|w| w.cost).unwrap_or(f64::MAX);
        if total_cost < current_cost {
            self.ss[gid].winner = Some(Winner {
                plan: mid,
                cost: total_cost,
            })
        }
    }

    fn copy_in(&mut self, expr: Expr, gid: GroupID) -> Option<MultiExprID> {
        // Recursively copy in the children.
        let expr = expr.map(|child| Leaf {
            gid: self.copy_in_new(child).0,
        });
        // If this is the first time we observe expr as a member of gid, add it to the group.
        if let Some(mid) = self.ss.add_mexpr(MultiExpr::new(gid, expr)) {
            // Add expr to group.
            if self.ss[mid].expr.is_logical() {
                self.ss[gid].logical.push(mid);
            } else {
                self.ss[gid].physical.push(mid);
            }
            Some(mid)
        } else {
            None
        }
    }

    fn copy_in_new(&mut self, expr: Expr) -> GroupID {
        if let Leaf { gid } = expr {
            GroupID(gid)
        } else if let Some(mid) = self.ss.find_dup(&expr) {
            self.ss[mid].parent
        } else {
            let gid = self.ss.reserve();
            // Recursively copy in the children.
            let expr = expr.map(|child| Leaf {
                gid: self.copy_in_new(child).0,
            });
            // Initialize a new MultiExpr.
            let mexpr = MultiExpr::new(gid, expr);
            let mid = self.ss.add_mexpr(mexpr).unwrap();
            // Initialize a new Group.
            let props = self.compute_logical_props(&self.ss[mid]);
            let lower_bound = compute_lower_bound(&self.ss, &self.ss[mid], &props);
            let group = Group {
                logical: vec![mid],
                physical: vec![],
                props,
                lower_bound,
                upper_bound: f64::MAX,
                winner: None,
                explored: false,
            };
            self.ss.add_group(gid, group);
            gid
        }
    }

    fn compute_logical_props(&self, mexpr: &MultiExpr) -> LogicalProps {
        let mut cardinality = 0 as usize;
        let mut column_unique_cardinality: HashMap<Column, usize> = HashMap::new();
        match &mexpr.expr {
            LogicalSingleGet => cardinality = 1,
            LogicalGet {
                projects,
                predicates,
                table,
            } => {
                // Scan
                cardinality = self.statistics.table_cardinality(table.id);
                for c in projects {
                    column_unique_cardinality.insert(
                        c.clone(),
                        self.statistics.column_unique_cardinality(table.id, &c.name),
                    );
                }
                // Filter
                let selectivity = total_selectivity(predicates, &column_unique_cardinality);
                cardinality = apply_selectivity(cardinality, selectivity);
                for (_, n) in column_unique_cardinality.iter_mut() {
                    *n = cardinality.min(*n);
                }
            }
            LogicalFilter { predicates, input } => {
                let scope = &self.ss[leaf(input)].props.column_unique_cardinality;
                let selectivity = total_selectivity(predicates, scope);
                cardinality =
                    apply_selectivity(self.ss[leaf(input)].props.cardinality, selectivity);
                for (c, n) in &self.ss[leaf(input)].props.column_unique_cardinality {
                    column_unique_cardinality.insert(c.clone(), cardinality.min(*n));
                }
            }
            LogicalOut { projects, input } => {
                let input = &self.ss[leaf(input)];
                cardinality = input.props.cardinality;
                for c in projects {
                    let n = input.props.column_unique_cardinality.get(c).expect(
                        format!(
                            "no column {:?} in {:?}",
                            c,
                            input.props.column_unique_cardinality.keys()
                        )
                        .as_str(),
                    );
                    column_unique_cardinality.insert(c.clone(), *n);
                }
            }
            LogicalMap {
                include_existing,
                projects,
                input,
            } => {
                cardinality = self.ss[leaf(input)].props.cardinality;
                if *include_existing {
                    for (c, n) in &self.ss[leaf(input)].props.column_unique_cardinality {
                        column_unique_cardinality.insert(c.clone(), *n);
                    }
                }
                for (x, c) in projects {
                    let n = scalar_unique_cardinality(
                        &x,
                        &self.ss[leaf(input)].props.column_unique_cardinality,
                    );
                    column_unique_cardinality.insert(c.clone(), n);
                }
            }
            LogicalJoin {
                join, left, right, ..
            } => {
                let left_cardinality = self.ss[leaf(left)].props.cardinality;
                let right_cardinality = self.ss[leaf(right)].props.cardinality;
                let left_scope = &self.ss[leaf(left)].props.column_unique_cardinality;
                let right_scope = &self.ss[leaf(right)].props.column_unique_cardinality;
                cardinality = left_cardinality * right_cardinality;
                for (c, n) in left_scope {
                    column_unique_cardinality.insert(c.clone(), *n);
                }
                for (c, n) in right_scope {
                    column_unique_cardinality.insert(c.clone(), *n);
                }
                // Mark join projects the $mark attribute
                if let Join::Mark(mark, _) = join {
                    column_unique_cardinality.insert(mark.clone(), 2);
                }
                // Apply predicates
                let mut is_equi_join = false;
                for p in join.predicates() {
                    if is_equi_predicate(p, left_scope, right_scope) {
                        is_equi_join = true;
                    } else {
                        let selectivity = predicate_selectivity(p, &column_unique_cardinality);
                        cardinality = apply_selectivity(cardinality, selectivity);
                    }
                }
                if is_equi_join {
                    cardinality = cardinality / left_cardinality.min(right_cardinality).max(1);
                }
                // We want (SemiJoin _ _) to have the same selectivity as (Filter $mark.$in (MarkJoin _ _))
                if let Join::Semi(_) | Join::Anti(_) = join {
                    cardinality = apply_selectivity(cardinality, 0.5);
                    for (_, n) in column_unique_cardinality.iter_mut() {
                        *n = cardinality.min(*n);
                    }
                }
            }
            LogicalDependentJoin { .. } | LogicalWith { .. } => panic!(
                "{} should have been eliminated during rewrite phase",
                mexpr.expr.name()
            ),
            LogicalGetWith { columns, .. } => {
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
                    let n = self.ss[leaf(input)].props.column_unique_cardinality[c];
                    column_unique_cardinality.insert(c.clone(), n);
                    cardinality *= n;
                }
                cardinality = self.ss[leaf(input)].props.cardinality.min(cardinality);
                for a in aggregate {
                    column_unique_cardinality.insert(a.output.clone(), cardinality);
                }
            }
            LogicalLimit { limit, input, .. } => {
                cardinality = *limit;
                for (c, n) in &self.ss[leaf(input)].props.column_unique_cardinality {
                    if *limit < *n {
                        column_unique_cardinality.insert(c.clone(), *limit);
                    } else {
                        column_unique_cardinality.insert(c.clone(), *n);
                    }
                }
            }
            LogicalSort { input, .. } => {
                cardinality = self.ss[leaf(input)].props.cardinality;
                column_unique_cardinality =
                    self.ss[leaf(input)].props.column_unique_cardinality.clone();
            }
            LogicalUnion { left, right } => {
                cardinality =
                    self.ss[leaf(left)].props.cardinality + self.ss[leaf(right)].props.cardinality;
                column_unique_cardinality = max_cuc(
                    &self.ss[leaf(left)].props.column_unique_cardinality,
                    &self.ss[leaf(right)].props.column_unique_cardinality,
                );
            }
            LogicalScript { statements } => {
                let last = statements.last().unwrap();
                cardinality = self.ss[leaf(last)].props.cardinality;
                column_unique_cardinality =
                    self.ss[leaf(last)].props.column_unique_cardinality.clone();
            }
            LogicalInsert { .. }
            | LogicalValues { .. }
            | LogicalUpdate { .. }
            | LogicalDelete { .. }
            | LogicalCreateDatabase { .. }
            | LogicalCreateTable { .. }
            | LogicalCreateTempTable { .. }
            | LogicalCreateIndex { .. }
            | LogicalDrop { .. }
            | LogicalAssign { .. }
            | LogicalCall { .. }
            | LogicalExplain { .. }
            | LogicalRewrite { .. } => {}
            Leaf { .. }
            | TableFreeScan { .. }
            | SeqScan { .. }
            | IndexScan { .. }
            | Filter { .. }
            | Out { .. }
            | Map { .. }
            | NestedLoop { .. }
            | HashJoin { .. }
            | CreateTempTable { .. }
            | GetTempTable { .. }
            | Aggregate { .. }
            | Limit { .. }
            | Sort { .. }
            | Union { .. }
            | Insert { .. }
            | Values { .. }
            | Delete { .. }
            | Script { .. }
            | Assign { .. }
            | Call { .. }
            | Explain { .. } => panic!("{} is a physical operator", mexpr.expr.name()),
        };
        LogicalProps {
            cardinality,
            column_unique_cardinality,
        }
    }

    fn init_costs_using_lower_bound(&self, mid: MultiExprID) -> Vec<Cost> {
        let mut costs = Vec::with_capacity(self.ss[mid].expr.len());
        for i in 0..self.ss[mid].expr.len() {
            let input = leaf(&self.ss[mid].expr[i]);
            let cost = match self.ss[input].winner.as_ref() {
                Some(winner) => winner.cost,
                None => self.ss[input].lower_bound,
            };
            costs.push(cost);
        }
        costs
    }

    fn winner(&self, gid: GroupID) -> Expr {
        let mid = self.ss[gid]
            .winner
            .unwrap_or_else(|| panic!("group {} has no winner {:?}", gid.0, self.ss))
            .plan;
        self.ss[mid]
            .expr
            .clone()
            .map(|expr| self.winner(leaf(&expr)))
    }
}

fn is_equi_predicate(
    p: &Scalar,
    left_scope: &HashMap<Column, usize>,
    right_scope: &HashMap<Column, usize>,
) -> bool {
    match p {
        Scalar::Call(f) => match f.as_ref() {
            F::Equal(left, right) | F::Is(left, right) => {
                let left_references = left.references();
                let right_references = right.references();
                (is_subset(&left_references, left_scope)
                    && is_subset(&right_references, right_scope))
                    || (is_subset(&left_references, right_scope)
                        && is_subset(&right_references, left_scope))
            }
            _ => false,
        },
        _ => false,
    }
}

fn is_subset(references: &HashSet<Column>, scope: &HashMap<Column, usize>) -> bool {
    references.iter().all(|column| scope.contains_key(column))
}

fn total_selectivity(predicates: &Vec<Scalar>, scope: &HashMap<Column, usize>) -> f64 {
    let mut selectivity = 1.0;
    for p in predicates {
        selectivity *= predicate_selectivity(p, scope);
    }
    selectivity
}

fn predicate_selectivity(predicate: &Scalar, scope: &HashMap<Column, usize>) -> f64 {
    1.0 // TODO
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
        Scalar::Literal(_) => 1,
        Scalar::Column(column) => *scope
            .get(column)
            .unwrap_or_else(|| panic!("no key {:?} in {:?}", column, scope)),
        Scalar::Parameter(_, _) => 1,
        Scalar::Call(_) => 1, // TODO
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
