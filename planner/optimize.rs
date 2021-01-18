use crate::{cost::*, rewrite::rewrite, rule::*, search_space::*};
use ast::{Index, *};
use std::collections::HashMap;
use storage::Storage;
use zetasql::SimpleCatalogProto;

const TRACE: bool = false;

// Our implementation of tasks differs from Columbia/Cascades:
// we use ordinary functions and recursion rather than task objects and a stack of pending tasks.
// However, the logic and the order of invocation should be exactly the same.

pub fn optimize(
    catalog_id: i64,
    catalog: &SimpleCatalogProto,
    indexes: &HashMap<i64, Vec<Index>>,
    storage: &Storage,
    expr: Expr,
) -> Expr {
    let mut optimizer = Optimizer {
        indexes,
        storage,
        ss: SearchSpace::new(),
    };
    let mut expr = rewrite(catalog_id, catalog, expr);
    optimizer.copy_in_new(&mut expr);
    let gid = match expr {
        Leaf { gid } => GroupID(gid),
        _ => panic!("copy_in_new did not replace expr with Leaf"),
    };
    optimizer.optimize_group(gid, PhysicalProp::None);
    optimizer.winner(gid, PhysicalProp::None)
}

struct Optimizer<'a> {
    indexes: &'a HashMap<i64, Vec<Index>>,
    storage: &'a Storage,
    ss: SearchSpace,
}

impl<'a> Optimizer<'a> {
    /// optimize_group optimizes the entire expression tree represented by gid in a top-down manner.
    fn optimize_group(&mut self, gid: GroupID, require: PhysicalProp) {
        if TRACE {
            println!("optimize_group @{:?} {:?}", gid.0, require)
        }
        // If the lower bound of the group is greater than the upper bound for the required property, give up.
        if self.ss[gid].lower_bound >= self.ss[gid].upper_bound[require].unwrap_or(f64::MAX) {
            return;
        }
        // If there is already a winner for the required property, stop early.
        if self.ss[gid].winners[require].is_some() {
            return;
        }
        // Cost all the physical mexprs with the same required property.
        for mid in self.ss[gid].physical.clone() {
            self.optimize_inputs_and_cost(mid, require);
        }
        // Optimize all logical mexprs with the same required property.
        for mid in self.ss[gid].logical.clone() {
            self.optimize_expr(mid, false, require);
        }
    }

    /// optimize_expr ensures that every matching rule has been applied to mexpr.
    fn optimize_expr(&mut self, mid: MultiExprID, explore: bool, require: PhysicalProp) {
        if TRACE {
            println!(
                "optimize_expr{} ({:?}) {:?}",
                if explore { " (exploring)" } else { "" },
                &self.ss[mid],
                require
            )
        }
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
            if rule.matches_fast(&self.ss[mid], require) {
                // Explore inputs recursively:
                for i in 0..self.ss[mid].expr.len() {
                    // If the i'th child of the LHS of the rule is not a leaf node, explore it recursively:
                    if rule.non_leaf(i) {
                        self.explore_group(leaf(&self.ss[mid].expr[i]), require)
                    }
                }
                // Apply the rule, potentially adding another MultiExpr to the Group:
                self.apply_rule(rule, mid, explore, require);
                self.ss[mid].fired.insert(rule);
            }
        }
    }

    /// apply_rule applies rule to mexpr.
    /// If the result is a logical expr, optimize it recursively.
    /// If the result is a physical expr, evaluate its cost and potentially declare it the current winner.
    fn apply_rule(&mut self, rule: Rule, mid: MultiExprID, explore: bool, require: PhysicalProp) {
        if TRACE {
            println!(
                "apply_rule{} {:?} ({:?}) {:?}",
                if explore { " (exploring)" } else { "" },
                rule,
                &self.ss[mid],
                require
            )
        }
        for expr in rule.bind(&self.ss, mid) {
            if let Some(expr) = rule.apply(&self.ss, &self.indexes, expr) {
                // Add mexpr if it isn't already present in the group.
                if let Some(mid) = self.copy_in(expr, self.ss[mid].parent) {
                    if TRACE {
                        println!("{:?}", &self.ss);
                    }
                    if !self.ss[mid].expr.is_logical() {
                        // If rule produced a physical implementation, cost the implementation:
                        self.optimize_inputs_and_cost(mid, require);
                    } else {
                        // If rule produced a new new logical expression, optimize it:
                        self.optimize_expr(mid, explore, require)
                    }
                }
            }
        }
    }

    /// explore_group ensures that a non-leaf input to a complex rule has been logically explored,
    /// to make sure the logical expression that matches the non-leaf input has a chance to be discovered.
    fn explore_group(&mut self, gid: GroupID, require: PhysicalProp) {
        if TRACE {
            println!("explore_group @{:?} {:?}", gid.0, require)
        }
        if !self.ss[gid].explored {
            for mid in self.ss[gid].logical.clone() {
                self.optimize_expr(mid, true, require)
            }
            self.ss[gid].explored = true;
        }
    }

    /// optimize_inputs_and_cost takes a physical expr, recursively optimizes all of its inputs,
    /// estimates its cost, and potentially declares it the winning physical expr of the group.
    fn optimize_inputs_and_cost(&mut self, mid: MultiExprID, require: PhysicalProp) {
        if TRACE {
            println!(
                "optimize_inputs_and_cost ({:?}) {:?}",
                &self.ss[mid], require
            )
        }
        // If this expression doesn't meet the required property, abandon it.
        if !require.met(&self.ss[mid].expr) {
            return;
        }
        // Identify the maximum cost we are willing to pay for the logical plan that is implemented by mid.
        let parent = self.ss[mid].parent;
        let upper_bound = self.ss[parent].upper_bound[require].unwrap_or(f64::MAX);
        let physical_cost = physical_cost(&self.ss, &self.storage, mid);
        // If we can find a winning strategy for each input and an associated cost,
        // try to declare the current MultiExpr as the winner of its group.
        if self.optimize_inputs(mid, physical_cost, upper_bound) {
            self.try_to_declare_winner(mid, physical_cost, require);
        }
    }

    fn optimize_inputs(
        &mut self,
        mid: MultiExprID,
        physical_cost: Cost,
        upper_bound: Cost,
    ) -> bool {
        // Initially, physicalCost is the actual physical cost of the operator at the head of the multi-expression,
        // and inputCosts are the total physical cost of the winning strategy for each input group.
        // If we don't yet have a winner for an inputGroup, we use the lower bound.
        // Thus, physicalCost + sum(inputCosts) = a lower-bound for the total cost of the best strategy for this group.
        let mut input_costs = self.init_costs_using_lower_bound(mid);
        for i in 0..self.ss[mid].expr.len() {
            // If we can prove the cost of this MultiExpr exceeds the upper_bound of the Group, give up:
            let lower_bound = cost_so_far(physical_cost, &input_costs);
            if lower_bound >= upper_bound {
                return false;
            }
            // Propagate the cost upper_bound downwards to the input group,
            // using the best available estimate of the cost of the other inputs:
            let input = leaf(&self.ss[mid].expr[i]);
            let require = PhysicalProp::required(&self.ss[mid].expr, i);
            let others_lower_bound = lower_bound - input_costs[i];
            let input_upper_bound = upper_bound - others_lower_bound;
            self.ss[input].upper_bound[require] = Some(input_upper_bound);
            // Optimize input group:
            self.optimize_group(leaf(&self.ss[mid].expr[i]), require);
            // If we failed to declare a winner, give up:
            if self.ss[input].winners[require].is_none() {
                return false;
            }
            input_costs[i] = self.ss[input].winners[require].as_ref().unwrap().cost
        }
        true
    }

    fn try_to_declare_winner(
        &mut self,
        mid: MultiExprID,
        physical_cost: Cost,
        require: PhysicalProp,
    ) {
        if TRACE {
            println!(
                "try_to_declare_winner ({:?}) ${:?} {:?}",
                &self.ss[mid], physical_cost, require
            )
        }
        let mut total_cost = physical_cost;
        for i in 0..self.ss[mid].expr.len() {
            let input = leaf(&self.ss[mid].expr[i]);
            let require_input = PhysicalProp::required(&self.ss[mid].expr, i);
            match self.ss[input].winners[require_input].as_ref() {
                Some(winner) => {
                    total_cost += winner.cost;
                }
                None => {
                    return;
                }
            }
        }
        let gid = self.ss[mid].parent;
        let current_cost = self.ss[gid].winners[require]
            .map(|w| w.cost)
            .unwrap_or(f64::MAX);
        if total_cost < current_cost {
            self.ss[gid].winners[require] = Some(Winner {
                plan: mid,
                cost: total_cost,
            });
            if TRACE {
                println!("{:?}", &self.ss);
            }
        }
    }

    fn copy_in(&mut self, mut expr: Expr, gid: GroupID) -> Option<MultiExprID> {
        // Recursively copy in the children.
        for i in 0..expr.len() {
            self.copy_in_new(&mut expr[i]);
        }
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

    fn copy_in_new(&mut self, expr: &mut Expr) {
        if let Leaf { .. } = expr {
            // Nothing to do.
        } else if let Some(mid) = self.ss.find_dup(&expr) {
            let gid = self.ss[mid].parent;
            *expr = Leaf { gid: gid.0 };
        } else {
            // Recursively copy in the children.
            for i in 0..expr.len() {
                self.copy_in_new(&mut expr[i]);
            }
            // Replace expr with a Leaf node.
            let gid = self.ss.reserve();
            let removed = std::mem::replace(expr, Leaf { gid: gid.0 });
            // Initialize a new MultiExpr.
            let mexpr = MultiExpr::new(gid, removed);
            let mid = self.ss.add_mexpr(mexpr).unwrap();
            // Initialize a new Group.
            let props = crate::cardinality_estimation::compute_logical_props(
                &self.ss,
                &mut self.storage,
                mid,
            );
            let lower_bound = compute_lower_bound(&self.ss, &self.ss[mid], &props);
            let group = Group {
                logical: vec![mid],
                physical: vec![],
                props,
                lower_bound,
                upper_bound: Context::empty(),
                winners: Context::empty(),
                explored: false,
            };
            self.ss.add_group(gid, group);
        }
    }

    fn init_costs_using_lower_bound(&self, mid: MultiExprID) -> Vec<Cost> {
        let mut costs = Vec::with_capacity(self.ss[mid].expr.len());
        for i in 0..self.ss[mid].expr.len() {
            let input = leaf(&self.ss[mid].expr[i]);
            let require = PhysicalProp::required(&self.ss[mid].expr, i);
            let cost = match self.ss[input].winners[require].as_ref() {
                Some(winner) => winner.cost,
                None => self.ss[input].lower_bound,
            };
            costs.push(cost);
        }
        costs
    }

    fn winner(&self, gid: GroupID, require: PhysicalProp) -> Expr {
        let mid = self.ss[gid].winners[require]
            .unwrap_or_else(|| {
                panic!(
                    "group @{} has no winner for {:?}\n{:?}",
                    gid.0, require, self.ss
                )
            })
            .plan;
        let mut expr = self.ss[mid].expr.clone();
        for i in 0..expr.len() {
            let require = PhysicalProp::required(&expr, i);
            expr[i] = self.winner(leaf(&expr[i]), require);
        }
        expr
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
