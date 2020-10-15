use encoding::*;
use node::*;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops;

struct SearchSpace {
    groups: Vec<Group>,
    mexprs: Vec<MultiExpr>,
    memo: HashSet<(GroupID, Operator<GroupID>)>,
}

#[derive(Copy, Clone, Hash, Eq, PartialEq)]
struct GroupID(usize);

const UNLINKED: GroupID = GroupID(usize::MAX);

#[derive(Copy, Clone, Hash, Eq, PartialEq)]
struct MultiExprID(usize);

// Group represents a single logical query, which can be realized by many
// specific logical and physical query plans.
struct Group {
    // logical holds a set of equivalent logical query plans.
    logical: Vec<MultiExprID>,
    // physical holds a set of physical implementations of the query plans in logical.
    physical: Vec<MultiExprID>,
    // props holds the logical characteristics of the output of this part of the query plan.
    // No matter how we implement this group using physical operators,
    // these logical characteristics will not change.
    props: LogicalProps,
    // lower_bound is a crude estimate of the lowest-cost plan we could possibly discover.
    // We calculated it by looking at the logical schema of the current group
    // and considering the minimal cost of joins and operator overhead to create it.
    // We use lower_bound to stop early when we know the cost of the current group
    // will exceed the upper_bound.
    lower_bound: Cost,
    // upper_bound is calculated by taking a winning plan and propagating a goal downwards.
    // We need to find a plan that is better than upper_bound, or it will be ignored
    // because it's worse than a plan we already know about.
    upper_bound: Cost,
    // winner holds the best physical plan discovered so far.
    winner: Option<Winner>,
    // explored is marked true on the first invocation of optimizeGroup,
    // whose job is to make sure optimizeExpr is called on every group at least once.
    explored: bool,
}

// MultiExpr represents a part of a Group.
// Unlike Group, which represents *all* equivalent query plans,
// MultiExpr specifies operator at the top of a the query.
struct MultiExpr {
    // Parent group of this expression.
    parent: GroupID,
    // The top operator in this query.
    // Inputs are represented using Group,
    // so they represent a class of equivalent plans rather than a single plan.
    op: Operator<GroupID>,
    // As we try different *logical* transformation rules,
    // we will record the fact that we've already tried this rule on this multi-expression
    // so we can avoid checking it agin. It's safe to mark transformations as complete,
    // because we explore the inputs to each MultiExpr before we start
    // applying transformation rules to the group.
    fired: HashSet<Rule>,
}

#[derive(Copy, Clone)]
struct Winner {
    plan: MultiExprID,
    cost: Cost,
}

struct LogicalProps {
    // cardinality contains the estimated number of rows in the query.
    cardinality: usize,
    // column_unique_cardinality contains the number of distinct values in each column.
    column_unique_cardinality: HashMap<Column, usize>,
}

impl SearchSpace {
    fn new() -> Self {
        Self {
            groups: vec![],
            mexprs: vec![],
            memo: HashSet::new(),
        }
    }

    fn add(&mut self, group: Group) -> GroupID {
        self.groups.push(group);
        GroupID(self.groups.len() - 1)
    }

    fn intern(&mut self, mexpr: MultiExpr) -> Option<MultiExprID> {
        let fingerprint = (mexpr.parent.clone(), mexpr.op.clone());
        if self.memo.contains(&fingerprint) {
            None
        } else {
            self.mexprs.push(mexpr);
            self.memo.insert(fingerprint);
            Some(MultiExprID(self.mexprs.len() - 1))
        }
    }

    // Our implementation of tasks differs from Columbia/Cascades:
    // we use ordinary functions and recursion rather than task objects and a stack of pending tasks.
    // However, the logic and the order of invocation should be exactly the same.

    fn optimize_group(&mut self, gid: GroupID) {
        if self[gid].lower_bound >= self[gid].upper_bound || self[gid].winner.is_some() {
            return;
        }
        for mid in self[gid].physical.clone() {
            self.optimize_inputs(mid);
        }
        for mid in self[gid].logical.clone() {
            self.optimize_expr(mid, false);
        }
    }

    // optimize_expr ensures that every matching rule has been applied to mexpr.
    fn optimize_expr(&mut self, mid: MultiExprID, explore: bool) {
        for rule in Rule::all() {
            // Have we already applied this rule to this multi-expression?
            if self[mid].fired.contains(&rule) {
                continue;
            }
            // If we are exploring, rather than optimizing, skip physical expressions:
            if explore && rule.output_is_physical() {
                continue;
            }
            // Does the pattern match the multi-expression?
            if rule.matches_fast(&self[mid]) {
                // Explore inputs recursively:
                for i in 0..self[mid].op.len() {
                    if rule.has_inputs(i) {
                        self.explore_group(self[mid].op[i])
                    }
                }
                // Apply the rule, potentially adding another MultiExpr to the Group:
                self.apply_rule(&rule, mid, explore);
                self[mid].fired.insert(rule);
            }
        }
    }

    // apply_rule applies rule to mexpr.
    // If the result is a logical expr, optimize it recursively.
    // If the result is a physical expr, evaluate its cost and potentially declare it the current winner.
    fn apply_rule(&mut self, rule: &Rule, mid: MultiExprID, explore: bool) {
        for bind in rule.bind(self, mid) {
            if let Some(bind) = rule.apply(self, bind) {
                // Add mexpr if it isn't already present in the group:
                if let Some(mid) = self.add_binding_to_group(self[mid].parent, bind) {
                    if !self[mid].op.is_logical() {
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

    // explore_group ensures that optimize_expr is called on every group at least once.
    fn explore_group(&mut self, gid: GroupID) {
        if !self[gid].explored {
            for mid in self[gid].logical.clone() {
                self.optimize_expr(mid, true)
            }
            self[gid].explored = true;
        }
    }

    // optimize_inputs takes a physical expr, recursively optimizes all of its inputs,
    // estimates its cost, and potentially declares it the winning physical expr of the group.
    fn optimize_inputs(&mut self, mid: MultiExprID) {
        // Initially, physicalCost is the actual physical cost of the operator at the head of the multi-expression,
        // and inputCosts are the total physical cost of the winning strategy for each input group.
        // If we don't yet have a winner for an inputGroup, we use the lower bound.
        // Thus, physicalCost + sum(inputCosts) = a lower-bound for the total cost of the best strategy for this group.
        let physical_cost = self.physical_cost(mid);
        let mut input_costs = self.init_costs_using_lower_bound(mid);
        for i in 0..self[mid].op.len() {
            let input = self[mid].op[i];
            // If we can prove the cost of this MultiExpr exceeds the upper_bound of the Group, give up:
            if self.stop_early(self[mid].parent, physical_cost, &input_costs) {
                return;
            }
            // Propagate the cost upper_bound downwards to the input group,
            // using the best available estimate of the cost of the other inputs:
            let total_cost = cost_so_far(physical_cost, &input_costs);
            let input_upper_bound =
                self[self[mid].parent].upper_bound - (total_cost - input_costs[i]);
            self[input].upper_bound = input_upper_bound;
            // Optimize input group:
            self.optimize_group(self[mid].op[i]);
            // If we failed to declare a winner, give up:
            if self[input].winner.is_none() {
                return;
            }
            input_costs[i] = self[input].winner.as_ref().unwrap().cost
        }
        // Now that we have a winning strategy for each input and an associated cost,
        // try to declare the current MultiExpr as the winner of its Group:
        self.try_to_declare_winner(mid, physical_cost);
    }

    fn stop_early(&self, gid: GroupID, physical_cost: Cost, input_costs: &Vec<Cost>) -> bool {
        let lower_bound = cost_so_far(physical_cost, input_costs);
        let upper_bound = self[gid].upper_bound;
        lower_bound >= upper_bound
    }

    fn try_to_declare_winner(&mut self, mid: MultiExprID, physical_cost: Cost) {
        let mut total_cost = physical_cost;
        for i in 0..self[mid].op.len() {
            let input = self[mid].op[i];
            match self[input].winner.as_ref() {
                Some(winner) => {
                    total_cost += winner.cost;
                }
                None => {
                    return;
                }
            }
        }
        let gid = self[mid].parent;
        let current_cost = self[gid]
            .winner
            .as_ref()
            .map(|w| w.cost)
            .unwrap_or(f64::MAX);
        if total_cost < current_cost {
            self[gid].winner = Some(Winner {
                plan: mid,
                cost: total_cost,
            })
        }
    }

    fn group_from_expr(&mut self, expr: Expr) -> GroupID {
        // recursively create new groups for each input to expr
        let mexpr = MultiExpr {
            parent: UNLINKED,
            op: expr.0.map(|child| self.group_from_expr(child)),
            fired: HashSet::new(),
        };
        self.group_from_unlinked_mexpr(mexpr)
    }

    fn add_binding_to_group(&mut self, gid: GroupID, bind: Operator<Bind>) -> Option<MultiExprID> {
        let visitor = |child| match child {
            Bind::Group(group) => group,
            Bind::Operator(bind) => self.group_from_bind(*bind),
        };
        let op = bind.map(visitor);
        let mexpr = MultiExpr {
            parent: gid,
            op,
            fired: HashSet::new(),
        };
        if let Some(mid) = self.intern(mexpr) {
            if self[mid].op.is_logical() {
                self[gid].logical.push(mid);
            } else {
                self[gid].physical.push(mid);
            }
            Some(mid)
        } else {
            None
        }
    }

    fn group_from_bind(&mut self, bind: Operator<Bind>) -> GroupID {
        let visitor = |child| match child {
            Bind::Group(group) => group,
            Bind::Operator(bind) => self.group_from_bind(*bind),
        };
        let mexpr = MultiExpr {
            parent: UNLINKED,
            op: bind.map(visitor),
            fired: HashSet::new(),
        };
        self.group_from_unlinked_mexpr(mexpr)
    }

    fn group_from_unlinked_mexpr(&mut self, mut mexpr: MultiExpr) -> GroupID {
        if mexpr.parent != UNLINKED {
            panic!("mexpr is already linked to group {}", mexpr.parent.0);
        }
        // link mexpr to the new group
        let gid = GroupID(self.groups.len());
        mexpr.parent = gid;
        // initialize a new group
        let props = self.compute_logical_props(&mexpr);
        let lower_bound = compute_lower_bound(&props.column_unique_cardinality);
        let mid = self.intern(mexpr).unwrap();
        self.groups.push(Group {
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

    fn compute_logical_props(&self, mexpr: &MultiExpr) -> LogicalProps {
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
                let scope = &self[*input].props.column_unique_cardinality;
                let selectivity = total_selectivity(predicates, scope);
                cardinality = apply_selectivity(self[*input].props.cardinality, selectivity);
                for (c, n) in &self[*input].props.column_unique_cardinality {
                    column_unique_cardinality.insert(c.clone(), apply_selectivity(*n, selectivity));
                }
            }
            LogicalProject(projects, input) => {
                cardinality = self[*input].props.cardinality;
                for (x, c) in projects {
                    let n = scalar_unique_cardinality(
                        &x,
                        &self[*input].props.column_unique_cardinality,
                    );
                    column_unique_cardinality.insert(c.clone(), n);
                }
            }
            LogicalJoin(join, left, right) => {
                let mut scope = HashMap::new();
                for (c, n) in &self[*left].props.column_unique_cardinality {
                    scope.insert(c.clone(), *n);
                }
                for (c, n) in &self[*right].props.column_unique_cardinality {
                    scope.insert(c.clone(), *n);
                }
                let product = self[*left].props.cardinality * self[*right].props.cardinality;
                for (c, n) in &self[*left].props.column_unique_cardinality {
                    column_unique_cardinality.insert(c.clone(), *n);
                }
                for (c, n) in &self[*right].props.column_unique_cardinality {
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
                    let n = self[*input].props.column_unique_cardinality[&c];
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
                for (c, n) in &self[*input].props.column_unique_cardinality {
                    if *limit < *n {
                        column_unique_cardinality.insert(c.clone(), *limit);
                    } else {
                        column_unique_cardinality.insert(c.clone(), *n);
                    }
                }
            }
            LogicalSort(_, input) => {
                cardinality = self[*input].props.cardinality;
                column_unique_cardinality = self[*input].props.column_unique_cardinality.clone();
            }
            LogicalUnion(left, right) => {
                cardinality = self[*left].props.cardinality + self[*right].props.cardinality;
                column_unique_cardinality = max_cuc(
                    &self[*left].props.column_unique_cardinality,
                    &self[*right].props.column_unique_cardinality,
                );
            }
            LogicalIntersect(left, right) => todo!("intersect"),
            LogicalExcept(left, right) => {
                cardinality = self[*left].props.cardinality;
                column_unique_cardinality = self[*left].props.column_unique_cardinality.clone();
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
    fn physical_cost(&self, mid: MultiExprID) -> Cost {
        let parent = self[mid].parent;
        match &self[mid].op {
            TableFreeScan { .. } => 0.0,
            SeqScan { .. } => {
                let output = self[parent].props.cardinality as f64;
                let blocks = f64::max(1.0, output * TUPLE_SIZE / BLOCK_SIZE);
                blocks * COST_READ_BLOCK
            }
            IndexScan { .. } => {
                let blocks = self[parent].props.cardinality as f64;
                blocks * COST_READ_BLOCK
            }
            Filter(predicates, input) => {
                let input = self[*input].props.cardinality as f64;
                let columns = predicates.len() as f64;
                input * columns * COST_CPU_PRED
            }
            Project(compute, input) => {
                let output = self[*input].props.cardinality as f64;
                let columns = compute.len() as f64;
                output * columns * COST_CPU_EVAL
            }
            NestedLoop(join, left, right) => {
                let build = self[*left].props.cardinality as f64;
                let probe = self[*right].props.cardinality as f64;
                let iterations = build * probe;
                build * COST_ARRAY_BUILD + iterations * COST_ARRAY_PROBE
            }
            HashJoin(join, equals, left, right) => {
                let build = self[*left].props.cardinality as f64;
                let probe = self[*right].props.cardinality as f64;
                build * COST_HASH_BUILD + probe * COST_HASH_PROBE
            }
            CreateTempTable { .. } => todo!("CreateTempTable"),
            GetTempTable { .. } => todo!("GetTempTable"),
            Aggregate {
                group_by,
                aggregate,
                input,
            } => {
                let n = self[*input].props.cardinality as f64;
                let n_group_by = n * group_by.len() as f64;
                let n_aggregate = n * aggregate.len() as f64;
                n_group_by * COST_HASH_BUILD + n_aggregate * COST_CPU_APPLY
            }
            Limit { .. } => 0.0,
            Sort { .. } => {
                let card = self[parent].props.cardinality.max(1) as f64;
                let log = 2.0 * card * f64::log2(card);
                log * COST_CPU_COMP_MOVE
            }
            Union(_, _) | Intersect(_, _) | Except(_, _) => 0.0,
            Values(_, _) => 0.0,
            Insert(_, _, input) | Update(_, input) | Delete(_, input) => {
                let length = self[*input].props.cardinality as f64;
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

    fn init_costs_using_lower_bound(&self, mid: MultiExprID) -> Vec<Cost> {
        let mut costs = Vec::with_capacity(self[mid].op.len());
        for i in 0..self[mid].op.len() {
            let input = self[mid].op[i];
            let cost = match self[input].winner.as_ref() {
                Some(winner) => winner.cost,
                None => self[input].lower_bound,
            };
            costs.push(cost);
        }
        costs
    }

    fn introduces(&self, gid: GroupID, column: &Column) -> bool {
        // use the first logical expression in the group to check for column
        let mid = self[gid].logical[0];
        // check if the operator at the head of the logical expression introduces column
        let op = &self[mid].op;
        if op.introduces(column) {
            return true;
        }
        // check if any of the inputs to the logical expression introduces column
        for i in 0..op.len() {
            if self.introduces(op[i], column) {
                return true;
            }
        }
        return false;
    }

    fn winner(&self, gid: GroupID) -> Expr {
        let mid = self[gid].winner.unwrap().plan;
        Expr(Box::new(self[mid].op.clone().map(|gid| self.winner(gid))))
    }
}

impl ops::Index<GroupID> for SearchSpace {
    type Output = Group;

    fn index(&self, index: GroupID) -> &Self::Output {
        &self.groups[index.0]
    }
}

impl ops::Index<MultiExprID> for SearchSpace {
    type Output = MultiExpr;

    fn index(&self, index: MultiExprID) -> &Self::Output {
        &self.mexprs[index.0]
    }
}

impl ops::IndexMut<GroupID> for SearchSpace {
    fn index_mut(&mut self, index: GroupID) -> &mut Self::Output {
        &mut self.groups[index.0]
    }
}

impl ops::IndexMut<MultiExprID> for SearchSpace {
    fn index_mut(&mut self, index: MultiExprID) -> &mut Self::Output {
        &mut self.mexprs[index.0]
    }
}

impl fmt::Debug for SearchSpace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "")?;
        for (i, group) in self.groups.iter().enumerate() {
            writeln!(f, "\t{}:", i)?;
            for mid in group.logical.clone() {
                writeln!(f, "\t\t{:?}", self[mid])?;
            }
            for mid in group.physical.clone() {
                writeln!(f, "\t\t{:?}", self[mid])?;
            }
        }
        Ok(())
    }
}

impl fmt::Debug for MultiExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.op.name())?;
        for i in 0..self.op.len() {
            write!(f, " {}", self.op[i].0)?;
        }
        Ok(())
    }
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

pub type Cost = f64;

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

#[derive(Debug, Eq, PartialEq, Hash)]
enum Rule {
    // Rewrite rules
    LogicalInnerJoinCommutivity,
    LogicalInnerJoinAssociativity,
    // Implementation rules
    LogicalGetToTableFreeScan,
    LogicalGetToSeqScan,
    LogicalGetToIndexScan,
    LogicalFilterToFilter,
    LogicalProjectToProject,
    LogicalJoinToNestedLoop,
    LogicalJoinToHashJoin,
    LogicalAggregateToAggregate,
    LogicalLimitToLimit,
    LogicalSortToSort,
    LogicallUnionToUnion,
    LogicalIntersectToIntersect,
    LogicalExceptToExcept,
    LogicalWithToCreateTempTable,
    LogicalGetWithToGetTempTable,
    LogicalInsertToInsert,
    LogicalValuesToValues,
    LogicalUpdateToUpdate,
    LogicalDeleteToDelete,
    LogicalCreateDatabaseToCreateDatabase,
    LogicalCreateTableToCreateTable,
    LogicalCreateIndexToCreateIndex,
    LogicalAlterTableToAlterTable,
    LogicalDropToDrop,
    LogicalRenameToRename,
}

impl Rule {
    fn output_is_physical(&self) -> bool {
        match self {
            Rule::LogicalGetToTableFreeScan
            | Rule::LogicalGetToSeqScan
            | Rule::LogicalGetToIndexScan
            | Rule::LogicalFilterToFilter
            | Rule::LogicalProjectToProject
            | Rule::LogicalJoinToNestedLoop
            | Rule::LogicalJoinToHashJoin
            | Rule::LogicalAggregateToAggregate
            | Rule::LogicalLimitToLimit
            | Rule::LogicalSortToSort
            | Rule::LogicallUnionToUnion
            | Rule::LogicalIntersectToIntersect
            | Rule::LogicalExceptToExcept
            | Rule::LogicalWithToCreateTempTable
            | Rule::LogicalGetWithToGetTempTable
            | Rule::LogicalInsertToInsert
            | Rule::LogicalValuesToValues
            | Rule::LogicalUpdateToUpdate
            | Rule::LogicalDeleteToDelete
            | Rule::LogicalCreateDatabaseToCreateDatabase
            | Rule::LogicalCreateTableToCreateTable
            | Rule::LogicalCreateIndexToCreateIndex
            | Rule::LogicalAlterTableToAlterTable
            | Rule::LogicalDropToDrop
            | Rule::LogicalRenameToRename => true,
            _ => false,
        }
    }

    fn promise(&self) -> isize {
        todo!("promise")
    }

    // Quickly check if rule matches expression *without* exploring the inputs to the expression.
    fn matches_fast(&self, mexpr: &MultiExpr) -> bool {
        match (self, &mexpr.op) {
            (Rule::LogicalInnerJoinCommutivity, LogicalJoin(Join::Inner(_), _, _))
            | (Rule::LogicalInnerJoinAssociativity, LogicalJoin(Join::Inner(_), _, _))
            | (Rule::LogicalGetToTableFreeScan, LogicalSingleGet)
            | (Rule::LogicalGetToSeqScan, LogicalGet(_))
            | (Rule::LogicalGetToIndexScan, LogicalFilter(_, _))
            | (Rule::LogicalFilterToFilter, LogicalFilter(_, _))
            | (Rule::LogicalProjectToProject, LogicalProject(_, _))
            | (Rule::LogicalJoinToNestedLoop, LogicalJoin(_, _, _))
            | (Rule::LogicalJoinToHashJoin, LogicalJoin(_, _, _))
            | (Rule::LogicalAggregateToAggregate, LogicalAggregate { .. })
            | (Rule::LogicalLimitToLimit, LogicalLimit { .. })
            | (Rule::LogicalSortToSort, LogicalSort(_, _))
            | (Rule::LogicallUnionToUnion, LogicalUnion(_, _))
            | (Rule::LogicalIntersectToIntersect, LogicalIntersect(_, _))
            | (Rule::LogicalExceptToExcept, LogicalExcept(_, _))
            | (Rule::LogicalWithToCreateTempTable, LogicalWith(_, _, _))
            | (Rule::LogicalGetWithToGetTempTable, LogicalGetWith(_))
            | (Rule::LogicalInsertToInsert, LogicalInsert(_, _, _))
            | (Rule::LogicalValuesToValues, LogicalValues(_, _, _))
            | (Rule::LogicalUpdateToUpdate, LogicalUpdate(_, _))
            | (Rule::LogicalDeleteToDelete, LogicalDelete(_, _))
            | (Rule::LogicalCreateDatabaseToCreateDatabase, LogicalCreateDatabase(_))
            | (Rule::LogicalCreateTableToCreateTable, LogicalCreateTable { .. })
            | (Rule::LogicalCreateIndexToCreateIndex, LogicalCreateIndex { .. })
            | (Rule::LogicalAlterTableToAlterTable, LogicalAlterTable { .. })
            | (Rule::LogicalDropToDrop, LogicalDrop { .. })
            | (Rule::LogicalRenameToRename, LogicalRename { .. }) => true,
            _ => false,
        }
    }

    fn has_inputs(&self, i: usize) -> bool {
        match (self, i) {
            (Rule::LogicalGetToIndexScan, 0) => true,
            _ => false,
        }
    }

    fn bind(&self, ss: &SearchSpace, mid: MultiExprID) -> Vec<Operator<Bind>> {
        let mut binds = vec![];
        match self {
            Rule::LogicalInnerJoinAssociativity => {
                if let LogicalJoin(Join::Inner(parent_predicates), left, right) = &ss[mid].op {
                    for left in &ss[*left].logical {
                        if let LogicalJoin(Join::Inner(left_predicates), left_left, left_middle) =
                            &ss[*left].op
                        {
                            binds.push(LogicalJoin(
                                Join::Inner(parent_predicates.clone()),
                                Bind::Operator(Box::new(LogicalJoin(
                                    Join::Inner(left_predicates.clone()),
                                    Bind::Group(*left_left),
                                    Bind::Group(*left_middle),
                                ))),
                                Bind::Group(*right),
                            ))
                        }
                    }
                }
            }
            Rule::LogicalGetToIndexScan => {
                if let LogicalFilter(predicates, input) = &ss[mid].op {
                    for input in &ss[*input].logical {
                        if let LogicalGet(table) = &ss[*input].op {
                            if can_index_scan(predicates, table) {
                                binds.push(LogicalFilter(
                                    predicates.clone(),
                                    Bind::Operator(Box::new(LogicalGet(table.clone()))),
                                ))
                            }
                        }
                    }
                }
            }
            _ => binds.push(ss[mid].op.clone().map(|group| Bind::Group(group))),
        }
        binds
    }

    fn apply(&self, ss: &SearchSpace, bind: Operator<Bind>) -> Option<Operator<Bind>> {
        match self {
            Rule::LogicalInnerJoinCommutivity => {
                if let LogicalJoin(Join::Inner(join_predicates), left, right) = bind {
                    return Some(LogicalJoin(
                        Join::Inner(join_predicates.clone()),
                        right,
                        left,
                    ));
                }
            }
            // Rearrange left-deep join into right-deep join.
            //
            //             +---+ parent +---+
            //             |                |
            //             +                +
            //      +--+leftJoin+---+     right
            //      |               |
            //      +               +
            //   leftLeft      leftMiddle
            Rule::LogicalInnerJoinAssociativity => {
                if let LogicalJoin(Join::Inner(parent_predicates), Bind::Operator(left), right) =
                    bind
                {
                    if let LogicalJoin(Join::Inner(left_predicates), left_left, left_middle) = *left
                    {
                        let mut new_parent_predicates = vec![];
                        let mut new_right_predicates = vec![];
                        todo!("redistribute predicates");
                        return Some(LogicalJoin(
                            Join::Inner(new_parent_predicates),
                            left_left,
                            Bind::Operator(Box::new(LogicalJoin(
                                Join::Inner(new_right_predicates),
                                left_middle,
                                right,
                            ))),
                        ));
                    }
                }
            }
            Rule::LogicalGetToTableFreeScan => {
                if let LogicalSingleGet = bind {
                    todo!()
                }
            }
            Rule::LogicalGetToSeqScan => {
                if let LogicalGet(table) = bind {
                    return Some(SeqScan(table));
                }
            }
            Rule::LogicalGetToIndexScan => {
                if let LogicalFilter(predicates, Bind::Operator(input)) = bind {
                    if let LogicalGet(table) = *input {
                        return index_scan(predicates, table);
                    }
                }
            }
            Rule::LogicalFilterToFilter => {
                if let LogicalFilter(predicates, input) = bind {
                    return Some(Filter(predicates, input));
                }
            }
            Rule::LogicalProjectToProject => {
                if let LogicalProject(_, _) = bind {
                    todo!()
                }
            }
            Rule::LogicalJoinToNestedLoop => {
                if let LogicalJoin(join, left, right) = bind {
                    return Some(NestedLoop(join, left, right));
                }
            }
            Rule::LogicalJoinToHashJoin => {
                if let LogicalJoin(join, Bind::Group(left), Bind::Group(right)) = bind {
                    let (hash_predicates, join) = match join {
                        Join::Inner(join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(ss, join_predicates, left, right);
                            let join = Join::Inner(remaining_predicates);
                            (hash_predicates, join)
                        }
                        Join::Right(join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(ss, join_predicates, left, right);
                            let join = Join::Right(remaining_predicates);
                            (hash_predicates, join)
                        }
                        Join::Outer(join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(ss, join_predicates, left, right);
                            let join = Join::Outer(remaining_predicates);
                            (hash_predicates, join)
                        }
                        Join::Semi(join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(ss, join_predicates, left, right);
                            let join = Join::Semi(remaining_predicates);
                            (hash_predicates, join)
                        }
                        Join::Anti(join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(ss, join_predicates, left, right);
                            let join = Join::Anti(remaining_predicates);
                            (hash_predicates, join)
                        }
                        Join::Single(join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(ss, join_predicates, left, right);
                            let join = Join::Single(remaining_predicates);
                            (hash_predicates, join)
                        }
                        Join::Mark(column, join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(ss, join_predicates, left, right);
                            let join = Join::Mark(column, remaining_predicates);
                            (hash_predicates, join)
                        }
                    };
                    if !hash_predicates.is_empty() {
                        return Some(HashJoin(
                            join,
                            hash_predicates,
                            Bind::Group(left),
                            Bind::Group(right),
                        ));
                    }
                }
            }
            Rule::LogicalAggregateToAggregate => {
                if let LogicalAggregate { .. } = bind {
                    todo!()
                }
            }
            Rule::LogicalLimitToLimit => {
                if let LogicalLimit { .. } = bind {
                    todo!()
                }
            }
            Rule::LogicalSortToSort => {
                if let LogicalSort(_, _) = bind {
                    todo!()
                }
            }
            Rule::LogicallUnionToUnion => {
                if let LogicalUnion(_, _) = bind {
                    todo!()
                }
            }
            Rule::LogicalIntersectToIntersect => {
                if let LogicalIntersect(_, _) = bind {
                    todo!()
                }
            }
            Rule::LogicalExceptToExcept => {
                if let LogicalExcept(_, _) = bind {
                    todo!()
                }
            }
            Rule::LogicalWithToCreateTempTable => {
                if let LogicalWith(_, _, _) = bind {
                    todo!()
                }
            }
            Rule::LogicalGetWithToGetTempTable => {
                if let LogicalGetWith(_) = bind {
                    todo!()
                }
            }
            Rule::LogicalInsertToInsert => {
                if let LogicalInsert(_, _, _) = bind {
                    todo!()
                }
            }
            Rule::LogicalValuesToValues => {
                if let LogicalValues(_, _, _) = bind {
                    todo!()
                }
            }
            Rule::LogicalUpdateToUpdate => {
                if let LogicalUpdate(_, _) = bind {
                    todo!()
                }
            }
            Rule::LogicalDeleteToDelete => {
                if let LogicalDelete(_, _) = bind {
                    todo!()
                }
            }
            Rule::LogicalCreateDatabaseToCreateDatabase => {
                if let LogicalCreateDatabase { .. } = bind {
                    todo!()
                }
            }
            Rule::LogicalCreateTableToCreateTable => {
                if let LogicalCreateTable { .. } = bind {
                    todo!()
                }
            }
            Rule::LogicalCreateIndexToCreateIndex => {
                if let LogicalCreateIndex { .. } = bind {
                    todo!()
                }
            }
            Rule::LogicalAlterTableToAlterTable => {
                if let LogicalAlterTable { .. } = bind {
                    todo!()
                }
            }
            Rule::LogicalDropToDrop => {
                if let LogicalDrop { .. } = bind {
                    todo!()
                }
            }
            Rule::LogicalRenameToRename => {
                if let LogicalRename { .. } = bind {
                    todo!()
                }
            }
        }
        None
    }

    fn all() -> Vec<Rule> {
        vec![
            Rule::LogicalInnerJoinCommutivity,
            Rule::LogicalInnerJoinAssociativity,
            Rule::LogicalGetToTableFreeScan,
            Rule::LogicalGetToSeqScan,
            Rule::LogicalGetToIndexScan,
            Rule::LogicalFilterToFilter,
            Rule::LogicalProjectToProject,
            Rule::LogicalJoinToNestedLoop,
            Rule::LogicalJoinToHashJoin,
            Rule::LogicalAggregateToAggregate,
            Rule::LogicalLimitToLimit,
            Rule::LogicalSortToSort,
            Rule::LogicallUnionToUnion,
            Rule::LogicalIntersectToIntersect,
            Rule::LogicalExceptToExcept,
            Rule::LogicalWithToCreateTempTable,
            Rule::LogicalGetWithToGetTempTable,
            Rule::LogicalInsertToInsert,
            Rule::LogicalValuesToValues,
            Rule::LogicalUpdateToUpdate,
            Rule::LogicalDeleteToDelete,
            Rule::LogicalCreateDatabaseToCreateDatabase,
            Rule::LogicalCreateTableToCreateTable,
            Rule::LogicalCreateIndexToCreateIndex,
            Rule::LogicalAlterTableToAlterTable,
            Rule::LogicalDropToDrop,
            Rule::LogicalRenameToRename,
        ]
    }
}

enum Bind {
    Group(GroupID),
    Operator(Box<Operator<Bind>>),
}

fn can_index_scan(predicates: &Vec<Scalar>, table: &Table) -> bool {
    index_scan(predicates.clone(), table.clone()).is_some()
}

fn index_scan(predicates: Vec<Scalar>, table: Table) -> Option<Operator<Bind>> {
    // TODO real implementation
    if let Some((column, scalar)) = match_indexed_lookup(predicates) {
        if column.table.clone() == Some(table.name.clone()) {
            return Some(IndexScan {
                table,
                equals: vec![(column, scalar)],
            });
        }
    }
    None
}

fn match_indexed_lookup(mut predicates: Vec<Scalar>) -> Option<(Column, Scalar)> {
    if predicates.len() == 1 {
        if let Scalar::Call(Function::Equal, mut arguments, _) = predicates.pop().unwrap() {
            match (arguments.pop().unwrap(), arguments.pop().unwrap()) {
                (Scalar::Column(column), equals) | (equals, Scalar::Column(column))
                    if column.name.ends_with("_id") =>
                {
                    return Some((column, equals))
                }
                _ => {}
            }
        }
    }
    None
}

fn hash_join(
    ss: &SearchSpace,
    mut join_predicates: Vec<Scalar>,
    left: GroupID,
    right: GroupID,
) -> (Vec<(Scalar, Scalar)>, Vec<Scalar>) {
    let mut hash_predicates = vec![];
    let mut remaining_predicates = vec![];
    for predicate in join_predicates.drain(0..) {
        if let Scalar::Call(Function::Equal, mut arguments, _) = predicate {
            let right_side = arguments.pop().unwrap();
            let left_side = arguments.pop().unwrap();
            if left_side.columns().all(|c| ss.introduces(left, c))
                && right_side.columns().all(|c| ss.introduces(right, c))
            {
                hash_predicates.push((left_side, right_side))
            } else if right_side.columns().all(|c| ss.introduces(left, c))
                && left_side.columns().all(|c| ss.introduces(right, c))
            {
                hash_predicates.push((right_side, left_side))
            } else {
                remaining_predicates.push(Scalar::Call(
                    Function::Equal,
                    vec![left_side, right_side],
                    Type::Bool,
                ));
            }
        } else {
            remaining_predicates.push(predicate);
        }
    }
    (hash_predicates, remaining_predicates)
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
    let gid = ss.group_from_expr(expr);
    ss.optimize_group(gid);
    ss.winner(gid)
}
