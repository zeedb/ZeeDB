use encoding::*;
use node::*;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash;
use std::ops;
use std::ptr;
use std::rc::Rc;

// Group represents a single logical query, which can be realized by many
// specific logical and physical query plans.
struct Group {
    // logical holds a set of equivalent logical query plans.
    logical: HashSet<MultiExpr>,
    // physical holds a set of physical implementations of the query plans in logical.
    physical: HashSet<MultiExpr>,
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
    // The top operator in this query.
    // Inputs are represented using Group,
    // so they represent a class of equivalent plans rather than a single plan.
    op: Operator<Rc<RefCell<Group>>>,
    // As we try different *logical* transformation rules,
    // we will record the fact that we've already tried this rule on this multi-expression
    // so we can avoid checking it agin. It's safe to mark transformations as complete,
    // because we explore the inputs to each multiExpr before we start
    // applying transformation rules to the group.
    fired: HashSet<Rule>,
}

impl hash::Hash for MultiExpr {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        todo!()
    }
}

impl PartialEq for MultiExpr {
    fn eq(&self, other: &Self) -> bool {
        todo!()
    }
}

impl Eq for MultiExpr {}

struct Winner {
    plan: Expr,
    cost: Cost,
}

struct LogicalProps {
    // cardinality contains the estimated number of rows in the query.
    cardinality: usize,
    // column_unique_cardinality contains the number of distinct values in each column.
    column_unique_cardinality: HashMap<Column, usize>,
}

impl Group {
    fn new(mexpr: MultiExpr) -> Self {
        let props = compute_logical_props(&mexpr);
        let lower_bound = compute_lower_bound(&props.column_unique_cardinality);
        let mut logical = HashSet::new();
        logical.insert(mexpr);
        Group {
            logical: logical,
            physical: HashSet::new(),
            props,
            lower_bound,
            upper_bound: f64::MAX,
            winner: None,
            explored: false,
        }
    }

    fn add(&mut self, bind: Operator<Bind>) -> Option<&mut MultiExpr> {
        let mexpr = MultiExpr::unbind(bind);
        if mexpr.op.is_logical() {
            if self.logical.insert(mexpr) {
                return todo!();
            }
        } else {
            if self.physical.insert(mexpr) {
                return todo!();
            }
        }
        None
    }

    fn correlated(&self, column: &Column) -> bool {
        todo!()
    }
}

impl MultiExpr {
    fn new(expr: Expr) -> Self {
        let op = expr
            .0
            .map(|child| Rc::new(RefCell::new(Group::new(MultiExpr::new(child)))));
        let fired = HashSet::new();
        MultiExpr { op, fired }
    }

    fn unbind(bind: Operator<Bind>) -> Self {
        let op = bind.map(|child| match child {
            Bind::Group(group) => group,
            Bind::Operator(bind) => Rc::new(RefCell::new(Group::new(MultiExpr::unbind(*bind)))),
        });
        let fired = HashSet::new();
        MultiExpr { op, fired }
    }
}

fn compute_logical_props(mexpr: &MultiExpr) -> LogicalProps {
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
            let scope = &input.borrow().props.column_unique_cardinality;
            let selectivity = total_selectivity(predicates, scope);
            cardinality = apply_selectivity(input.borrow().props.cardinality, selectivity);
            for (c, n) in &input.borrow().props.column_unique_cardinality {
                column_unique_cardinality.insert(c.clone(), apply_selectivity(*n, selectivity));
            }
        }
        LogicalProject(projects, input) => {
            cardinality = input.borrow().props.cardinality;
            for (x, c) in projects {
                let n =
                    scalar_unique_cardinality(&x, &input.borrow().props.column_unique_cardinality);
                column_unique_cardinality.insert(c.clone(), n);
            }
        }
        LogicalJoin(join, left, right) => {
            let mut scope = HashMap::new();
            for (c, n) in &left.borrow().props.column_unique_cardinality {
                scope.insert(c.clone(), *n);
            }
            for (c, n) in &right.borrow().props.column_unique_cardinality {
                scope.insert(c.clone(), *n);
            }
            let product = left.borrow().props.cardinality * right.borrow().props.cardinality;
            for (c, n) in &left.borrow().props.column_unique_cardinality {
                column_unique_cardinality.insert(c.clone(), *n);
            }
            for (c, n) in &right.borrow().props.column_unique_cardinality {
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
                let n = input.borrow().props.column_unique_cardinality[&c];
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
            for (c, n) in &input.borrow().props.column_unique_cardinality {
                if *limit < *n {
                    column_unique_cardinality.insert(c.clone(), *limit);
                } else {
                    column_unique_cardinality.insert(c.clone(), *n);
                }
            }
        }
        LogicalSort(_, input) => {
            cardinality = input.borrow().props.cardinality;
            column_unique_cardinality = input.borrow().props.column_unique_cardinality.clone();
        }
        LogicalUnion(left, right) => {
            cardinality = left.borrow().props.cardinality + right.borrow().props.cardinality;
            column_unique_cardinality = max_cuc(
                &left.borrow().props.column_unique_cardinality,
                &right.borrow().props.column_unique_cardinality,
            );
        }
        LogicalIntersect(left, right) => todo!("intersect"),
        LogicalExcept(left, right) => {
            cardinality = left.borrow().props.cardinality;
            column_unique_cardinality = left.borrow().props.column_unique_cardinality.clone();
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

    fn bind(&self, mexpr: &MultiExpr) -> Vec<Operator<Bind>> {
        let mut binds = vec![];
        match self {
            Rule::LogicalInnerJoinAssociativity => {
                if let LogicalJoin(Join::Inner(parent_predicates), left, right) = &mexpr.op {
                    for left in &left.borrow().logical {
                        if let LogicalJoin(Join::Inner(left_predicates), left_left, left_middle) =
                            &left.op
                        {
                            binds.push(LogicalJoin(
                                Join::Inner(parent_predicates.clone()),
                                Bind::Operator(Box::new(LogicalJoin(
                                    Join::Inner(left_predicates.clone()),
                                    Bind::Group(left_left.clone()),
                                    Bind::Group(left_middle.clone()),
                                ))),
                                Bind::Group(right.clone()),
                            ))
                        }
                    }
                }
            }
            Rule::LogicalGetToIndexScan => {
                if let LogicalFilter(predicates, input) = &mexpr.op {
                    for input in &input.borrow().logical {
                        if let LogicalGet(table) = &input.op {
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
            _ => binds.push(mexpr.op.clone().map(|group| Bind::Group(group))),
        }
        binds
    }

    fn apply(&self, bind: Operator<Bind>) -> Option<Operator<Bind>> {
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
                    return Some(PhysicalSeqScan(table));
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
                    return Some(PhysicalFilter(predicates, input));
                }
            }
            Rule::LogicalProjectToProject => {
                if let LogicalProject(_, _) = bind {
                    todo!()
                }
            }
            Rule::LogicalJoinToNestedLoop => {
                if let LogicalJoin(join, left, right) = bind {
                    return Some(PhysicalNestedLoop(join, left, right));
                }
            }
            Rule::LogicalJoinToHashJoin => {
                if let LogicalJoin(join, Bind::Group(left), Bind::Group(right)) = bind {
                    let (hash_predicates, join) = match join {
                        Join::Inner(join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(join_predicates, &left.borrow(), &right.borrow());
                            let join = Join::Inner(remaining_predicates);
                            (hash_predicates, join)
                        }
                        Join::Right(join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(join_predicates, &left.borrow(), &right.borrow());
                            let join = Join::Right(remaining_predicates);
                            (hash_predicates, join)
                        }
                        Join::Outer(join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(join_predicates, &left.borrow(), &right.borrow());
                            let join = Join::Outer(remaining_predicates);
                            (hash_predicates, join)
                        }
                        Join::Semi(join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(join_predicates, &left.borrow(), &right.borrow());
                            let join = Join::Semi(remaining_predicates);
                            (hash_predicates, join)
                        }
                        Join::Anti(join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(join_predicates, &left.borrow(), &right.borrow());
                            let join = Join::Anti(remaining_predicates);
                            (hash_predicates, join)
                        }
                        Join::Single(join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(join_predicates, &left.borrow(), &right.borrow());
                            let join = Join::Single(remaining_predicates);
                            (hash_predicates, join)
                        }
                        Join::Mark(column, join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(join_predicates, &left.borrow(), &right.borrow());
                            let join = Join::Mark(column, remaining_predicates);
                            (hash_predicates, join)
                        }
                    };
                    if !hash_predicates.is_empty() {
                        return Some(PhysicalHashJoin(
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
    Group(Rc<RefCell<Group>>),
    Operator(Box<Operator<Bind>>),
}

fn can_index_scan(predicates: &Vec<Scalar>, table: &Table) -> bool {
    index_scan(predicates.clone(), table.clone()).is_some()
}

fn index_scan(predicates: Vec<Scalar>, table: Table) -> Option<Operator<Bind>> {
    // TODO real implementation
    if let Some((column, scalar)) = match_indexed_lookup(predicates) {
        if column.table.clone() == Some(table.name.clone()) {
            return Some(PhysicalIndexScan {
                table,
                equals: vec![(column, scalar)],
            });
        }
    }
    None
}

fn match_indexed_lookup(mut predicates: Vec<Scalar>) -> Option<(Column, Scalar)> {
    if predicates.len() == 0 {
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
    mut join_predicates: Vec<Scalar>,
    left: &Group,
    right: &Group,
) -> (Vec<(Scalar, Scalar)>, Vec<Scalar>) {
    let mut hash_predicates = vec![];
    let mut remaining_predicates = vec![];
    for predicate in join_predicates.drain(0..) {
        if let Scalar::Call(Function::Equal, mut arguments, _) = predicate {
            let right_side = arguments.pop().unwrap();
            let left_side = arguments.pop().unwrap();
            if left_side.columns().all(|c| !left.correlated(c))
                && right_side.columns().all(|c| !right.correlated(c))
            {
                hash_predicates.push((left_side, right_side))
            } else if right_side.columns().all(|c| !left.correlated(c))
                && left_side.columns().all(|c| !right.correlated(c))
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

// Our implementation of tasks differs from Columbia/Cascades:
// we use ordinary functions and recursion rather than task objects and a stack of pending tasks.
// However, the logic and the order of invocation should be exactly the same.

fn optimize_group(group: &mut Group) {
    if group.lower_bound >= group.upper_bound || group.winner.is_some() {
        return;
    }
    for mut e in &group.physical {
        optimize_inputs(group, &mut e);
    }
    for mut e in &group.logical {
        optimize_expr(group, &mut e, false);
    }
}

// optimize_expr ensures that every matching rule has been applied to mexpr.
fn optimize_expr(group: &mut Group, mexpr: &mut MultiExpr, explore: bool) {
    for rule in Rule::all() {
        // Have we already applied this rule to this multi-expression?
        if mexpr.fired.contains(&rule) {
            continue;
        }
        // If we are exploring, rather than optimizing, skip physical expressions:
        if explore && rule.output_is_physical() {
            continue;
        }
        // Does the pattern match the multi-expression?
        if rule.matches_fast(mexpr) {
            // Explore inputs recursively:
            for i in 0..mexpr.op.len() {
                if rule.has_inputs(i) {
                    explore_group(&mut mexpr.op[i].borrow_mut())
                }
            }
            // Apply the rule, potentially adding another MultiExpr to the Group:
            apply_rule(&rule, group, mexpr, explore);
            mexpr.fired.insert(rule);
        }
    }
}

// apply_rule applies rule to mexpr.
// If the result is a logical expr, optimize it recursively.
// If the result is a physical expr, evaluate its cost and potentially declare it the current winner.
fn apply_rule(rule: &Rule, group: &mut Group, mexpr: &MultiExpr, explore: bool) {
    for bind in rule.bind(mexpr) {
        if let Some(bind) = rule.apply(bind) {
            // Add mexpr if it isn't already present in the group:
            if let Some(mut mexpr) = group.add(bind) {
                if !mexpr.op.is_logical() {
                    // If rule produced a physical implementation, cost the implementation:
                    optimize_inputs(group, mexpr);
                } else {
                    // If rule produced a new new logical expression, optimize it:
                    optimize_expr(group, mexpr, explore)
                }
            }
        }
    }
}

// explore_group ensures that optimize_expr is called on every group at least once.
fn explore_group(group: &mut Group) {
    if !group.explored {
        for mut mexpr in &group.logical {
            optimize_expr(group, &mut mexpr, true)
        }
        group.explored = true;
    }
}

// optimize_inputs takes a physical expr, recursively optimizes all of its inputs,
// estimates its cost, and potentially declares it the winning physical expr of the group.
fn optimize_inputs(group: &mut Group, mexpr: &mut MultiExpr) {
    // Initially, physicalCost is the actual physical cost of the operator at the head of the multi-expression,
    // and inputCosts are the total physical cost of the winning strategy for each input group.
    // If we don't yet have a winner for an inputGroup, we use the lower bound.
    // Thus, physicalCost + sum(inputCosts) = a lower-bound for the total cost of the best strategy for this group.
    let physical_cost = physical_cost(group, mexpr);
    let mut input_costs = init_costs_using_lower_bound(mexpr);
    for i in 0..mexpr.op.len() {
        // If we can prove the cost of this MultiExpr exceeds the upper_bound of the Group, give up:
        if stop_early(group, physical_cost, &input_costs) {
            return;
        }
        // Propagate the cost upper_bound downwards to the input group,
        // using the best available estimate of the cost of the other inputs:
        let total_cost = cost_so_far(physical_cost, &input_costs);
        let input_upper_bound = group.upper_bound - (total_cost - input_costs[i]);
        mexpr.op[i].borrow_mut().upper_bound = input_upper_bound;
        // Optimize input group:
        optimize_group(&mut mexpr.op[i].borrow_mut());
        // If we failed to declare a winner, give up:
        if mexpr.op[i].borrow().winner.is_none() {
            return;
        }
        input_costs[i] = mexpr.op[i].borrow().winner.as_ref().unwrap().cost
    }
    // Now that we have a winning strategy for each input and an associated cost,
    // try to declare the current MultiExpr as the winner of its Group:
    try_to_declare_winner(group, mexpr, physical_cost);
}

// physicalCost computes the local cost of the physical operator at the head of a multi-expression tree.
// To compute the total physical cost of an expression, you need to choose a single physical expression
// at every node of the tree and add up the local costs.
fn physical_cost(group: &Group, mexpr: &MultiExpr) -> Cost {
    match &mexpr.op {
        PhysicalTableFreeScan { .. } => 0.0,
        PhysicalSeqScan { .. } => {
            let output = group.props.cardinality as f64;
            let blocks = f64::max(1.0, output * TUPLE_SIZE / BLOCK_SIZE);
            blocks * COST_READ_BLOCK
        }
        PhysicalIndexScan { .. } => {
            let blocks = group.props.cardinality as f64;
            blocks * COST_READ_BLOCK
        }
        PhysicalFilter(predicates, input) => {
            let input = input.borrow().props.cardinality as f64;
            let columns = predicates.len() as f64;
            input * columns * COST_CPU_PRED
        }
        PhysicalProject(compute, input) => {
            let output = input.borrow().props.cardinality as f64;
            let columns = compute.len() as f64;
            output * columns * COST_CPU_EVAL
        }
        PhysicalNestedLoop(join, left, right) => {
            let build = left.borrow().props.cardinality as f64;
            let probe = right.borrow().props.cardinality as f64;
            let iterations = build * probe;
            build * COST_ARRAY_BUILD + iterations * COST_ARRAY_PROBE
        }
        PhysicalHashJoin(join, equals, left, right) => {
            let build = left.borrow().props.cardinality as f64;
            let probe = right.borrow().props.cardinality as f64;
            build * COST_HASH_BUILD + probe * COST_HASH_PROBE
        }
        PhysicalCreateTempTable { .. } => todo!("PhysicalCreateTempTable"),
        PhysicalGetTempTable { .. } => todo!("PhysicalGetTempTable"),
        PhysicalAggregate {
            group_by,
            aggregate,
            input,
        } => {
            let n = input.borrow().props.cardinality as f64;
            let n_group_by = n * group_by.len() as f64;
            let n_aggregate = n * aggregate.len() as f64;
            n_group_by * COST_HASH_BUILD + n_aggregate * COST_CPU_APPLY
        }
        PhysicalLimit { .. } => 0.0,
        PhysicalSort { .. } => {
            let card = group.props.cardinality.max(1) as f64;
            let log = 2.0 * card * f64::log2(card);
            log * COST_CPU_COMP_MOVE
        }
        PhysicalUnion(_, _) | PhysicalIntersect(_, _) | PhysicalExcept(_, _) => 0.0,
        PhysicalValues(_, _) => 0.0,
        PhysicalInsert(_, _, input) | PhysicalUpdate(_, input) | PhysicalDelete(_, input) => {
            let length = input.borrow().props.cardinality as f64;
            let blocks = f64::max(1.0, length * TUPLE_SIZE / BLOCK_SIZE);
            blocks * COST_WRITE_BLOCK
        }
        PhysicalCreateDatabase { .. }
        | PhysicalCreateTable { .. }
        | PhysicalCreateIndex { .. }
        | PhysicalAlterTable { .. }
        | PhysicalDrop { .. }
        | PhysicalRename { .. } => 0.0,
        _ => panic!(),
    }
}

fn init_costs_using_lower_bound(mexpr: &MultiExpr) -> Vec<Cost> {
    let mut costs = Vec::with_capacity(mexpr.op.len());
    for i in 0..mexpr.op.len() {
        let cost = match mexpr.op[i].borrow().winner.as_ref() {
            Some(winner) => winner.cost,
            None => mexpr.op[i].borrow().lower_bound,
        };
        costs.push(cost);
    }
    costs
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

fn stop_early(group: &Group, physical_cost: Cost, input_costs: &Vec<Cost>) -> bool {
    let lower_bound = cost_so_far(physical_cost, input_costs);
    let upper_bound = group.upper_bound;
    lower_bound >= upper_bound
}

fn try_to_declare_winner(group: &mut Group, mexpr: &MultiExpr, physical_cost: Cost) {
    let mut total_cost = physical_cost;
    for i in 0..mexpr.op.len() {
        match mexpr.op[i].borrow().winner.as_ref() {
            Some(winner) => {
                total_cost += winner.cost;
            }
            None => {
                return;
            }
        }
    }
    let current_cost = group.winner.as_ref().map(|w| w.cost).unwrap_or(f64::MAX);
    if total_cost < current_cost {
        todo!()
    }
}

pub fn optimize(expr: Expr) -> (Expr, Cost) {
    let expr = crate::rewrite::rewrite(expr);
    let mut group = Group::new(MultiExpr::new(expr));
    optimize_group(&mut group);
    match group.winner {
        None => panic!("No winner"),
        Some(winner) => (winner.plan, winner.cost),
    }
}
