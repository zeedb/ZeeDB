use crate::rewrite::rewrite;
use encoding::*;
use node::*;
use std::cell::{Cell, RefCell};
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash;
use std::ptr;
use std::rc::Rc;

// Group represents a single logical query, which can be realized by many
// specific logical and physical query plans.
struct Group {
    // logical holds a set of equivalent logical query plans.
    logical: RefCell<HashSet<MultiExpr>>,
    // physical holds a set of physical implementations of the query plans in logical.
    physical: RefCell<HashSet<MultiExpr>>,
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
    upper_bound: Cell<Cost>,
    // winner holds the best physical plan discovered so far.
    winner: RefCell<Option<Winner>>,
    // explored is marked true on the first invocation of optimizeGroup,
    // whose job is to make sure optimizeExpr is called on every group at least once.
    explored: Cell<bool>,
}

// MultiExpr represents a part of a Group.
// Unlike Group, which represents *all* equivalent query plans,
// MultiExpr specifies operator at the top of a the query.
struct MultiExpr {
    // The top operator in this query.
    // Inputs are represented using Group,
    // so they represent a class of equivalent plans rather than a single plan.
    op: Operator<Rc<Group>>,
    // As we try different *logical* transformation rules,
    // we will record the fact that we've already tried this rule on this multi-expression
    // so we can avoid checking it agin. It's safe to mark transformations as complete,
    // because we explore the inputs to each multiExpr before we start
    // applying transformation rules to the group.
    fired: RefCell<HashSet<Rule>>,
}

#[derive(Debug, Clone)]
struct Winner {
    plan: Expr,
    cost: Cost,
}

#[derive(Debug)]
struct LogicalProps {
    // cardinality contains the estimated number of rows in the query.
    cardinality: usize,
    // column_unique_cardinality contains the number of distinct values in each column.
    column_unique_cardinality: HashMap<Column, usize>,
}

impl Group {
    fn new(expr: PartialExpr) -> Self {
        let mexpr = MultiExpr::new(expr);
        let props = compute_logical_props(&mexpr);
        let lower_bound = compute_lower_bound(&props.column_unique_cardinality);
        let mut logical = HashSet::new();
        logical.insert(mexpr);
        Group {
            logical: RefCell::new(logical),
            physical: RefCell::new(HashSet::new()),
            props,
            lower_bound,
            upper_bound: Cell::new(f64::MAX),
            winner: RefCell::new(None),
            explored: Cell::new(false),
        }
    }

    fn contains(&self, mexpr: &MultiExpr) -> bool {
        if mexpr.op.reflect().is_logical() {
            self.logical.borrow().contains(mexpr)
        } else {
            self.physical.borrow().contains(mexpr)
        }
    }

    fn add(&self, mexpr: MultiExpr) {
        if mexpr.op.reflect().is_logical() {
            self.logical.borrow_mut().insert(mexpr);
        } else {
            self.physical.borrow_mut().insert(mexpr);
        }
    }

    // fn intern(&self, mexpr: MultiExpr) -> &MultiExpr {
    //     if mexpr.op.reflect().is_logical() {
    //         self.logical.get_or_insert(mexpr)
    //     } else {
    //         self.physical.get_or_insert(mexpr)
    //     }
    // }
}

impl MultiExpr {
    fn new(expr: PartialExpr) -> Self {
        let mut inputs = Vec::with_capacity(expr.1.len());
        for e in expr.1 {
            match e {
                PartialExprOption::Leaf(group) => inputs.push(group.clone()),
                PartialExprOption::Expr(expr) => {
                    inputs.push(Rc::new(Group::new(expr)));
                }
            }
        }
        MultiExpr {
            op: expr.0,
            inputs,
            fired: RefCell::new(HashSet::new()),
        }
    }
}

impl hash::Hash for MultiExpr {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.op.hash(state);
        for i in &self.inputs {
            ptr::hash(i, state);
        }
    }
}

impl cmp::PartialEq for MultiExpr {
    fn eq(&self, other: &Self) -> bool {
        if self.op != other.op {
            return false;
        }
        if self.inputs.len() != other.inputs.len() {
            return false;
        }
        for i in 0..self.inputs.len() {
            if !Rc::ptr_eq(&self.inputs[i], &other.inputs[i]) {
                return false;
            }
        }
        true
    }
}

impl cmp::Eq for MultiExpr {}

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
        LogicalFilter(predicates) => {
            let scope = &mexpr.inputs[0].props.column_unique_cardinality;
            let selectivity = total_selectivity(predicates, scope);
            cardinality = apply_selectivity(mexpr.inputs[0].props.cardinality, selectivity);
            for (c, n) in &mexpr.inputs[0].props.column_unique_cardinality {
                column_unique_cardinality.insert(c.clone(), apply_selectivity(*n, selectivity));
            }
        }
        LogicalProject(projects) => {
            cardinality = mexpr.inputs[0].props.cardinality;
            for (x, c) in projects {
                let n =
                    scalar_unique_cardinality(&x, &mexpr.inputs[0].props.column_unique_cardinality);
                column_unique_cardinality.insert(c.clone(), n);
            }
        }
        LogicalJoin {
            join, predicates, ..
        } => {
            let mut scope = HashMap::new();
            for (c, n) in &mexpr.inputs[0].props.column_unique_cardinality {
                scope.insert(c.clone(), *n);
            }
            for (c, n) in &mexpr.inputs[1].props.column_unique_cardinality {
                scope.insert(c.clone(), *n);
            }
            let selectivity = total_selectivity(predicates, &scope);
            let product = mexpr.inputs[0].props.cardinality * mexpr.inputs[1].props.cardinality;
            cardinality = apply_selectivity(product, selectivity);
            for (c, n) in &mexpr.inputs[0].props.column_unique_cardinality {
                column_unique_cardinality.insert(c.clone(), apply_selectivity(*n, selectivity));
            }
            for (c, n) in &mexpr.inputs[1].props.column_unique_cardinality {
                column_unique_cardinality.insert(c.clone(), apply_selectivity(*n, selectivity));
            }
            // We want (SemiJoin _ _) to have the same selectivity as (Filter $mark.$in (MarkJoin _ _))
            match join {
                Join::Semi | Join::Anti => {
                    cardinality = apply_selectivity(cardinality, 0.5);
                    for (_, n) in column_unique_cardinality.iter_mut() {
                        *n = apply_selectivity(*n, 0.5);
                    }
                }
                _ => {}
            }
        }
        LogicalWith(name) => todo!(),
        LogicalGetWith(name) => todo!(),
        LogicalAggregate {
            group_by,
            aggregate,
        } => {
            cardinality = 1;
            for c in group_by {
                let n = mexpr.inputs[0].props.column_unique_cardinality[&c];
                column_unique_cardinality.insert(c.clone(), n);
                cardinality *= n;
            }
            for (_, c) in aggregate {
                column_unique_cardinality.insert(c.clone(), cardinality);
            }
        }
        LogicalLimit { limit, .. } => {
            cardinality = *limit;
            for (c, n) in &mexpr.inputs[0].props.column_unique_cardinality {
                if *limit < *n {
                    column_unique_cardinality.insert(c.clone(), *limit);
                } else {
                    column_unique_cardinality.insert(c.clone(), *n);
                }
            }
        }
        LogicalSort(_) => {
            cardinality = mexpr.inputs[0].props.cardinality;
            column_unique_cardinality = mexpr.inputs[0].props.column_unique_cardinality.clone();
        }
        LogicalUnion => {
            let left = &mexpr.inputs[0];
            let right = &mexpr.inputs[1];
            cardinality = left.props.cardinality + right.props.cardinality;
            column_unique_cardinality = max_cuc(
                &left.props.column_unique_cardinality,
                &right.props.column_unique_cardinality,
            );
        }
        LogicalIntersect => todo!(),
        LogicalExcept => {
            cardinality = mexpr.inputs[0].props.cardinality;
            column_unique_cardinality = mexpr.inputs[0].props.column_unique_cardinality.clone();
        }
        LogicalInsert(_, _)
        | LogicalValues(_, _)
        | LogicalUpdate(_)
        | LogicalDelete(_)
        | LogicalCreateDatabase(_)
        | LogicalCreateTable { .. }
        | LogicalCreateIndex { .. }
        | LogicalAlterTable { .. }
        | LogicalDrop { .. }
        | LogicalRename { .. } => {}
        _ => panic!("{}", &mexpr.op),
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
        Scalar::Call(_, _, _) => todo!(),
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
    todo!()
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
    PushImplicitFilterThroughNestedLoop,
    MarkJoinToSemiJoin,
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

    fn pattern(&self) -> Pattern {
        match self {
            Rule::PushImplicitFilterThroughNestedLoop => Pattern(
                OperatorType::LogicalJoin,
                vec![PatternOption::Leaf, PatternOption::Leaf],
            ),
            Rule::MarkJoinToSemiJoin => Pattern(
                OperatorType::LogicalJoin,
                vec![PatternOption::Leaf, PatternOption::Leaf],
            ),
            Rule::LogicalInnerJoinCommutivity => Pattern(
                OperatorType::LogicalJoin,
                vec![PatternOption::Leaf, PatternOption::Leaf],
            ),
            Rule::LogicalInnerJoinAssociativity => Pattern(
                OperatorType::LogicalJoin,
                vec![
                    PatternOption::Pattern(Pattern(
                        OperatorType::LogicalJoin,
                        vec![PatternOption::Leaf, PatternOption::Leaf],
                    )),
                    PatternOption::Leaf,
                ],
            ),
            Rule::LogicalGetToTableFreeScan => Pattern(OperatorType::LogicalSingleGet, vec![]),
            Rule::LogicalGetToSeqScan => Pattern(OperatorType::LogicalGet, vec![]),
            Rule::LogicalGetToIndexScan => Pattern(
                OperatorType::LogicalFilter,
                vec![PatternOption::Pattern(Pattern(
                    OperatorType::LogicalGet,
                    vec![],
                ))],
            ),
            Rule::LogicalFilterToFilter => {
                Pattern(OperatorType::LogicalFilter, vec![PatternOption::Leaf])
            }
            Rule::LogicalProjectToProject => {
                Pattern(OperatorType::LogicalProject, vec![PatternOption::Leaf])
            }
            Rule::LogicalJoinToNestedLoop => Pattern(
                OperatorType::LogicalJoin,
                vec![PatternOption::Leaf, PatternOption::Leaf],
            ),
            Rule::LogicalJoinToHashJoin => Pattern(
                OperatorType::LogicalJoin,
                vec![PatternOption::Leaf, PatternOption::Leaf],
            ),
            Rule::LogicalAggregateToAggregate => {
                Pattern(OperatorType::LogicalAggregate, vec![PatternOption::Leaf])
            }
            Rule::LogicalLimitToLimit => {
                Pattern(OperatorType::LogicalLimit, vec![PatternOption::Leaf])
            }
            Rule::LogicalSortToSort => {
                Pattern(OperatorType::LogicalSort, vec![PatternOption::Leaf])
            }
            Rule::LogicallUnionToUnion => Pattern(
                OperatorType::LogicalUnion,
                vec![PatternOption::Leaf, PatternOption::Leaf],
            ),
            Rule::LogicalIntersectToIntersect => Pattern(
                OperatorType::LogicalIntersect,
                vec![PatternOption::Leaf, PatternOption::Leaf],
            ),
            Rule::LogicalExceptToExcept => Pattern(
                OperatorType::LogicalExcept,
                vec![PatternOption::Leaf, PatternOption::Leaf],
            ),
            Rule::LogicalWithToCreateTempTable => Pattern(
                OperatorType::LogicalWith,
                vec![PatternOption::Leaf, PatternOption::Leaf],
            ),
            Rule::LogicalGetWithToGetTempTable => Pattern(OperatorType::LogicalGetWith, vec![]),
            Rule::LogicalInsertToInsert => {
                Pattern(OperatorType::LogicalInsert, vec![PatternOption::Leaf])
            }
            Rule::LogicalValuesToValues => {
                Pattern(OperatorType::LogicalValues, vec![PatternOption::Leaf])
            }
            Rule::LogicalInsertToInsert => {
                Pattern(OperatorType::LogicalInsert, vec![PatternOption::Leaf])
            }
            Rule::LogicalUpdateToUpdate => {
                Pattern(OperatorType::LogicalUpdate, vec![PatternOption::Leaf])
            }
            Rule::LogicalDeleteToDelete => {
                Pattern(OperatorType::LogicalDelete, vec![PatternOption::Leaf])
            }
            Rule::LogicalCreateDatabaseToCreateDatabase => Pattern(
                OperatorType::LogicalCreateDatabase,
                vec![PatternOption::Leaf],
            ),
            Rule::LogicalCreateTableToCreateTable => {
                Pattern(OperatorType::LogicalCreateTable, vec![PatternOption::Leaf])
            }
            Rule::LogicalCreateIndexToCreateIndex => {
                Pattern(OperatorType::LogicalCreateIndex, vec![PatternOption::Leaf])
            }
            Rule::LogicalAlterTableToAlterTable => {
                Pattern(OperatorType::LogicalAlterTable, vec![PatternOption::Leaf])
            }
            Rule::LogicalDropToDrop => {
                Pattern(OperatorType::LogicalDrop, vec![PatternOption::Leaf])
            }
            Rule::LogicalRenameToRename => {
                Pattern(OperatorType::LogicalRename, vec![PatternOption::Leaf])
            }
        }
    }

    fn promise(&self) -> isize {
        todo!()
    }

    fn substitute(&self, before: PartialExpr) -> Vec<PartialExpr> {
        match self {
            Rule::PushImplicitFilterThroughNestedLoop => {
                if let Extract::Binary(
                    LogicalJoin {
                        join,
                        predicates,
                        mark,
                    },
                    left,
                    right,
                ) = before.extract()
                {
                    return push_implicit_filter_through_nested_loop(
                        join, predicates, mark, left, right,
                    );
                }
            }
            Rule::MarkJoinToSemiJoin => {
                if let Extract::Binary(
                    LogicalJoin {
                        join: Join::Mark,
                        predicates,
                        mark: Some(mark),
                    },
                    left,
                    right,
                ) = before.extract()
                {
                    fn remove(predicates: Vec<Scalar>, remove: Scalar) -> Vec<Scalar> {
                        let mut acc = Vec::with_capacity(predicates.len() - 1);
                        for p in predicates {
                            if p != remove {
                                acc.push(p);
                            }
                        }
                        acc
                    };
                    let semi = Scalar::Column(mark.clone());
                    if predicates.contains(&semi) {
                        return vec![PartialExpr(
                            LogicalJoin {
                                join: Join::Semi,
                                predicates: remove(predicates, semi),
                                mark: Some(mark),
                            },
                            vec![left, right],
                        )];
                    }
                    let anti = Scalar::Call(
                        Function::Not,
                        vec![Scalar::Column(mark.clone())],
                        Type::Bool,
                    );
                    if predicates.contains(&anti) {
                        return vec![PartialExpr(
                            LogicalJoin {
                                join: Join::Semi,
                                predicates: remove(predicates, anti),
                                mark: Some(mark),
                            },
                            vec![left, right],
                        )];
                    }
                    return vec![];
                }
            }
            Rule::LogicalInnerJoinCommutivity => {
                if let Extract::Binary(
                    LogicalJoin {
                        join: Join::Inner,
                        predicates,
                        mark,
                    },
                    left,
                    right,
                ) = before.extract()
                {
                    return vec![PartialExpr(
                        LogicalJoin {
                            join: Join::Inner,
                            predicates,
                            mark,
                        },
                        vec![right, left],
                    )];
                }
            }
            Rule::LogicalInnerJoinAssociativity => todo!(),
            Rule::LogicalGetToTableFreeScan => todo!(),
            Rule::LogicalGetToSeqScan => {
                if let Extract::Leaf(LogicalGet(table)) = before.extract() {
                    return vec![PartialExpr(PhysicalSeqScan(table), vec![])];
                }
            }
            Rule::LogicalGetToIndexScan => {
                if let Extract::Unary(
                    LogicalFilter(predicates),
                    PartialExprOption::Expr(PartialExpr(LogicalGet(table), _)),
                ) = before.extract()
                {
                    return index_scan(predicates, table);
                }
            }
            Rule::LogicalFilterToFilter => {
                if let Extract::Unary(LogicalFilter(predicates), input) = before.extract() {
                    return vec![PartialExpr(PhysicalFilter(predicates), vec![input])];
                }
            }
            Rule::LogicalProjectToProject => todo!(),
            Rule::LogicalJoinToNestedLoop => {
                if let Extract::Binary(
                    LogicalJoin {
                        join,
                        predicates,
                        mark,
                    },
                    left,
                    right,
                ) = before.extract()
                {
                    return vec![PartialExpr(
                        PhysicalNestedLoop {
                            join,
                            predicates,
                            mark,
                        },
                        vec![left, right],
                    )];
                }
            }
            Rule::LogicalJoinToHashJoin => {
                if let Extract::Binary(
                    LogicalJoin {
                        join,
                        predicates,
                        mark,
                    },
                    left,
                    right,
                ) = before.extract()
                {
                    let mut equals = vec![];
                    let mut remaining = vec![];
                    fn unpack(mut arguments: Vec<Scalar>) -> (Scalar, Scalar) {
                        (arguments.remove(0), arguments.remove(0))
                    }
                    fn is_equi_predicate(
                        left: &PartialExprOption,
                        right: &PartialExprOption,
                        equals: &Vec<Scalar>,
                    ) -> bool {
                        todo!()
                    }
                    for p in predicates {
                        match p {
                            Scalar::Call(Function::Equal, arguments, _)
                                if is_equi_predicate(&left, &right, &arguments) =>
                            {
                                equals.push(unpack(arguments));
                            }
                            Scalar::Call(Function::Equal, arguments, _)
                                if is_equi_predicate(&right, &left, &arguments) =>
                            {
                                let (left, right) = unpack(arguments);
                                equals.push((right, left));
                            }
                            p => remaining.push(p),
                        }
                    }
                    if !equals.is_empty() {
                        return vec![PartialExpr(
                            PhysicalHashJoin {
                                join,
                                mark,
                                equals,
                                predicates: remaining,
                            },
                            vec![left, right],
                        )];
                    }
                }
            }
            Rule::LogicalAggregateToAggregate => todo!(),
            Rule::LogicalLimitToLimit => todo!(),
            Rule::LogicalSortToSort => todo!(),
            Rule::LogicallUnionToUnion => todo!(),
            Rule::LogicalIntersectToIntersect => todo!(),
            Rule::LogicalExceptToExcept => todo!(),
            Rule::LogicalWithToCreateTempTable => todo!(),
            Rule::LogicalGetWithToGetTempTable => todo!(),
            Rule::LogicalInsertToInsert => todo!(),
            Rule::LogicalValuesToValues => todo!(),
            Rule::LogicalUpdateToUpdate => todo!(),
            Rule::LogicalDeleteToDelete => todo!(),
            Rule::LogicalCreateDatabaseToCreateDatabase => todo!(),
            Rule::LogicalCreateTableToCreateTable => todo!(),
            Rule::LogicalCreateIndexToCreateIndex => todo!(),
            Rule::LogicalAlterTableToAlterTable => todo!(),
            Rule::LogicalDropToDrop => todo!(),
            Rule::LogicalRenameToRename => todo!(),
        }
        return vec![];
    }

    fn all() -> Vec<Rule> {
        vec![
            Rule::PushImplicitFilterThroughNestedLoop,
            Rule::MarkJoinToSemiJoin,
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
        ]
    }
}

fn push_implicit_filter_through_nested_loop(
    join: Join,
    predicates: Vec<Scalar>,
    mark: Option<Column>,
    left: PartialExprOption,
    right: PartialExprOption,
) -> Vec<PartialExpr> {
    let mut push_predicates = vec![];
    let mut join_predicates = vec![];
    for p in predicates {
        if left.contains_any(&p) && right.contains_any(&p) {
            push_predicates.push(p);
        } else {
            join_predicates.push(p);
        }
    }
    if push_predicates.is_empty() {
        return vec![];
    }
    let left = PartialExprOption::Expr(PartialExpr(LogicalFilter(push_predicates), vec![left]));
    vec![PartialExpr(
        LogicalJoin {
            join,
            predicates: join_predicates,
            mark,
        },
        vec![left, right],
    )]
}

fn index_scan(predicates: Vec<Scalar>, table: Table) -> Vec<PartialExpr> {
    // TODO real implementation
    if let Some((column, equals)) = match_indexed_lookup(&predicates) {
        vec![PartialExpr(
            PhysicalIndexScan {
                table: table,
                equals: vec![(column, equals)],
            },
            vec![],
        )]
    } else {
        vec![]
    }
}

fn match_indexed_lookup(predicates: &Vec<Scalar>) -> Option<(Column, Scalar)> {
    if let [Scalar::Call(Function::Equal, arguments, _)] = predicates.as_slice() {
        match arguments.as_slice() {
            [Scalar::Column(column), equals] if column.name.ends_with("_id") => {
                Some((column.clone(), equals.clone()))
            }
            [equals, Scalar::Column(column)] if column.name.ends_with("_id") => {
                Some((column.clone(), equals.clone()))
            }
            _ => None,
        }
    } else {
        None
    }
}

#[derive(Debug)]
struct Pattern(OperatorType, Vec<PatternOption>);

#[derive(Debug)]
enum PatternOption {
    Pattern(Pattern),
    Leaf,
}

#[derive(Clone)]
struct PartialExpr(Operator, Vec<PartialExprOption>);

#[derive(Clone)]
enum PartialExprOption {
    Expr(PartialExpr),
    Leaf(Rc<Group>),
}

impl Pattern {
    fn bind(&self, mexpr: &MultiExpr) -> Vec<PartialExpr> {
        match self {
            Pattern(op, inputs) => {
                if *op != mexpr.op.reflect() || inputs.len() != mexpr.inputs.len() {
                    return vec![];
                }
                let mut binds = vec![];
                match inputs.as_slice() {
                    [] => binds.push(PartialExpr(mexpr.op.clone(), vec![])),
                    [input] => {
                        for input in input.bind(mexpr.inputs[0].clone()) {
                            binds.push(PartialExpr(mexpr.op.clone(), vec![input]));
                        }
                    }
                    [left, right] => {
                        for left in left.bind(mexpr.inputs[0].clone()) {
                            for right in right.bind(mexpr.inputs[1].clone()) {
                                binds.push(PartialExpr(
                                    mexpr.op.clone(),
                                    vec![left.clone(), right.clone()],
                                ));
                            }
                        }
                    }
                    _ => panic!("{} inputs", inputs.len()),
                }
                binds
            }
            _ => vec![],
        }
    }
}

impl PatternOption {
    fn bind(&self, group: Rc<Group>) -> Vec<PartialExprOption> {
        match self {
            // Leaf binds the entire group:
            PatternOption::Leaf => vec![PartialExprOption::Leaf(group)],
            // Expr binds each logical member of the group:
            PatternOption::Pattern(pattern) => group
                .logical
                .borrow()
                .iter()
                .flat_map(|mexpr| pattern.bind(mexpr))
                .map(|pexpr| PartialExprOption::Expr(pexpr))
                .collect(),
        }
    }

    fn has_inputs(&self) -> bool {
        match self {
            PatternOption::Pattern(pattern) => !pattern.1.is_empty(),
            PatternOption::Leaf => false,
        }
    }
}

impl PartialExpr {
    fn new(expr: Expr) -> Self {
        let mut inputs = Vec::with_capacity(expr.1.len());
        for input in expr.1 {
            inputs.push(PartialExprOption::Expr(PartialExpr::new(input)));
        }
        PartialExpr(expr.0, inputs)
    }

    pub fn extract(self) -> Extract<PartialExprOption> {
        Extract::new(self.0, self.1)
    }
}

impl PartialExprOption {
    fn contains_any(&self, expr: &Scalar) -> bool {
        todo!()
    }

    fn contains_all(&self, expr: &Scalar) -> bool {
        todo!()
    }
}

// #[test]
// fn test_bind() {
//     let pattern = Pattern(
//         OperatorType::LogicalSingleGet,
//         vec![
//             Pattern(OperatorType::LogicalGetWith, vec![]),
//             Pattern(OperatorType::LogicalGetWith, vec![]),
//         ],
//     );
//     let a = MultiExpr::new(Operator::LogicalGetWith("a".to_string()), vec![]);
//     let b = MultiExpr::new(Operator::LogicalGetWith("b".to_string()), vec![]);
//     let c = MultiExpr::new(Operator::LogicalGetWith("c".to_string()), vec![]);
//     let d = MultiExpr::new(Operator::LogicalGetWith("d".to_string()), vec![]);
//     let mut left = Group {
//         logical: HashSet::new(),
//         physical: HashSet::new(),
//         props: LogicalProps {
//             cardinality: 0,
//             column_unique_cardinality: HashMap::new(),
//         },
//         lower_bound: 0.0,
//         upper_bound: Cell::new(f64::MAX),
//         winner: None,
//         explored: Cell::new(false),
//     };
//     let mut right = Group {
//         logical: HashSet::new(),
//         physical: HashSet::new(),
//         props: LogicalProps {
//             cardinality: 0,
//             column_unique_cardinality: HashMap::new(),
//         },
//         lower_bound: 0.0,
//         upper_bound: Cell::new(f64::MAX),
//         winner: None,
//         explored: Cell::new(false),
//     };
//     left.logical.insert(a);
//     left.logical.insert(b);
//     right.logical.insert(c);
//     right.logical.insert(d);
//     let mexpr = MultiExpr::new(
//         Operator::LogicalSingleGet,
//         vec![Rc::new(left), Rc::new(right)],
//     );
//     let mut group = Group {
//         logical: HashSet::new(),
//         physical: HashSet::new(),
//         props: LogicalProps {
//             cardinality: 0,
//             column_unique_cardinality: HashMap::new(),
//         },
//         lower_bound: 0.0,
//         upper_bound: Cell::new(f64::MAX),
//         winner: None,
//         explored: Cell::new(false),
//     };
//     group.logical.insert(mexpr);
//     for mexpr in &group.logical {
//         let choices = pattern.bind(&mexpr);
//         dbg!(choices);
//     }
// }

// Our implementation of tasks differs from Columbia/Cascades:
// we use ordinary functions and recursion rather than task objects and a stack of pending tasks.
// However, the logic and the order of invocation should be exactly the same.

fn optimize_group(group: &Group) {
    if group.lower_bound >= group.upper_bound.get() || group.winner.borrow().is_some() {
        return;
    }
    for e in group.physical.borrow().iter() {
        optimize_inputs(group, e);
    }
    for e in group.logical.borrow().iter() {
        optimize_expr(group, e, false);
    }
}

fn optimize_expr(group: &Group, mexpr: &MultiExpr, explore: bool) {
    for rule in Rule::all() {
        // Have we already applied this rule to this multi-expression?
        if mexpr.fired.borrow().contains(&rule) {
            continue;
        }
        // If we are exploring, rather than optimizing, skip physical expressions:
        if explore && rule.output_is_physical() {
            continue;
        }
        // Does the pattern match the multi-expression?
        let pattern = rule.pattern();
        if pattern.0 == mexpr.op.reflect() && pattern.1.len() == mexpr.inputs.len() {
            // Explore inputs recursively:
            for i in 0..pattern.1.len() {
                if pattern.1[i].has_inputs() {
                    explore_group(&mexpr.inputs[i]);
                }
            }
            // Apply the rule, potentially adding another MultiExpr to the Group:
            apply_rule(&rule, group, mexpr, explore);
            mexpr.fired.borrow_mut().insert(rule);
        }
    }
}

fn apply_rule(rule: &Rule, group: &Group, mexpr: &MultiExpr, explore: bool) {
    for before in rule.pattern().bind(mexpr) {
        for after in rule.substitute(before) {
            let mexpr = MultiExpr::new(after);
            // Do we already know about this substitution?
            if group.contains(&mexpr) {
                continue;
            }
            if mexpr.op.reflect().is_physical() {
                // If rule produced a physical implementation, cost the implementation:
                optimize_inputs(group, &mexpr);
            } else {
                // If rule produced a new new logical expression, optimize it:
                optimize_expr(group, &mexpr, explore)
            }
            group.add(mexpr);
        }
    }
}

fn explore_group(group: &Group) {
    if !group.explored.get() {
        for mexpr in group.logical.borrow().iter() {
            optimize_expr(group, mexpr, true)
        }
        group.explored.set(true);
    }
}

fn optimize_inputs(group: &Group, mexpr: &MultiExpr) {
    // Initially, physicalCost is the actual physical cost of the operator at the head of the multi-expression,
    // and inputCosts are the total physical cost of the winning strategy for each input group.
    // If we don't yet have a winner for an inputGroup, we use the lower bound.
    // Thus, physicalCost + sum(inputCosts) = a lower-bound for the total cost of the best strategy for this group.
    let physical_cost = physical_cost(group, mexpr);
    let mut input_costs = init_costs_using_lower_bound(mexpr);
    for i in 0..mexpr.inputs.len() {
        // If we can prove the cost of this MultiExpr exceeds the upper_bound of the Group, give up:
        if stop_early(group, physical_cost, &input_costs) {
            return;
        }
        // Propagate the cost upper_bound downwards to the input group,
        // using the best available estimate of the cost of the other inputs:
        let total_cost = cost_so_far(physical_cost, &input_costs);
        let input_upper_bound = group.upper_bound.get() - (total_cost - input_costs[i]);
        mexpr.inputs[i].upper_bound.set(input_upper_bound);
        // Optimize input group:
        optimize_group(&mexpr.inputs[i]);
        // If we failed to declare a winner, give up:
        if mexpr.inputs[i].winner.borrow().is_none() {
            return;
        }
        input_costs[i] = mexpr.inputs[i].winner.borrow().as_ref().unwrap().cost
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
        PhysicalFilter(predicates) => {
            let input = mexpr.inputs[0].props.cardinality as f64;
            let columns = predicates.len() as f64;
            input * columns * COST_CPU_PRED
        }
        PhysicalProject(compute) => {
            let output = mexpr.inputs[0].props.cardinality as f64;
            let columns = compute.len() as f64;
            output * columns * COST_CPU_EVAL
        }
        PhysicalNestedLoop { predicates, .. } => {
            let build = mexpr.inputs[0].props.cardinality as f64;
            let probe = mexpr.inputs[1].props.cardinality as f64;
            let iterations = build * probe;
            let filter = iterations * predicates.len() as f64;
            build * COST_ARRAY_BUILD + iterations * COST_ARRAY_PROBE + filter * COST_CPU_PRED
        }
        PhysicalHashJoin { predicates, .. } => {
            let build = mexpr.inputs[0].props.cardinality as f64;
            let probe = mexpr.inputs[1].props.cardinality as f64;
            let filter = build.max(probe) * predicates.len() as f64;
            build * COST_HASH_BUILD + probe * COST_HASH_PROBE + filter * COST_CPU_PRED
        }
        PhysicalCreateTempTable { .. } => todo!(),
        PhysicalGetTempTable { .. } => todo!(),
        PhysicalAggregate {
            group_by,
            aggregate,
        } => {
            let n = mexpr.inputs[0].props.cardinality as f64;
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
        PhysicalUnion { .. } | PhysicalIntersect { .. } | PhysicalExcept { .. } => 0.0,
        PhysicalValues { .. } => 0.0,
        PhysicalInsert { .. } | PhysicalUpdate { .. } | PhysicalDelete { .. } => {
            let length = mexpr.inputs[0].props.cardinality as f64;
            let blocks = f64::max(1.0, length * TUPLE_SIZE / BLOCK_SIZE);
            blocks * COST_WRITE_BLOCK
        }
        PhysicalCreateDatabase { .. }
        | PhysicalCreateTable { .. }
        | PhysicalCreateIndex { .. }
        | PhysicalAlterTable { .. }
        | PhysicalDrop { .. }
        | PhysicalRename { .. } => 0.0,
        _ => panic!("{}", mexpr.op),
    }
}

fn init_costs_using_lower_bound(mexpr: &MultiExpr) -> Vec<Cost> {
    mexpr
        .inputs
        .iter()
        .map(|group| match group.winner.borrow().as_ref() {
            Some(winner) => winner.cost,
            None => group.lower_bound,
        })
        .collect()
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
    let upper_bound = group.upper_bound.get();
    lower_bound >= upper_bound
}

fn try_to_declare_winner(group: &Group, mexpr: &MultiExpr, physical_cost: Cost) {
    let mut total_cost = physical_cost;
    let mut inputs = Vec::with_capacity(mexpr.inputs.len());
    for input in &mexpr.inputs {
        match input.winner.borrow().as_ref() {
            Some(winner) => {
                total_cost += winner.cost;
                inputs.push(winner.plan.clone());
            }
            None => {
                return;
            }
        }
    }
    if group.winner.borrow().is_none() || total_cost < group.winner.borrow().as_ref().unwrap().cost
    {
        group.winner.replace(Some(Winner {
            plan: Expr(mexpr.op.clone(), inputs),
            cost: total_cost,
        }));
    }
}

pub fn optimize(expr: &Expr) -> (Expr, Cost) {
    let expr = rewrite(expr);
    let group = Group::new(PartialExpr::new(expr));
    optimize_group(&group);
    if group.winner.borrow().is_none() {
        panic!("No winner")
    }
    let winner = group.winner.borrow().as_ref().unwrap().clone();
    (winner.plan.clone(), winner.cost)
}
