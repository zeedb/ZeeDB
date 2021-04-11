use crate::{column::Column, index::Index, values::*};
use chrono::Weekday;
use kernel::*;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    fmt,
    hash::Hash,
};

// Expr plan nodes combine inputs in a Plan tree.
#[derive(Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Expr {
    Leaf {
        gid: usize,
    },
    // LogicalSingleGet acts as the input to a SELECT with no FROM clause.
    LogicalSingleGet,
    // LogicalGet { table } implements the FROM clause.
    LogicalGet {
        projects: Vec<Column>,
        predicates: Vec<Scalar>,
        table: Table,
    },
    // LogicalFilter { predicates } implements the WHERE/HAVING clauses.
    LogicalFilter {
        predicates: Vec<Scalar>,
        input: Box<Expr>,
    },
    LogicalOut {
        projects: Vec<Column>,
        input: Box<Expr>,
    },
    // LogicalMap { columns } implements the SELECT clause.
    LogicalMap {
        include_existing: bool,
        projects: Vec<(Scalar, Column)>,
        input: Box<Expr>,
    },
    // LogicalJoin implements the JOIN operator.
    LogicalJoin {
        join: Join,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    // LogicalJoin implements a special inner join where the right side (domain) is deduplicated,
    // and the left side (subquery) is allowed to depend on the right side.
    LogicalDependentJoin {
        parameters: Vec<Column>,
        predicates: Vec<Scalar>,
        subquery: Box<Expr>,
        domain: Box<Expr>,
    },
    // LogicalWith { table } implements with subquery as  _.
    // The with-subquery is always on the left.
    LogicalWith {
        name: String,
        columns: Vec<Column>,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    LogicalCreateTempTable {
        name: String,
        columns: Vec<Column>,
        input: Box<Expr>,
    },
    // LogicalGetWith { table } reads the subquery that was created by With.
    LogicalGetWith {
        name: String,
        columns: Vec<Column>,
    },
    // LogicalAggregate { group_by, aggregate } implements the GROUP BY clause.
    LogicalAggregate {
        group_by: Vec<Column>,
        aggregate: Vec<AggregateExpr>,
        input: Box<Expr>,
    },
    // LogicalLimit { n } implements the LIMIT / OFFSET / TOP clause.
    LogicalLimit {
        limit: usize,
        offset: usize,
        input: Box<Expr>,
    },
    // LogicalSort { columns } implements the ORDER BY clause.
    LogicalSort {
        order_by: Vec<OrderBy>,
        input: Box<Expr>,
    },
    // LogicalUnion implements SELECT _ UNION ALL SELECT _.
    LogicalUnion {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    // LogicalInsert { table, columns } implements the INSERT operation.
    LogicalInsert {
        table: Table,
        input: Box<Expr>,
        /// [(query_output_column, table_column), ..]
        columns: Vec<(Column, String)>,
    },
    // LogicalValues { columns, values } implements VALUES expressions.
    LogicalValues {
        columns: Vec<Column>,
        values: Vec<Vec<Scalar>>,
        input: Box<Expr>,
    },
    // LogicalUpdate { tid } implements the UPDATE operation.
    LogicalUpdate {
        table: Table,
        tid: Column,
        input: Box<Expr>,
        /// [(query_output_column, table_column), ..]
        columns: Vec<(Column, String)>,
    },
    // LogicalDelete { table } implements the DELETE operation.
    LogicalDelete {
        table: Table,
        tid: Column,
        input: Box<Expr>,
    },
    // LogicalCreateDatabase { database } implements the CREATE DATABASE operation.
    LogicalCreateDatabase {
        name: Name,
    },
    // LogicalCreateTable implements the CREATE TABLE operation.
    LogicalCreateTable {
        name: Name,
        columns: Vec<(String, DataType)>,
    },
    // LogicalCreateIndex implements the CREATE INDEX operation.
    LogicalCreateIndex {
        name: Name,
        table: Name,
        columns: Vec<String>,
    },
    // LogicalDrop implements the DROP DATABASE/TABLE/INDEX operation.
    LogicalDrop {
        object: ObjectType,
        name: Name,
    },
    LogicalScript {
        statements: Vec<Expr>,
    },
    LogicalAssign {
        variable: String,
        value: Scalar,
        input: Box<Expr>,
    },
    LogicalCall {
        procedure: Procedure,
        input: Box<Expr>,
    },
    LogicalExplain {
        input: Box<Expr>,
    },
    LogicalRewrite {
        sql: String,
    },
    TableFreeScan,
    SeqScan {
        projects: Vec<Column>,
        predicates: Vec<Scalar>,
        table: Table,
    },
    IndexScan {
        include_existing: bool,
        projects: Vec<Column>,
        predicates: Vec<Scalar>,
        lookup: Vec<Scalar>,
        index: Index,
        table: Table,
        input: Box<Expr>,
    },
    Filter {
        predicates: Vec<Scalar>,
        input: Box<Expr>,
    },
    Out {
        projects: Vec<Column>,
        input: Box<Expr>,
    },
    Map {
        include_existing: bool,
        projects: Vec<(Scalar, Column)>,
        input: Box<Expr>,
    },
    NestedLoop {
        join: Join,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    HashJoin {
        broadcast: bool,
        join: Join,
        partition_left: Column,
        partition_right: Column,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    CreateTempTable {
        name: String,
        columns: Vec<Column>,
        input: Box<Expr>,
    },
    GetTempTable {
        name: String,
        columns: Vec<Column>,
    },
    Aggregate {
        partition_by: Option<Column>,
        group_by: Vec<Column>,
        aggregate: Vec<AggregateExpr>,
        input: Box<Expr>,
    },
    Limit {
        limit: usize,
        offset: usize,
        input: Box<Expr>,
    },
    Sort {
        order_by: Vec<OrderBy>,
        input: Box<Expr>,
    },
    Union {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// Broadcast broadcasts the build side of a join to every node.
    Broadcast {
        input: Box<Expr>,
    },
    /// Exchange shuffles data during joins and aggregations.
    Exchange {
        /// hash_column is initially unset during the optimization phase, to reduce the size of the search space, then added before compilation.
        hash_column: Option<Column>,
        input: Box<Expr>,
    },
    Insert {
        table: Table,
        indexes: Vec<Index>,
        input: Box<Expr>,
        /// [(query_output_column, table_column), ..]
        columns: Vec<(Column, String)>,
    },
    Values {
        columns: Vec<Column>,
        values: Vec<Vec<Scalar>>,
        input: Box<Expr>,
    },
    Delete {
        table: Table,
        tid: Column,
        input: Box<Expr>,
    },
    Script {
        statements: Vec<Expr>,
    },
    Assign {
        variable: String,
        value: Scalar,
        input: Box<Expr>,
    },
    Call {
        procedure: Procedure,
        input: Box<Expr>,
    },
    Explain {
        input: Box<Expr>,
    },
}

impl Expr {
    pub fn is_logical(&self) -> bool {
        match self {
            Expr::LogicalJoin { .. }
            | Expr::LogicalDependentJoin { .. }
            | Expr::LogicalWith { .. }
            | Expr::LogicalCreateTempTable { .. }
            | Expr::LogicalUnion { .. }
            | Expr::LogicalFilter { .. }
            | Expr::LogicalOut { .. }
            | Expr::LogicalMap { .. }
            | Expr::LogicalAggregate { .. }
            | Expr::LogicalLimit { .. }
            | Expr::LogicalSort { .. }
            | Expr::LogicalInsert { .. }
            | Expr::LogicalValues { .. }
            | Expr::LogicalUpdate { .. }
            | Expr::LogicalDelete { .. }
            | Expr::LogicalSingleGet
            | Expr::LogicalGet { .. }
            | Expr::LogicalGetWith { .. }
            | Expr::LogicalCreateDatabase { .. }
            | Expr::LogicalCreateTable { .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalDrop { .. }
            | Expr::LogicalRewrite { .. }
            | Expr::LogicalScript { .. }
            | Expr::LogicalAssign { .. }
            | Expr::LogicalCall { .. }
            | Expr::LogicalExplain { .. } => true,
            Expr::Leaf { .. }
            | Expr::TableFreeScan { .. }
            | Expr::SeqScan { .. }
            | Expr::IndexScan { .. }
            | Expr::Filter { .. }
            | Expr::Out { .. }
            | Expr::Map { .. }
            | Expr::NestedLoop { .. }
            | Expr::HashJoin { .. }
            | Expr::CreateTempTable { .. }
            | Expr::GetTempTable { .. }
            | Expr::Aggregate { .. }
            | Expr::Limit { .. }
            | Expr::Sort { .. }
            | Expr::Union { .. }
            | Expr::Broadcast { .. }
            | Expr::Exchange { .. }
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Delete { .. }
            | Expr::Script { .. }
            | Expr::Assign { .. }
            | Expr::Call { .. }
            | Expr::Explain { .. } => false,
        }
    }

    pub fn is_leaf(&self) -> bool {
        match self {
            Expr::Leaf { .. } => true,
            _ => false,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Expr::LogicalJoin { .. }
            | Expr::LogicalDependentJoin { .. }
            | Expr::LogicalWith { .. }
            | Expr::LogicalUnion { .. }
            | Expr::NestedLoop { .. }
            | Expr::HashJoin { .. }
            | Expr::Union { .. } => 2,
            Expr::LogicalFilter { .. }
            | Expr::LogicalOut { .. }
            | Expr::LogicalMap { .. }
            | Expr::LogicalAggregate { .. }
            | Expr::LogicalLimit { .. }
            | Expr::LogicalSort { .. }
            | Expr::LogicalInsert { .. }
            | Expr::LogicalValues { .. }
            | Expr::LogicalUpdate { .. }
            | Expr::LogicalDelete { .. }
            | Expr::Filter { .. }
            | Expr::Out { .. }
            | Expr::Map { .. }
            | Expr::IndexScan { .. }
            | Expr::Aggregate { .. }
            | Expr::Limit { .. }
            | Expr::Sort { .. }
            | Expr::Broadcast { .. }
            | Expr::Exchange { .. }
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Delete { .. }
            | Expr::LogicalCreateTempTable { .. }
            | Expr::CreateTempTable { .. }
            | Expr::LogicalAssign { .. }
            | Expr::Assign { .. }
            | Expr::LogicalCall { .. }
            | Expr::Call { .. }
            | Expr::LogicalExplain { .. }
            | Expr::Explain { .. } => 1,
            Expr::Leaf { .. }
            | Expr::LogicalSingleGet
            | Expr::LogicalGet { .. }
            | Expr::LogicalGetWith { .. }
            | Expr::LogicalCreateDatabase { .. }
            | Expr::LogicalCreateTable { .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalDrop { .. }
            | Expr::TableFreeScan
            | Expr::SeqScan { .. }
            | Expr::GetTempTable { .. }
            | Expr::LogicalRewrite { .. } => 0,
            Expr::LogicalScript { statements } | Expr::Script { statements } => statements.len(),
        }
    }

    pub fn pre_order(&self) -> PreOrderTraversal {
        PreOrderTraversal {
            deferred: vec![self],
        }
    }

    pub fn attributes(&self) -> HashSet<Column> {
        match self {
            Expr::LogicalGet { projects, .. } | Expr::LogicalOut { projects, .. } => {
                projects.iter().map(|c| c.clone()).collect()
            }
            Expr::LogicalMap {
                include_existing,
                projects,
                input,
            } => {
                let mut set = if *include_existing {
                    input.attributes()
                } else {
                    HashSet::new()
                };
                for (_, c) in projects {
                    set.insert(c.clone());
                }
                set
            }
            Expr::LogicalFilter { input, .. }
            | Expr::LogicalWith { right: input, .. }
            | Expr::LogicalLimit { input, .. }
            | Expr::LogicalSort { input, .. }
            | Expr::LogicalUnion { right: input, .. }
            | Expr::LogicalJoin {
                join: Join::Semi { .. },
                right: input,
                ..
            }
            | Expr::LogicalJoin {
                join: Join::Anti { .. },
                right: input,
                ..
            }
            | Expr::LogicalJoin {
                join: Join::Single { .. },
                right: input,
                ..
            }
            | Expr::LogicalAssign { input, .. }
            | Expr::LogicalCall { input, .. }
            | Expr::LogicalExplain { input, .. } => input.attributes(),
            Expr::LogicalJoin {
                join: Join::Mark(mark, _),
                right,
                ..
            } => {
                let mut set = right.attributes();
                set.insert(mark.clone());
                set
            }
            Expr::LogicalJoin {
                join: Join::Inner { .. },
                left,
                right,
                ..
            }
            | Expr::LogicalJoin {
                join: Join::Right { .. },
                left,
                right,
                ..
            }
            | Expr::LogicalJoin {
                join: Join::Outer { .. },
                left,
                right,
                ..
            }
            | Expr::LogicalDependentJoin {
                subquery: left,
                domain: right,
                ..
            } => {
                let mut set = left.attributes();
                set.extend(right.attributes());
                set
            }
            Expr::LogicalGetWith { columns, .. } | Expr::LogicalValues { columns, .. } => {
                columns.iter().map(|c| c.clone()).collect()
            }
            Expr::LogicalAggregate {
                group_by,
                aggregate,
                ..
            } => {
                let mut set = HashSet::new();
                set.extend(group_by.clone());
                for AggregateExpr { output, .. } in aggregate {
                    set.insert(output.clone());
                }
                set
            }
            Expr::Leaf { .. }
            | Expr::LogicalSingleGet { .. }
            | Expr::LogicalInsert { .. }
            | Expr::LogicalUpdate { .. }
            | Expr::LogicalDelete { .. }
            | Expr::LogicalCreateDatabase { .. }
            | Expr::LogicalCreateTempTable { .. }
            | Expr::LogicalCreateTable { .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalDrop { .. }
            | Expr::LogicalScript { .. }
            | Expr::LogicalRewrite { .. } => HashSet::new(),
            Expr::TableFreeScan { .. }
            | Expr::SeqScan { .. }
            | Expr::IndexScan { .. }
            | Expr::Filter { .. }
            | Expr::Out { .. }
            | Expr::Map { .. }
            | Expr::NestedLoop { .. }
            | Expr::HashJoin { .. }
            | Expr::CreateTempTable { .. }
            | Expr::GetTempTable { .. }
            | Expr::Aggregate { .. }
            | Expr::Limit { .. }
            | Expr::Sort { .. }
            | Expr::Union { .. }
            | Expr::Broadcast { .. }
            | Expr::Exchange { .. }
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Delete { .. }
            | Expr::Script { .. }
            | Expr::Assign { .. }
            | Expr::Call { .. }
            | Expr::Explain { .. } => panic!(
                "attributes is not implemented for physical operator {}",
                &self.name()
            ),
        }
    }

    pub fn references(&self) -> HashSet<Column> {
        let mut set = HashSet::new();
        self.collect_references(&mut set);
        set
    }

    fn collect_references(&self, set: &mut HashSet<Column>) {
        match self {
            Expr::LogicalGet { predicates, .. } | Expr::LogicalFilter { predicates, .. } => {
                for p in predicates {
                    p.collect_references(set);
                }
            }
            Expr::LogicalMap { projects, .. } => {
                for (x, _) in projects {
                    x.collect_references(set);
                }
            }
            Expr::LogicalJoin { join, .. } => {
                for p in join.predicates() {
                    p.collect_references(set);
                }
            }
            Expr::LogicalDependentJoin { predicates, .. } => {
                for p in predicates {
                    p.collect_references(set);
                }
            }
            Expr::LogicalWith { columns, .. } | Expr::LogicalGetWith { columns, .. } => {
                set.extend(columns.clone());
            }
            Expr::LogicalAggregate {
                group_by,
                aggregate,
                ..
            } => {
                set.extend(group_by.clone());
                for AggregateExpr { input, .. } in aggregate {
                    set.insert(input.clone());
                }
            }
            Expr::LogicalSort { order_by, .. } => {
                for o in order_by {
                    set.insert(o.column.clone());
                }
            }
            Expr::LogicalValues {
                columns, values, ..
            } => {
                set.extend(columns.clone());
                for array in values {
                    for x in array {
                        x.collect_references(set);
                    }
                }
            }
            Expr::LogicalAssign { value, .. } => {
                value.collect_references(set);
            }
            Expr::LogicalCall { procedure, .. } => procedure.collect_references(set),
            Expr::LogicalSingleGet { .. }
            | Expr::Leaf { .. }
            | Expr::LogicalOut { .. }
            | Expr::LogicalLimit { .. }
            | Expr::LogicalUnion { .. }
            | Expr::LogicalInsert { .. }
            | Expr::LogicalUpdate { .. }
            | Expr::LogicalDelete { .. }
            | Expr::LogicalCreateDatabase { .. }
            | Expr::LogicalCreateTempTable { .. }
            | Expr::LogicalCreateTable { .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalDrop { .. }
            | Expr::LogicalScript { .. }
            | Expr::LogicalRewrite { .. }
            | Expr::LogicalExplain { .. } => {}
            Expr::TableFreeScan { .. }
            | Expr::SeqScan { .. }
            | Expr::IndexScan { .. }
            | Expr::Filter { .. }
            | Expr::Out { .. }
            | Expr::Map { .. }
            | Expr::NestedLoop { .. }
            | Expr::HashJoin { .. }
            | Expr::CreateTempTable { .. }
            | Expr::GetTempTable { .. }
            | Expr::Aggregate { .. }
            | Expr::Limit { .. }
            | Expr::Sort { .. }
            | Expr::Union { .. }
            | Expr::Broadcast { .. }
            | Expr::Exchange { .. }
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Delete { .. }
            | Expr::Script { .. }
            | Expr::Assign { .. }
            | Expr::Call { .. }
            | Expr::Explain { .. } => unimplemented!(
                "references is not implemented for physical operator {}",
                self.name()
            ),
        }
        for i in 0..self.len() {
            self[i].collect_references(set)
        }
    }

    pub fn subst(&mut self, map: &HashMap<Column, Column>) {
        let subst_c = |c: &Column| map.get(c).unwrap_or(c).clone();
        let subst_x = |x: &Scalar| x.clone().subst(map);
        let subst_o = |o: &OrderBy| OrderBy {
            column: map.get(&o.column).unwrap_or(&o.column).clone(),
            descending: o.descending,
        };
        match self {
            Expr::LogicalGet {
                projects,
                predicates,
                ..
            } => {
                *projects = projects.iter().map(subst_c).collect();
                *predicates = predicates.iter().map(subst_x).collect();
            }
            Expr::LogicalFilter { predicates, .. } => {
                *predicates = predicates.iter().map(subst_x).collect();
            }
            Expr::LogicalMap { projects, .. } => {
                *projects = projects
                    .iter()
                    .map(|(x, c)| (subst_x(x), subst_c(c)))
                    .collect();
            }
            Expr::LogicalJoin { join, .. } => {
                *join = join.replace(join.predicates().iter().map(subst_x).collect());
            }
            Expr::LogicalDependentJoin {
                parameters,
                predicates,
                ..
            } => {
                *parameters = parameters.iter().map(subst_c).collect();
                *predicates = predicates.iter().map(subst_x).collect();
            }
            Expr::LogicalWith { columns, .. }
            | Expr::LogicalCreateTempTable { columns, .. }
            | Expr::LogicalGetWith { columns, .. } => {
                *columns = columns.iter().map(subst_c).collect();
            }
            Expr::LogicalAggregate {
                group_by,
                aggregate,
                ..
            } => {
                *group_by = group_by.iter().map(subst_c).collect();
                *aggregate = aggregate
                    .iter()
                    .map(
                        |AggregateExpr {
                             function,
                             distinct,
                             input,
                             output,
                         }| AggregateExpr {
                            function: *function,
                            distinct: *distinct,
                            input: subst_c(input),
                            output: subst_c(output),
                        },
                    )
                    .collect();
            }
            Expr::LogicalSort { order_by, .. } => {
                *order_by = order_by.iter().map(subst_o).collect()
            }
            Expr::LogicalValues {
                columns, values, ..
            } => {
                *columns = columns.iter().map(subst_c).collect();
                *values = values
                    .iter()
                    .map(|row| row.iter().map(subst_x).collect())
                    .collect();
            }
            Expr::LogicalOut { projects, .. } => {
                *projects = projects.iter().map(subst_c).collect();
            }
            Expr::LogicalUpdate { tid, .. } | Expr::LogicalDelete { tid, .. } => {
                *tid = subst_c(&tid);
            }
            Expr::Leaf { .. }
            | Expr::LogicalSingleGet
            | Expr::LogicalLimit { .. }
            | Expr::LogicalUnion { .. }
            | Expr::LogicalInsert { .. }
            | Expr::LogicalCreateDatabase { .. }
            | Expr::LogicalCreateTable { .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalDrop { .. }
            | Expr::LogicalScript { .. }
            | Expr::LogicalAssign { .. }
            | Expr::LogicalCall { .. }
            | Expr::LogicalExplain { .. }
            | Expr::LogicalRewrite { .. } => {}
            Expr::TableFreeScan { .. }
            | Expr::SeqScan { .. }
            | Expr::IndexScan { .. }
            | Expr::Filter { .. }
            | Expr::Out { .. }
            | Expr::Map { .. }
            | Expr::NestedLoop { .. }
            | Expr::HashJoin { .. }
            | Expr::CreateTempTable { .. }
            | Expr::GetTempTable { .. }
            | Expr::Aggregate { .. }
            | Expr::Limit { .. }
            | Expr::Sort { .. }
            | Expr::Union { .. }
            | Expr::Broadcast { .. }
            | Expr::Exchange { .. }
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Delete { .. }
            | Expr::Script { .. }
            | Expr::Assign { .. }
            | Expr::Call { .. }
            | Expr::Explain { .. } => panic!(
                "subst is not implemented for physical operator {}",
                self.name()
            ),
        }
        for i in 0..self.len() {
            self[i].subst(map)
        }
    }
}

impl std::ops::Index<usize> for Expr {
    type Output = Expr;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            Expr::LogicalJoin { left, right, .. }
            | Expr::LogicalDependentJoin {
                subquery: left,
                domain: right,
                ..
            }
            | Expr::LogicalWith { left, right, .. }
            | Expr::LogicalUnion { left, right, .. }
            | Expr::NestedLoop { left, right, .. }
            | Expr::HashJoin { left, right, .. }
            | Expr::Union { left, right } => match index {
                0 => left,
                1 => right,
                _ => panic!("{} is out of bounds [0,2)", index),
            },
            Expr::LogicalFilter { input, .. }
            | Expr::LogicalOut { input, .. }
            | Expr::LogicalMap { input, .. }
            | Expr::LogicalAggregate { input, .. }
            | Expr::LogicalLimit { input, .. }
            | Expr::LogicalSort { input, .. }
            | Expr::LogicalInsert { input, .. }
            | Expr::LogicalValues { input, .. }
            | Expr::LogicalUpdate { input, .. }
            | Expr::LogicalDelete { input, .. }
            | Expr::Filter { input, .. }
            | Expr::Out { input, .. }
            | Expr::Map { input, .. }
            | Expr::IndexScan { input, .. }
            | Expr::Aggregate { input, .. }
            | Expr::Limit { input, .. }
            | Expr::Sort { input, .. }
            | Expr::Broadcast { input, .. }
            | Expr::Exchange { input, .. }
            | Expr::Insert { input, .. }
            | Expr::Values { input, .. }
            | Expr::Delete { input, .. }
            | Expr::LogicalCreateTempTable { input, .. }
            | Expr::CreateTempTable { input, .. }
            | Expr::LogicalAssign { input, .. }
            | Expr::Assign { input, .. }
            | Expr::LogicalCall { input, .. }
            | Expr::Call { input, .. }
            | Expr::LogicalExplain { input, .. }
            | Expr::Explain { input, .. } => match index {
                0 => input,
                _ => panic!("{} is out of bounds [0,1)", index),
            },
            Expr::Leaf { .. }
            | Expr::LogicalSingleGet { .. }
            | Expr::LogicalGet { .. }
            | Expr::LogicalGetWith { .. }
            | Expr::LogicalCreateDatabase { .. }
            | Expr::LogicalCreateTable { .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalDrop { .. }
            | Expr::TableFreeScan { .. }
            | Expr::SeqScan { .. }
            | Expr::GetTempTable { .. }
            | Expr::LogicalRewrite { .. } => panic!("{} has no inputs", self.name()),
            Expr::LogicalScript { statements } | Expr::Script { statements } => &statements[index],
        }
    }
}

impl std::ops::IndexMut<usize> for Expr {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        match self {
            Expr::LogicalJoin { left, right, .. }
            | Expr::LogicalDependentJoin {
                subquery: left,
                domain: right,
                ..
            }
            | Expr::LogicalWith { left, right, .. }
            | Expr::LogicalUnion { left, right, .. }
            | Expr::NestedLoop { left, right, .. }
            | Expr::HashJoin { left, right, .. }
            | Expr::Union { left, right } => match index {
                0 => left.as_mut(),
                1 => right.as_mut(),
                _ => panic!("{} is out of bounds [0,2)", index),
            },
            Expr::LogicalFilter { input, .. }
            | Expr::LogicalOut { input, .. }
            | Expr::LogicalMap { input, .. }
            | Expr::LogicalAggregate { input, .. }
            | Expr::LogicalLimit { input, .. }
            | Expr::LogicalSort { input, .. }
            | Expr::LogicalInsert { input, .. }
            | Expr::LogicalValues { input, .. }
            | Expr::LogicalUpdate { input, .. }
            | Expr::LogicalDelete { input, .. }
            | Expr::Filter { input, .. }
            | Expr::Out { input, .. }
            | Expr::Map { input, .. }
            | Expr::IndexScan { input, .. }
            | Expr::Aggregate { input, .. }
            | Expr::Limit { input, .. }
            | Expr::Sort { input, .. }
            | Expr::Broadcast { input, .. }
            | Expr::Exchange { input, .. }
            | Expr::Insert { input, .. }
            | Expr::Values { input, .. }
            | Expr::Delete { input, .. }
            | Expr::LogicalCreateTempTable { input, .. }
            | Expr::CreateTempTable { input, .. }
            | Expr::LogicalAssign { input, .. }
            | Expr::Assign { input, .. }
            | Expr::LogicalCall { input, .. }
            | Expr::Call { input, .. }
            | Expr::LogicalExplain { input, .. }
            | Expr::Explain { input, .. } => match index {
                0 => input.as_mut(),
                _ => panic!("{} is out of bounds [0,1)", index),
            },
            Expr::Leaf { .. }
            | Expr::LogicalSingleGet { .. }
            | Expr::LogicalGet { .. }
            | Expr::LogicalGetWith { .. }
            | Expr::LogicalCreateDatabase { .. }
            | Expr::LogicalCreateTable { .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalDrop { .. }
            | Expr::TableFreeScan { .. }
            | Expr::SeqScan { .. }
            | Expr::GetTempTable { .. }
            | Expr::LogicalRewrite { .. } => panic!("{} has no inputs", self.name()),
            Expr::LogicalScript { statements } | Expr::Script { statements } => {
                &mut statements[index]
            }
        }
    }
}

impl Default for Expr {
    fn default() -> Self {
        Expr::Leaf { gid: usize::MAX }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Join {
    // Inner implements the default SQL join.
    Inner(Vec<Scalar>),
    // Right is used for both left and right joins.
    // When parsing a left join, we convert it to a right join by reversing the order of left and right.
    // The left side is the build side of the join.
    Right(Vec<Scalar>),
    // Outer includes non-matching rows from both sides.
    Outer(Vec<Scalar>),
    // Semi indicates a semi-join like `select 1 from customer where store_id in (select store_id from store)`.
    // Note that the left side is the build side of the join, which is the reverse of the usual convention.
    Semi(Vec<Scalar>),
    // Anti indicates an anti-join like `select 1 from customer where store_id not in (select store_id from store)`.
    // Note that like Semi, the left side is the build side of the join.
    Anti(Vec<Scalar>),
    // Single implements "Single Join" from http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
    // The correlated subquery is always on the left.
    Single(Vec<Scalar>),
    // Mark implements "Mark Join" from http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
    // The correlated subquery is always on the left.
    Mark(Column, Vec<Scalar>),
}

impl Join {
    pub fn predicates(&self) -> &Vec<Scalar> {
        match self {
            Join::Inner(predicates)
            | Join::Right(predicates)
            | Join::Outer(predicates)
            | Join::Semi(predicates)
            | Join::Anti(predicates)
            | Join::Single(predicates)
            | Join::Mark(_, predicates) => predicates,
        }
    }

    pub fn replace(&self, predicates: Vec<Scalar>) -> Self {
        match self {
            Join::Inner(_) => Join::Inner(predicates),
            Join::Right(_) => Join::Right(predicates),
            Join::Outer(_) => Join::Outer(predicates),
            Join::Semi(_) => Join::Semi(predicates),
            Join::Anti(_) => Join::Anti(predicates),
            Join::Single(_) => Join::Single(predicates),
            Join::Mark(mark, _) => Join::Mark(mark.clone(), predicates),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Table {
    pub id: i64,
    pub name: String,
}

impl Table {
    pub fn from(table: &zetasql::ResolvedTableScanProto) -> Self {
        match table {
            zetasql::ResolvedTableScanProto {
                table:
                    Some(zetasql::TableRefProto {
                        serialization_id: Some(id),
                        name: Some(name),
                        ..
                    }),
                ..
            } => Table {
                id: *id,
                name: name.clone(),
            },
            other => panic!("{:?}", other),
        }
    }
}

impl PartialOrd for Table {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Table {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Name {
    pub catalog_id: i64,
    pub path: Vec<String>,
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path.join("."))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum ObjectType {
    Database,
    Table,
    Index,
    Column,
}

impl ObjectType {
    pub fn from(name: &String) -> Self {
        match name.to_lowercase().as_str() {
            "database" => ObjectType::Database,
            "table" => ObjectType::Table,
            "index" => ObjectType::Index,
            "column" => ObjectType::Column,
            _ => panic!("{}", name),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct OrderBy {
    pub column: Column,
    pub descending: bool,
}

impl fmt::Display for OrderBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.descending {
            write!(f, "-{}", self.column)
        } else {
            write!(f, "{}", self.column)
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Scalar {
    Literal(Value),
    Column(Column),
    Parameter(String, DataType),
    Call(Box<F>),
    Cast(Box<Scalar>, DataType),
}

impl Scalar {
    pub fn data_type(&self) -> DataType {
        match self {
            Scalar::Literal(value) => value.data_type().clone(),
            Scalar::Column(column) => column.data_type.clone(),
            Scalar::Parameter(_, data_type) => data_type.clone(),
            Scalar::Call(function) => function.returns().clone(),
            Scalar::Cast(_, data_type) => data_type.clone(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Scalar::Literal(_) | Scalar::Column(_) | Scalar::Parameter(_, _) => 0,
            Scalar::Call(f) => f.len(),
            Scalar::Cast(_, _) => 1,
        }
    }

    pub fn references(&self) -> HashSet<Column> {
        let mut set = HashSet::new();
        self.collect_references(&mut set);
        set
    }

    fn collect_references(&self, free: &mut HashSet<Column>) {
        match self {
            Scalar::Literal(_) | Scalar::Parameter(_, _) => {}
            Scalar::Column(column) => {
                free.insert(column.clone());
            }
            Scalar::Call(function) => {
                for scalar in function.arguments() {
                    scalar.collect_references(free)
                }
            }
            Scalar::Cast(scalar, _) => scalar.collect_references(free),
        }
    }

    pub fn subst(self, map: &HashMap<Column, Column>) -> Self {
        match self {
            Scalar::Column(c) if map.contains_key(&c) => Scalar::Column(map[&c].clone()),
            Scalar::Call(f) => Scalar::Call(Box::new(f.map(|scalar| scalar.subst(map)))),
            Scalar::Cast(x, t) => Scalar::Cast(Box::new(x.subst(map)), t),
            _ => self,
        }
    }

    pub fn inline(self, expr: &Scalar, column: &Column) -> Self {
        match self {
            Scalar::Column(c) if &c == column => expr.clone(),
            Scalar::Call(f) => Scalar::Call(Box::new(f.map(|scalar| scalar.inline(expr, column)))),
            Scalar::Cast(uncast, data_type) => {
                Scalar::Cast(Box::new(uncast.inline(expr, column)), data_type.clone())
            }
            _ => self.clone(),
        }
    }

    pub fn is_just(&self, column: &Column) -> bool {
        match self {
            Scalar::Column(c) => c == column,
            _ => false,
        }
    }
}

impl std::ops::Index<usize> for Scalar {
    type Output = Scalar;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            Scalar::Literal(_) | Scalar::Column(_) | Scalar::Parameter(_, _) => panic!(index),
            Scalar::Call(f) => &f[index],
            Scalar::Cast(x, _) => {
                if index == 0 {
                    x.as_ref()
                } else {
                    panic!(index)
                }
            }
        }
    }
}

impl std::ops::IndexMut<usize> for Scalar {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        match self {
            Scalar::Literal(_) | Scalar::Column(_) | Scalar::Parameter(_, _) => panic!(index),
            Scalar::Call(f) => &mut f[index],
            Scalar::Cast(x, _) => {
                if index == 0 {
                    x.as_mut()
                } else {
                    panic!(index)
                }
            }
        }
    }
}

// Functions appear in scalar expressions.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum F {
    CurrentDate,
    CurrentTimestamp,
    Xid,
    Coalesce(Vec<Scalar>),
    ConcatString(Vec<Scalar>),
    Hash(Vec<Scalar>),
    Greatest(Vec<Scalar>),
    Least(Vec<Scalar>),
    AbsDouble(Scalar),
    AbsInt64(Scalar),
    AcosDouble(Scalar),
    AcoshDouble(Scalar),
    AsinDouble(Scalar),
    AsinhDouble(Scalar),
    AtanDouble(Scalar),
    AtanhDouble(Scalar),
    ByteLengthString(Scalar),
    CeilDouble(Scalar),
    CharLengthString(Scalar),
    ChrString(Scalar),
    CosDouble(Scalar),
    CoshDouble(Scalar),
    DateFromTimestamp(Scalar),
    DateFromUnixDate(Scalar),
    DecimalLogarithmDouble(Scalar),
    ExpDouble(Scalar),
    ExtractDateFromTimestamp(Scalar),
    FloorDouble(Scalar),
    IsFalse(Scalar),
    IsInf(Scalar),
    IsNan(Scalar),
    IsNull(Scalar),
    IsTrue(Scalar),
    LengthString(Scalar),
    LowerString(Scalar),
    NaturalLogarithmDouble(Scalar),
    NextVal(Scalar),
    Not(Scalar),
    ReverseString(Scalar),
    RoundDouble(Scalar),
    SignDouble(Scalar),
    SignInt64(Scalar),
    SinDouble(Scalar),
    SinhDouble(Scalar),
    SqrtDouble(Scalar),
    StringFromDate(Scalar),
    StringFromTimestamp(Scalar),
    TanDouble(Scalar),
    TanhDouble(Scalar),
    TimestampFromDate(Scalar),
    TimestampFromString(Scalar),
    TimestampFromUnixMicrosInt64(Scalar),
    TruncDouble(Scalar),
    UnaryMinusDouble(Scalar),
    UnaryMinusInt64(Scalar),
    UnixDate(Scalar),
    UnixMicrosFromTimestamp(Scalar),
    UpperString(Scalar),
    DateTruncDate(Scalar, DatePart),
    ExtractFromDate(Scalar, DatePart),
    ExtractFromTimestamp(Scalar, DatePart),
    TimestampTrunc(Scalar, DatePart),
    In(Scalar, Vec<Scalar>),
    AddDouble(Scalar, Scalar),
    AddInt64(Scalar, Scalar),
    And(Scalar, Scalar),
    Atan2Double(Scalar, Scalar),
    DivideDouble(Scalar, Scalar),
    DivInt64(Scalar, Scalar),
    EndsWithString(Scalar, Scalar),
    Equal(Scalar, Scalar),
    FormatDate(Scalar, Scalar),
    FormatTimestamp(Scalar, Scalar),
    Greater(Scalar, Scalar),
    GreaterOrEqual(Scalar, Scalar),
    Ifnull(Scalar, Scalar),
    Is(Scalar, Scalar),
    LeftString(Scalar, Scalar),
    Less(Scalar, Scalar),
    LessOrEqual(Scalar, Scalar),
    LogarithmDouble(Scalar, Scalar),
    LtrimString(Scalar, Option<Scalar>),
    ModInt64(Scalar, Scalar),
    MultiplyDouble(Scalar, Scalar),
    MultiplyInt64(Scalar, Scalar),
    NotEqual(Scalar, Scalar),
    Nullif(Scalar, Scalar),
    Or(Scalar, Scalar),
    ParseDate(Scalar, Scalar),
    ParseTimestamp(Scalar, Scalar),
    PowDouble(Scalar, Scalar),
    RegexpContainsString(Scalar, Scalar),
    RegexpExtractString(Scalar, Scalar),
    RepeatString(Scalar, Scalar),
    RightString(Scalar, Scalar),
    RoundWithDigitsDouble(Scalar, Scalar),
    RtrimString(Scalar, Option<Scalar>),
    StartsWithString(Scalar, Scalar),
    StringLike(Scalar, Scalar),
    StrposString(Scalar, Scalar),
    SubtractDouble(Scalar, Scalar),
    SubtractInt64(Scalar, Scalar),
    TrimString(Scalar, Option<Scalar>),
    TruncWithDigitsDouble(Scalar, Scalar),
    DateAddDate(Scalar, Scalar, DatePart),
    DateDiffDate(Scalar, Scalar, DatePart),
    DateSubDate(Scalar, Scalar, DatePart),
    TimestampAdd(Scalar, Scalar, DatePart),
    TimestampDiff(Scalar, Scalar, DatePart),
    TimestampSub(Scalar, Scalar, DatePart),
    Between(Scalar, Scalar, Scalar),
    DateFromYearMonthDay(Scalar, Scalar, Scalar),
    If(Scalar, Scalar, Scalar),
    LpadString(Scalar, Scalar, Scalar),
    RegexpReplaceString(Scalar, Scalar, Scalar),
    ReplaceString(Scalar, Scalar, Scalar),
    RpadString(Scalar, Scalar, Scalar),
    SubstrString(Scalar, Scalar, Option<Scalar>),
    CaseNoValue(Vec<(Scalar, Scalar)>, Scalar),
    CaseWithValue(Scalar, Vec<(Scalar, Scalar)>, Scalar),
}

impl F {
    pub fn len(&self) -> usize {
        match self {
            F::CurrentDate | F::CurrentTimestamp | F::Xid => 0,
            F::AbsDouble(_)
            | F::AbsInt64(_)
            | F::AcosDouble(_)
            | F::AcoshDouble(_)
            | F::AsinDouble(_)
            | F::AsinhDouble(_)
            | F::AtanDouble(_)
            | F::AtanhDouble(_)
            | F::ByteLengthString(_)
            | F::CeilDouble(_)
            | F::CharLengthString(_)
            | F::ChrString(_)
            | F::CosDouble(_)
            | F::CoshDouble(_)
            | F::DateFromTimestamp(_)
            | F::DateFromUnixDate(_)
            | F::DecimalLogarithmDouble(_)
            | F::ExpDouble(_)
            | F::ExtractDateFromTimestamp(_)
            | F::FloorDouble(_)
            | F::IsFalse(_)
            | F::IsInf(_)
            | F::IsNan(_)
            | F::IsNull(_)
            | F::IsTrue(_)
            | F::LengthString(_)
            | F::LowerString(_)
            | F::NaturalLogarithmDouble(_)
            | F::NextVal(_)
            | F::Not(_)
            | F::ReverseString(_)
            | F::RoundDouble(_)
            | F::SignDouble(_)
            | F::SignInt64(_)
            | F::SinDouble(_)
            | F::SinhDouble(_)
            | F::SqrtDouble(_)
            | F::StringFromDate(_)
            | F::StringFromTimestamp(_)
            | F::TanDouble(_)
            | F::TanhDouble(_)
            | F::TimestampFromDate(_)
            | F::TimestampFromString(_)
            | F::TimestampFromUnixMicrosInt64(_)
            | F::TruncDouble(_)
            | F::UnaryMinusDouble(_)
            | F::UnaryMinusInt64(_)
            | F::UnixDate(_)
            | F::UnixMicrosFromTimestamp(_)
            | F::UpperString(_)
            | F::DateTruncDate(_, _)
            | F::ExtractFromDate(_, _)
            | F::ExtractFromTimestamp(_, _)
            | F::TrimString(_, None)
            | F::TimestampTrunc(_, _)
            | F::LtrimString(_, None)
            | F::RtrimString(_, None) => 1,
            F::AddDouble(_, _)
            | F::AddInt64(_, _)
            | F::And(_, _)
            | F::Atan2Double(_, _)
            | F::DivideDouble(_, _)
            | F::DivInt64(_, _)
            | F::EndsWithString(_, _)
            | F::Equal(_, _)
            | F::FormatDate(_, _)
            | F::FormatTimestamp(_, _)
            | F::Greater(_, _)
            | F::GreaterOrEqual(_, _)
            | F::Ifnull(_, _)
            | F::Is(_, _)
            | F::LeftString(_, _)
            | F::Less(_, _)
            | F::LessOrEqual(_, _)
            | F::LogarithmDouble(_, _)
            | F::LtrimString(_, Some(_))
            | F::ModInt64(_, _)
            | F::MultiplyDouble(_, _)
            | F::MultiplyInt64(_, _)
            | F::NotEqual(_, _)
            | F::Nullif(_, _)
            | F::Or(_, _)
            | F::ParseDate(_, _)
            | F::ParseTimestamp(_, _)
            | F::PowDouble(_, _)
            | F::RegexpContainsString(_, _)
            | F::RegexpExtractString(_, _)
            | F::RepeatString(_, _)
            | F::RightString(_, _)
            | F::RoundWithDigitsDouble(_, _)
            | F::RtrimString(_, Some(_))
            | F::StartsWithString(_, _)
            | F::StringLike(_, _)
            | F::StrposString(_, _)
            | F::SubtractDouble(_, _)
            | F::SubtractInt64(_, _)
            | F::TrimString(_, Some(_))
            | F::TruncWithDigitsDouble(_, _)
            | F::DateAddDate(_, _, _)
            | F::DateDiffDate(_, _, _)
            | F::DateSubDate(_, _, _)
            | F::TimestampAdd(_, _, _)
            | F::TimestampDiff(_, _, _)
            | F::TimestampSub(_, _, _)
            | F::SubstrString(_, _, None) => 2,
            F::Between(_, _, _)
            | F::DateFromYearMonthDay(_, _, _)
            | F::If(_, _, _)
            | F::LpadString(_, _, _)
            | F::RegexpReplaceString(_, _, _)
            | F::ReplaceString(_, _, _)
            | F::RpadString(_, _, _)
            | F::SubstrString(_, _, Some(_)) => 3,
            F::Coalesce(varargs)
            | F::ConcatString(varargs)
            | F::Hash(varargs)
            | F::Greatest(varargs)
            | F::Least(varargs) => varargs.len(),
            F::In(_, varargs) => varargs.len() + 1,
            F::CaseNoValue(varargs, _) => varargs.len() * 2 + 1,
            F::CaseWithValue(_, varargs, _) => varargs.len() * 2 + 2,
        }
    }
}

impl std::ops::Index<usize> for F {
    type Output = Scalar;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            F::CurrentDate | F::CurrentTimestamp | F::Xid => panic!(index),
            F::AbsDouble(a)
            | F::AbsInt64(a)
            | F::AcosDouble(a)
            | F::AcoshDouble(a)
            | F::AsinDouble(a)
            | F::AsinhDouble(a)
            | F::AtanDouble(a)
            | F::AtanhDouble(a)
            | F::ByteLengthString(a)
            | F::CeilDouble(a)
            | F::CharLengthString(a)
            | F::ChrString(a)
            | F::CosDouble(a)
            | F::CoshDouble(a)
            | F::DateFromTimestamp(a)
            | F::DateFromUnixDate(a)
            | F::DecimalLogarithmDouble(a)
            | F::ExpDouble(a)
            | F::ExtractDateFromTimestamp(a)
            | F::FloorDouble(a)
            | F::IsFalse(a)
            | F::IsInf(a)
            | F::IsNan(a)
            | F::IsNull(a)
            | F::IsTrue(a)
            | F::LengthString(a)
            | F::LowerString(a)
            | F::NaturalLogarithmDouble(a)
            | F::NextVal(a)
            | F::Not(a)
            | F::ReverseString(a)
            | F::RoundDouble(a)
            | F::SignDouble(a)
            | F::SignInt64(a)
            | F::SinDouble(a)
            | F::SinhDouble(a)
            | F::SqrtDouble(a)
            | F::StringFromDate(a)
            | F::StringFromTimestamp(a)
            | F::TanDouble(a)
            | F::TanhDouble(a)
            | F::TimestampFromDate(a)
            | F::TimestampFromString(a)
            | F::TimestampFromUnixMicrosInt64(a)
            | F::TruncDouble(a)
            | F::UnaryMinusDouble(a)
            | F::UnaryMinusInt64(a)
            | F::UnixDate(a)
            | F::UnixMicrosFromTimestamp(a)
            | F::UpperString(a)
            | F::DateTruncDate(a, _)
            | F::ExtractFromDate(a, _)
            | F::ExtractFromTimestamp(a, _)
            | F::TrimString(a, None)
            | F::TimestampTrunc(a, _)
            | F::LtrimString(a, None)
            | F::RtrimString(a, None) => match index {
                0 => a,
                _ => panic!(index),
            },
            F::AddDouble(a, b)
            | F::AddInt64(a, b)
            | F::And(a, b)
            | F::Atan2Double(a, b)
            | F::DivideDouble(a, b)
            | F::DivInt64(a, b)
            | F::EndsWithString(a, b)
            | F::Equal(a, b)
            | F::FormatDate(a, b)
            | F::FormatTimestamp(a, b)
            | F::Greater(a, b)
            | F::GreaterOrEqual(a, b)
            | F::Ifnull(a, b)
            | F::Is(a, b)
            | F::LeftString(a, b)
            | F::Less(a, b)
            | F::LessOrEqual(a, b)
            | F::LogarithmDouble(a, b)
            | F::LtrimString(a, Some(b))
            | F::ModInt64(a, b)
            | F::MultiplyDouble(a, b)
            | F::MultiplyInt64(a, b)
            | F::NotEqual(a, b)
            | F::Nullif(a, b)
            | F::Or(a, b)
            | F::ParseDate(a, b)
            | F::ParseTimestamp(a, b)
            | F::PowDouble(a, b)
            | F::RegexpContainsString(a, b)
            | F::RegexpExtractString(a, b)
            | F::RepeatString(a, b)
            | F::RightString(a, b)
            | F::RoundWithDigitsDouble(a, b)
            | F::RtrimString(a, Some(b))
            | F::StartsWithString(a, b)
            | F::StringLike(a, b)
            | F::StrposString(a, b)
            | F::SubtractDouble(a, b)
            | F::SubtractInt64(a, b)
            | F::TrimString(a, Some(b))
            | F::TruncWithDigitsDouble(a, b)
            | F::DateAddDate(a, b, _)
            | F::DateDiffDate(a, b, _)
            | F::DateSubDate(a, b, _)
            | F::TimestampAdd(a, b, _)
            | F::TimestampDiff(a, b, _)
            | F::TimestampSub(a, b, _)
            | F::SubstrString(a, b, None) => match index {
                0 => a,
                1 => b,
                _ => panic!(index),
            },
            F::Between(a, b, c)
            | F::DateFromYearMonthDay(a, b, c)
            | F::If(a, b, c)
            | F::LpadString(a, b, c)
            | F::RegexpReplaceString(a, b, c)
            | F::ReplaceString(a, b, c)
            | F::RpadString(a, b, c)
            | F::SubstrString(a, b, Some(c)) => match index {
                0 => a,
                1 => b,
                2 => c,
                _ => panic!(index),
            },
            F::Coalesce(varargs)
            | F::ConcatString(varargs)
            | F::Hash(varargs)
            | F::Greatest(varargs)
            | F::Least(varargs) => &varargs[index],
            F::In(a, varargs) => match index {
                0 => a,
                _ => &varargs[index - 1],
            },
            F::CaseNoValue(varargs, default) => {
                if index < varargs.len() * 2 {
                    let (a, b) = &varargs[index / 2];
                    match index % 2 {
                        0 => a,
                        1 => b,
                        _ => panic!(),
                    }
                } else if index == varargs.len() * 2 {
                    default
                } else {
                    panic!(index)
                }
            }
            F::CaseWithValue(value, varargs, default) => {
                if index == 0 {
                    value
                } else if index - 1 < varargs.len() * 2 {
                    let (a, b) = &varargs[(index - 1) / 2];
                    match (index - 1) % 2 {
                        0 => a,
                        1 => b,
                        _ => panic!(),
                    }
                } else if (index - 1) == varargs.len() * 2 {
                    default
                } else {
                    panic!(index)
                }
            }
        }
    }
}

impl std::ops::IndexMut<usize> for F {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        match self {
            F::CurrentDate | F::CurrentTimestamp | F::Xid => panic!(index),
            F::AbsDouble(a)
            | F::AbsInt64(a)
            | F::AcosDouble(a)
            | F::AcoshDouble(a)
            | F::AsinDouble(a)
            | F::AsinhDouble(a)
            | F::AtanDouble(a)
            | F::AtanhDouble(a)
            | F::ByteLengthString(a)
            | F::CeilDouble(a)
            | F::CharLengthString(a)
            | F::ChrString(a)
            | F::CosDouble(a)
            | F::CoshDouble(a)
            | F::DateFromTimestamp(a)
            | F::DateFromUnixDate(a)
            | F::DecimalLogarithmDouble(a)
            | F::ExpDouble(a)
            | F::ExtractDateFromTimestamp(a)
            | F::FloorDouble(a)
            | F::IsFalse(a)
            | F::IsInf(a)
            | F::IsNan(a)
            | F::IsNull(a)
            | F::IsTrue(a)
            | F::LengthString(a)
            | F::LowerString(a)
            | F::NaturalLogarithmDouble(a)
            | F::NextVal(a)
            | F::Not(a)
            | F::ReverseString(a)
            | F::RoundDouble(a)
            | F::SignDouble(a)
            | F::SignInt64(a)
            | F::SinDouble(a)
            | F::SinhDouble(a)
            | F::SqrtDouble(a)
            | F::StringFromDate(a)
            | F::StringFromTimestamp(a)
            | F::TanDouble(a)
            | F::TanhDouble(a)
            | F::TimestampFromDate(a)
            | F::TimestampFromString(a)
            | F::TimestampFromUnixMicrosInt64(a)
            | F::TruncDouble(a)
            | F::UnaryMinusDouble(a)
            | F::UnaryMinusInt64(a)
            | F::UnixDate(a)
            | F::UnixMicrosFromTimestamp(a)
            | F::UpperString(a)
            | F::DateTruncDate(a, _)
            | F::ExtractFromDate(a, _)
            | F::ExtractFromTimestamp(a, _)
            | F::TrimString(a, None)
            | F::TimestampTrunc(a, _)
            | F::LtrimString(a, None)
            | F::RtrimString(a, None) => match index {
                0 => a,
                _ => panic!(index),
            },
            F::AddDouble(a, b)
            | F::AddInt64(a, b)
            | F::And(a, b)
            | F::Atan2Double(a, b)
            | F::DivideDouble(a, b)
            | F::DivInt64(a, b)
            | F::EndsWithString(a, b)
            | F::Equal(a, b)
            | F::FormatDate(a, b)
            | F::FormatTimestamp(a, b)
            | F::Greater(a, b)
            | F::GreaterOrEqual(a, b)
            | F::Ifnull(a, b)
            | F::Is(a, b)
            | F::LeftString(a, b)
            | F::Less(a, b)
            | F::LessOrEqual(a, b)
            | F::LogarithmDouble(a, b)
            | F::LtrimString(a, Some(b))
            | F::ModInt64(a, b)
            | F::MultiplyDouble(a, b)
            | F::MultiplyInt64(a, b)
            | F::NotEqual(a, b)
            | F::Nullif(a, b)
            | F::Or(a, b)
            | F::ParseDate(a, b)
            | F::ParseTimestamp(a, b)
            | F::PowDouble(a, b)
            | F::RegexpContainsString(a, b)
            | F::RegexpExtractString(a, b)
            | F::RepeatString(a, b)
            | F::RightString(a, b)
            | F::RoundWithDigitsDouble(a, b)
            | F::RtrimString(a, Some(b))
            | F::StartsWithString(a, b)
            | F::StringLike(a, b)
            | F::StrposString(a, b)
            | F::SubtractDouble(a, b)
            | F::SubtractInt64(a, b)
            | F::TrimString(a, Some(b))
            | F::TruncWithDigitsDouble(a, b)
            | F::DateAddDate(a, b, _)
            | F::DateDiffDate(a, b, _)
            | F::DateSubDate(a, b, _)
            | F::TimestampAdd(a, b, _)
            | F::TimestampDiff(a, b, _)
            | F::TimestampSub(a, b, _)
            | F::SubstrString(a, b, None) => match index {
                0 => a,
                1 => b,
                _ => panic!(index),
            },
            F::Between(a, b, c)
            | F::DateFromYearMonthDay(a, b, c)
            | F::If(a, b, c)
            | F::LpadString(a, b, c)
            | F::RegexpReplaceString(a, b, c)
            | F::ReplaceString(a, b, c)
            | F::RpadString(a, b, c)
            | F::SubstrString(a, b, Some(c)) => match index {
                0 => a,
                1 => b,
                2 => c,
                _ => panic!(index),
            },
            F::Coalesce(varargs)
            | F::ConcatString(varargs)
            | F::Hash(varargs)
            | F::Greatest(varargs)
            | F::Least(varargs) => &mut varargs[index],
            F::In(a, varargs) => match index {
                0 => a,
                _ => &mut varargs[index - 1],
            },
            F::CaseNoValue(varargs, default) => {
                if index < varargs.len() * 2 {
                    let (a, b) = &mut varargs[index / 2];
                    match index % 2 {
                        0 => a,
                        1 => b,
                        _ => panic!(),
                    }
                } else if index == varargs.len() * 2 {
                    default
                } else {
                    panic!(index)
                }
            }
            F::CaseWithValue(value, varargs, default) => {
                if index == 0 {
                    value
                } else if index - 1 < varargs.len() * 2 {
                    let (a, b) = &mut varargs[(index - 1) / 2];
                    match (index - 1) % 2 {
                        0 => a,
                        1 => b,
                        _ => panic!(),
                    }
                } else if (index - 1) == varargs.len() * 2 {
                    default
                } else {
                    panic!(index)
                }
            }
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(from = "i32", into = "i32")]
pub enum DatePart {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    DayOfWeek,
    Day,
    DayOfYear,
    Week(Weekday),
    IsoWeek,
    Month,
    Quarter,
    Year,
    IsoYear,
}

impl DatePart {
    fn from_scalar(scalar: Scalar) -> Self {
        if let Scalar::Literal(Value::EnumValue(i)) = scalar {
            match i {
                1 => DatePart::Year,
                2 => DatePart::Month,
                3 => DatePart::Day,
                4 => DatePart::DayOfWeek,
                5 => DatePart::DayOfYear,
                6 => DatePart::Quarter,
                7 => DatePart::Hour,
                8 => DatePart::Minute,
                9 => DatePart::Second,
                10 => DatePart::Millisecond,
                11 => DatePart::Microsecond,
                12 => panic!("NANOSECOND part is not supported"),
                13 => panic!("DATE part is not supported"),
                14 => DatePart::Week(Weekday::Sun),
                15 => panic!("DATETIME part is not supported"),
                16 => panic!("TIME part is not supported"),
                17 => DatePart::IsoYear,
                18 => DatePart::IsoWeek,
                19 => DatePart::Week(Weekday::Mon),
                20 => DatePart::Week(Weekday::Tue),
                21 => DatePart::Week(Weekday::Wed),
                22 => DatePart::Week(Weekday::Thu),
                23 => DatePart::Week(Weekday::Fri),
                24 => DatePart::Week(Weekday::Sat),
                _ => panic!("{} is not a recognized date part", i),
            }
        } else {
            panic!("{:?} is not an enum value", scalar)
        }
    }
}

impl From<i32> for DatePart {
    fn from(i: i32) -> Self {
        match i {
            1 => DatePart::Microsecond,
            2 => DatePart::Millisecond,
            3 => DatePart::Second,
            4 => DatePart::Minute,
            5 => DatePart::Hour,
            6 => DatePart::DayOfWeek,
            7 => DatePart::Day,
            8 => DatePart::DayOfYear,
            9 => DatePart::Week(Weekday::Mon),
            10 => DatePart::Week(Weekday::Tue),
            11 => DatePart::Week(Weekday::Wed),
            12 => DatePart::Week(Weekday::Thu),
            13 => DatePart::Week(Weekday::Fri),
            14 => DatePart::Week(Weekday::Sat),
            15 => DatePart::Week(Weekday::Sun),
            16 => DatePart::IsoWeek,
            17 => DatePart::Month,
            18 => DatePart::Quarter,
            19 => DatePart::Year,
            20 => DatePart::IsoYear,
            other => panic!("{}", other),
        }
    }
}
impl Into<i32> for DatePart {
    fn into(self) -> i32 {
        match self {
            DatePart::Microsecond => 1,
            DatePart::Millisecond => 2,
            DatePart::Second => 3,
            DatePart::Minute => 4,
            DatePart::Hour => 5,
            DatePart::DayOfWeek => 6,
            DatePart::Day => 7,
            DatePart::DayOfYear => 8,
            DatePart::Week(Weekday::Mon) => 9,
            DatePart::Week(Weekday::Tue) => 10,
            DatePart::Week(Weekday::Wed) => 11,
            DatePart::Week(Weekday::Thu) => 12,
            DatePart::Week(Weekday::Fri) => 13,
            DatePart::Week(Weekday::Sat) => 14,
            DatePart::Week(Weekday::Sun) => 15,
            DatePart::IsoWeek => 16,
            DatePart::Month => 17,
            DatePart::Quarter => 18,
            DatePart::Year => 19,
            DatePart::IsoYear => 20,
        }
    }
}

fn thunk(args: Vec<Scalar>, f: impl Fn() -> F) -> F {
    assert_eq!(args.len(), 0);
    f()
}

fn unary(mut args: Vec<Scalar>, f: impl Fn(Scalar) -> F) -> F {
    assert_eq!(args.len(), 1);
    let a = args.pop().unwrap();
    f(a)
}

fn unary_date_part(mut args: Vec<Scalar>, f: impl Fn(Scalar, DatePart) -> F) -> F {
    assert_eq!(args.len(), 2);
    let b = DatePart::from_scalar(args.pop().unwrap());
    let a = args.pop().unwrap();
    f(a, b)
}

fn unary_vararg(mut args: Vec<Scalar>, f: impl Fn(Scalar, Vec<Scalar>) -> F) -> F {
    assert!(args.len() >= 2);
    f(args.remove(0), args)
}

fn binary(mut args: Vec<Scalar>, f: impl Fn(Scalar, Scalar) -> F) -> F {
    assert_eq!(args.len(), 2);
    let b = args.pop().unwrap();
    let a = args.pop().unwrap();
    f(a, b)
}

fn binary_date_part(mut args: Vec<Scalar>, f: impl Fn(Scalar, Scalar, DatePart) -> F) -> F {
    assert_eq!(args.len(), 3);
    let c = DatePart::from_scalar(args.pop().unwrap());
    let b = args.pop().unwrap();
    let a = args.pop().unwrap();
    f(a, b, c)
}

fn ternary(mut args: Vec<Scalar>, f: impl Fn(Scalar, Scalar, Scalar) -> F) -> F {
    assert_eq!(args.len(), 3);
    let c = args.pop().unwrap();
    let b = args.pop().unwrap();
    let a = args.pop().unwrap();
    f(a, b, c)
}

impl F {
    pub fn from(
        function: &zetasql::FunctionRefProto,
        signature: &zetasql::FunctionSignatureProto,
        mut args: Vec<Scalar>,
    ) -> Self {
        let name = function.name.as_ref().unwrap().as_str();
        let first_argument = signature
            .argument
            .first()
            .map(|argument| DataType::from(argument.r#type.as_ref().unwrap()));
        let returns = DataType::from(
            signature
                .return_type
                .as_ref()
                .unwrap()
                .r#type
                .as_ref()
                .unwrap(),
        );
        match name {
            "ZetaSQL:$add" if returns == DataType::F64 => binary(args, |a, b| F::AddDouble(a, b)),
            "ZetaSQL:$add" if returns == DataType::I64 => binary(args, |a, b| F::AddInt64(a, b)),
            "ZetaSQL:$and" => binary(args, |a, b| F::And(a, b)),
            "ZetaSQL:$case_no_value" => {
                let mut cases = vec![];
                while args.len() > 1 {
                    cases.push((args.remove(0), args.remove(0)))
                }
                let default = args.pop().unwrap();
                F::CaseNoValue(cases, default)
            }
            "ZetaSQL:$case_with_value" => {
                let value = args.remove(0);
                let mut cases = vec![];
                while args.len() > 1 {
                    cases.push((args.remove(0), args.remove(0)))
                }
                let default = args.pop().unwrap();
                F::CaseWithValue(value, cases, default)
            }
            "ZetaSQL:$divide" => binary(args, |a, b| F::DivideDouble(a, b)),
            "ZetaSQL:$greater" => binary(args, |a, b| F::Greater(a, b)),
            "ZetaSQL:$greater_or_equal" => binary(args, |a, b| F::GreaterOrEqual(a, b)),
            "ZetaSQL:$less" => binary(args, |a, b| F::Less(a, b)),
            "ZetaSQL:$less_or_equal" => binary(args, |a, b| F::LessOrEqual(a, b)),
            "ZetaSQL:$equal" => binary(args, |a, b| F::Equal(a, b)),
            "ZetaSQL:$like" => binary(args, |a, b| F::StringLike(a, b)),
            "ZetaSQL:$in" => unary_vararg(args, |a, varargs| F::In(a, varargs)),
            "ZetaSQL:$between" => ternary(args, |a, b, c| F::Between(a, b, c)),
            "ZetaSQL:$is_null" => unary(args, |a| F::IsNull(a)),
            "ZetaSQL:$is_true" => unary(args, |a| F::IsTrue(a)),
            "ZetaSQL:$is_false" => unary(args, |a| F::IsFalse(a)),
            "ZetaSQL:$multiply" if returns == DataType::F64 => {
                binary(args, |a, b| F::MultiplyDouble(a, b))
            }
            "ZetaSQL:$multiply" if returns == DataType::I64 => {
                binary(args, |a, b| F::MultiplyInt64(a, b))
            }
            "ZetaSQL:$not" => unary(args, |a| F::Not(a)),
            "ZetaSQL:$not_equal" => binary(args, |a, b| F::NotEqual(a, b)),
            "ZetaSQL:$or" => binary(args, |a, b| F::Or(a, b)),
            "ZetaSQL:$subtract" if returns == DataType::F64 => {
                binary(args, |a, b| F::SubtractDouble(a, b))
            }
            "ZetaSQL:$subtract" if returns == DataType::I64 => {
                binary(args, |a, b| F::SubtractInt64(a, b))
            }
            "ZetaSQL:$unary_minus" if returns == DataType::I64 => {
                unary(args, |a| F::UnaryMinusInt64(a))
            }
            "ZetaSQL:$unary_minus" if returns == DataType::F64 => {
                unary(args, |a| F::UnaryMinusDouble(a))
            }
            "ZetaSQL:concat" => F::ConcatString(args),
            "ZetaSQL:strpos" => binary(args, |a, b| F::StrposString(a, b)),
            "ZetaSQL:lower" => unary(args, |a| F::LowerString(a)),
            "ZetaSQL:upper" => unary(args, |a| F::UpperString(a)),
            "ZetaSQL:length" => unary(args, |a| F::LengthString(a)),
            "ZetaSQL:starts_with" => binary(args, |a, b| F::StartsWithString(a, b)),
            "ZetaSQL:ends_with" => binary(args, |a, b| F::EndsWithString(a, b)),
            "ZetaSQL:substr" if args.len() == 2 => binary(args, |a, b| F::SubstrString(a, b, None)),
            "ZetaSQL:substr" if args.len() == 3 => {
                ternary(args, |a, b, c| F::SubstrString(a, b, Some(c)))
            }
            "ZetaSQL:trim" if args.len() == 1 => unary(args, |a| F::TrimString(a, None)),
            "ZetaSQL:ltrim" if args.len() == 1 => unary(args, |a| F::LtrimString(a, None)),
            "ZetaSQL:rtrim" if args.len() == 1 => unary(args, |a| F::RtrimString(a, None)),
            "ZetaSQL:trim" if args.len() == 2 => binary(args, |a, b| F::TrimString(a, Some(b))),
            "ZetaSQL:ltrim" if args.len() == 2 => binary(args, |a, b| F::LtrimString(a, Some(b))),
            "ZetaSQL:rtrim" if args.len() == 2 => binary(args, |a, b| F::RtrimString(a, Some(b))),
            "ZetaSQL:replace" => ternary(args, |a, b, c| F::ReplaceString(a, b, c)),
            "ZetaSQL:regexp_extract" => binary(args, |a, b| F::RegexpExtractString(a, b)),
            "ZetaSQL:regexp_replace" => ternary(args, |a, b, c| F::RegexpReplaceString(a, b, c)),
            "ZetaSQL:byte_length" => unary(args, |a| F::ByteLengthString(a)),
            "ZetaSQL:char_length" => unary(args, |a| F::CharLengthString(a)),
            "ZetaSQL:regexp_contains" => binary(args, |a, b| F::RegexpContainsString(a, b)),
            "ZetaSQL:lpad" if args.len() == 2 => binary(args, |a, b| {
                F::LpadString(a, b, Scalar::Literal(Value::String(Some(" ".to_string()))))
            }),
            "ZetaSQL:lpad" if args.len() == 3 => ternary(args, |a, b, c| F::LpadString(a, b, c)),
            "ZetaSQL:rpad" if args.len() == 2 => binary(args, |a, b| {
                F::RpadString(a, b, Scalar::Literal(Value::String(Some(" ".to_string()))))
            }),
            "ZetaSQL:rpad" if args.len() == 3 => ternary(args, |a, b, c| F::RpadString(a, b, c)),
            "ZetaSQL:left" => binary(args, |a, b| F::LeftString(a, b)),
            "ZetaSQL:right" => binary(args, |a, b| F::RightString(a, b)),
            "ZetaSQL:repeat" => binary(args, |a, b| F::RepeatString(a, b)),
            "ZetaSQL:reverse" => unary(args, |a| F::ReverseString(a)),
            "ZetaSQL:chr" => unary(args, |a| F::ChrString(a)),
            "ZetaSQL:if" => ternary(args, |a, b, c| F::If(a, b, c)),
            "ZetaSQL:coalesce" => F::Coalesce(args),
            "ZetaSQL:ifnull" => binary(args, |a, b| F::Ifnull(a, b)),
            "ZetaSQL:nullif" => binary(args, |a, b| F::Nullif(a, b)),
            "ZetaSQL:current_date" => thunk(args, || F::CurrentDate),
            "ZetaSQL:current_timestamp" => thunk(args, || F::CurrentTimestamp),
            "ZetaSQL:date_add" => {
                binary_date_part(args, |a, b, date_part| F::DateAddDate(a, b, date_part))
            }
            "ZetaSQL:timestamp_add" => {
                binary_date_part(args, |a, b, date_part| F::TimestampAdd(a, b, date_part))
            }
            "ZetaSQL:date_diff" => {
                binary_date_part(args, |a, b, date_part| F::DateDiffDate(a, b, date_part))
            }
            "ZetaSQL:timestamp_diff" => {
                binary_date_part(args, |a, b, date_part| F::TimestampDiff(a, b, date_part))
            }
            "ZetaSQL:date_sub" => {
                binary_date_part(args, |a, b, date_part| F::DateSubDate(a, b, date_part))
            }
            "ZetaSQL:timestamp_sub" => {
                binary_date_part(args, |a, b, date_part| F::TimestampSub(a, b, date_part))
            }
            "ZetaSQL:date_trunc" => {
                unary_date_part(args, |a, date_part| F::DateTruncDate(a, date_part))
            }
            "ZetaSQL:timestamp_trunc" if args.len() == 2 => {
                unary_date_part(args, |a, date_part| F::TimestampTrunc(a, date_part))
            }
            "ZetaSQL:timestamp_trunc" if args.len() == 3 => {
                panic!("TIMESTAMP_TRUNC with time zone is not supported")
            }
            "ZetaSQL:date_from_unix_date" => unary(args, |a| F::DateFromUnixDate(a)),
            "ZetaSQL:timestamp_from_unix_micros" => {
                unary(args, |a| F::TimestampFromUnixMicrosInt64(a))
            }
            "ZetaSQL:unix_date" => unary(args, |a| F::UnixDate(a)),
            "ZetaSQL:unix_micros" => unary(args, |a| F::UnixMicrosFromTimestamp(a)),
            "ZetaSQL:date" if first_argument == Some(DataType::Timestamp) => {
                unary(args, |a| F::DateFromTimestamp(a))
            }
            "ZetaSQL:date" if signature.argument.len() == 3 => {
                ternary(args, |a, b, c| F::DateFromYearMonthDay(a, b, c))
            }
            "ZetaSQL:timestamp" if first_argument == Some(DataType::String) => {
                unary(args, |a| F::TimestampFromString(a))
            }
            "ZetaSQL:timestamp" if first_argument == Some(DataType::Date) => {
                unary(args, |a| F::TimestampFromDate(a))
            }
            "ZetaSQL:string" if first_argument == Some(DataType::Date) => {
                unary(args, |a| F::StringFromDate(a))
            }
            "ZetaSQL:string" if first_argument == Some(DataType::Timestamp) => {
                unary(args, |a| F::StringFromTimestamp(a))
            }
            "ZetaSQL:$extract" if first_argument == Some(DataType::Date) => {
                unary_date_part(args, |a, date_part| F::ExtractFromDate(a, date_part))
            }
            "ZetaSQL:$extract" if first_argument == Some(DataType::Timestamp) => {
                unary_date_part(args, |a, date_part| F::ExtractFromTimestamp(a, date_part))
            }
            "ZetaSQL:$extract_date" if args.len() == 1 => {
                unary(args, |a| F::ExtractDateFromTimestamp(a))
            }
            "ZetaSQL:$extract_date" if args.len() == 2 => {
                panic!("EXTRACT from date with time zone is not supported")
            }
            "ZetaSQL:format_date" => binary(args, |a, b| F::FormatDate(a, b)),
            "ZetaSQL:format_timestamp" if args.len() == 2 => {
                binary(args, |a, b| F::FormatTimestamp(a, b))
            }
            "ZetaSQL:format_timestamp" if args.len() == 3 => {
                panic!("FORMAT_TIMESTAMP with time zone is not supported")
            }
            "ZetaSQL:parse_date" => binary(args, |a, b| F::ParseDate(a, b)),
            "ZetaSQL:parse_timestamp" => binary(args, |a, b| F::ParseTimestamp(a, b)),
            "ZetaSQL:abs" if returns == DataType::I64 => unary(args, |a| F::AbsInt64(a)),
            "ZetaSQL:abs" if returns == DataType::F64 => unary(args, |a| F::AbsDouble(a)),
            "ZetaSQL:sign" if returns == DataType::I64 => unary(args, |a| F::SignInt64(a)),
            "ZetaSQL:sign" if returns == DataType::F64 => unary(args, |a| F::SignDouble(a)),
            "ZetaSQL:round" if signature.argument.len() == 1 => unary(args, |a| F::RoundDouble(a)),
            "ZetaSQL:round" if signature.argument.len() == 2 => {
                binary(args, |a, b| F::RoundWithDigitsDouble(a, b))
            }
            "ZetaSQL:trunc" if signature.argument.len() == 1 => unary(args, |a| F::TruncDouble(a)),
            "ZetaSQL:trunc" if signature.argument.len() == 2 => {
                binary(args, |a, b| F::TruncWithDigitsDouble(a, b))
            }
            "ZetaSQL:ceil" => unary(args, |a| F::CeilDouble(a)),
            "ZetaSQL:floor" => unary(args, |a| F::FloorDouble(a)),
            "ZetaSQL:mod" => binary(args, |a, b| F::ModInt64(a, b)),
            "ZetaSQL:div" => binary(args, |a, b| F::DivInt64(a, b)),
            "ZetaSQL:is_inf" => unary(args, |a| F::IsInf(a)),
            "ZetaSQL:is_nan" => unary(args, |a| F::IsNan(a)),
            "ZetaSQL:greatest" => F::Greatest(args),
            "ZetaSQL:least" => F::Least(args),
            "ZetaSQL:sqrt" => unary(args, |a| F::SqrtDouble(a)),
            "ZetaSQL:pow" => binary(args, |a, b| F::PowDouble(a, b)),
            "ZetaSQL:exp" => unary(args, |a| F::ExpDouble(a)),
            "ZetaSQL:ln" => unary(args, |a| F::NaturalLogarithmDouble(a)),
            "ZetaSQL:log10" => unary(args, |a| F::DecimalLogarithmDouble(a)),
            "ZetaSQL:log" if args.len() == 1 => unary(args, |a| F::NaturalLogarithmDouble(a)),
            "ZetaSQL:log" if args.len() == 2 => binary(args, |a, b| F::LogarithmDouble(a, b)),
            "ZetaSQL:cos" => unary(args, |a| F::CosDouble(a)),
            "ZetaSQL:cosh" => unary(args, |a| F::CoshDouble(a)),
            "ZetaSQL:acos" => unary(args, |a| F::AcosDouble(a)),
            "ZetaSQL:acosh" => unary(args, |a| F::AcoshDouble(a)),
            "ZetaSQL:sin" => unary(args, |a| F::SinDouble(a)),
            "ZetaSQL:sinh" => unary(args, |a| F::SinhDouble(a)),
            "ZetaSQL:asin" => unary(args, |a| F::AsinDouble(a)),
            "ZetaSQL:asinh" => unary(args, |a| F::AsinhDouble(a)),
            "ZetaSQL:tan" => unary(args, |a| F::TanDouble(a)),
            "ZetaSQL:tanh" => unary(args, |a| F::TanhDouble(a)),
            "ZetaSQL:atan" => unary(args, |a| F::AtanDouble(a)),
            "ZetaSQL:atanh" => unary(args, |a| F::AtanhDouble(a)),
            "ZetaSQL:atan2" => binary(args, |a, b| F::Atan2Double(a, b)),
            "system:next_val" => unary(args, |a| F::NextVal(a)),
            other => panic!("{} is not a known function name", other),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            F::CurrentDate => "CurrentDate",
            F::CurrentTimestamp => "CurrentTimestamp",
            F::Xid => "Xid",
            F::Coalesce(_) => "Coalesce",
            F::ConcatString(_) => "ConcatString",
            F::Hash(_) => "Hash",
            F::Greatest(_) => "Greatest",
            F::Least(_) => "Least",
            F::AbsDouble(_) => "AbsDouble",
            F::AbsInt64(_) => "AbsInt64",
            F::AcosDouble(_) => "AcosDouble",
            F::AcoshDouble(_) => "AcoshDouble",
            F::AsinDouble(_) => "AsinDouble",
            F::AsinhDouble(_) => "AsinhDouble",
            F::AtanDouble(_) => "AtanDouble",
            F::AtanhDouble(_) => "AtanhDouble",
            F::ByteLengthString(_) => "ByteLengthString",
            F::CeilDouble(_) => "CeilDouble",
            F::CharLengthString(_) => "CharLengthString",
            F::ChrString(_) => "ChrString",
            F::CosDouble(_) => "CosDouble",
            F::CoshDouble(_) => "CoshDouble",
            F::DateFromTimestamp(_) => "DateFromTimestamp",
            F::DateFromUnixDate(_) => "DateFromUnixDate",
            F::DecimalLogarithmDouble(_) => "DecimalLogarithmDouble",
            F::ExpDouble(_) => "ExpDouble",
            F::ExtractDateFromTimestamp(_) => "ExtractDateFromTimestamp",
            F::FloorDouble(_) => "FloorDouble",
            F::IsFalse(_) => "IsFalse",
            F::IsInf(_) => "IsInf",
            F::IsNan(_) => "IsNan",
            F::IsNull(_) => "IsNull",
            F::IsTrue(_) => "IsTrue",
            F::LengthString(_) => "LengthString",
            F::LowerString(_) => "LowerString",
            F::NaturalLogarithmDouble(_) => "NaturalLogarithmDouble",
            F::NextVal(_) => "NextVal",
            F::Not(_) => "Not",
            F::ReverseString(_) => "ReverseString",
            F::RoundDouble(_) => "RoundDouble",
            F::SignDouble(_) => "SignDouble",
            F::SignInt64(_) => "SignInt64",
            F::SinDouble(_) => "SinDouble",
            F::SinhDouble(_) => "SinhDouble",
            F::SqrtDouble(_) => "SqrtDouble",
            F::StringFromDate(_) => "StringFromDate",
            F::StringFromTimestamp(_) => "StringFromTimestamp",
            F::TanDouble(_) => "TanDouble",
            F::TanhDouble(_) => "TanhDouble",
            F::TimestampFromDate(_) => "TimestampFromDate",
            F::TimestampFromString(_) => "TimestampFromString",
            F::TimestampFromUnixMicrosInt64(_) => "TimestampFromUnixMicrosInt64",
            F::TruncDouble(_) => "TruncDouble",
            F::UnaryMinusDouble(_) => "UnaryMinusDouble",
            F::UnaryMinusInt64(_) => "UnaryMinusInt64",
            F::UnixDate(_) => "UnixDate",
            F::UnixMicrosFromTimestamp(_) => "UnixMicrosFromTimestamp",
            F::UpperString(_) => "UpperString",
            F::DateTruncDate(_, _) => "DateTruncDate",
            F::ExtractFromDate(_, _) => "ExtractFromDate",
            F::ExtractFromTimestamp(_, _) => "ExtractFromTimestamp",
            F::TimestampTrunc(_, _) => "TimestampTrunc",
            F::In(_, _) => "In",
            F::AddDouble(_, _) => "AddDouble",
            F::AddInt64(_, _) => "AddInt64",
            F::And(_, _) => "And",
            F::Atan2Double(_, _) => "Atan2Double",
            F::DivideDouble(_, _) => "DivideDouble",
            F::DivInt64(_, _) => "DivInt64",
            F::EndsWithString(_, _) => "EndsWithString",
            F::Equal(_, _) => "Equal",
            F::FormatDate(_, _) => "FormatDate",
            F::FormatTimestamp(_, _) => "FormatTimestamp",
            F::Greater(_, _) => "Greater",
            F::GreaterOrEqual(_, _) => "GreaterOrEqual",
            F::Ifnull(_, _) => "Ifnull",
            F::Is(_, _) => "Is",
            F::LeftString(_, _) => "LeftString",
            F::Less(_, _) => "Less",
            F::LessOrEqual(_, _) => "LessOrEqual",
            F::LogarithmDouble(_, _) => "LogarithmDouble",
            F::LtrimString(_, _) => "LtrimString",
            F::ModInt64(_, _) => "ModInt64",
            F::MultiplyDouble(_, _) => "MultiplyDouble",
            F::MultiplyInt64(_, _) => "MultiplyInt64",
            F::NotEqual(_, _) => "NotEqual",
            F::Nullif(_, _) => "Nullif",
            F::Or(_, _) => "Or",
            F::ParseDate(_, _) => "ParseDate",
            F::ParseTimestamp(_, _) => "ParseTimestamp",
            F::PowDouble(_, _) => "PowDouble",
            F::RegexpContainsString(_, _) => "RegexpContainsString",
            F::RegexpExtractString(_, _) => "RegexpExtractString",
            F::RepeatString(_, _) => "RepeatString",
            F::RightString(_, _) => "RightString",
            F::RoundWithDigitsDouble(_, _) => "RoundWithDigitsDouble",
            F::RtrimString(_, _) => "RtrimString",
            F::StartsWithString(_, _) => "StartsWithString",
            F::StringLike(_, _) => "StringLike",
            F::StrposString(_, _) => "StrposString",
            F::SubtractDouble(_, _) => "SubtractDouble",
            F::SubtractInt64(_, _) => "SubtractInt64",
            F::TrimString(_, _) => "TrimString",
            F::TruncWithDigitsDouble(_, _) => "TruncWithDigitsDouble",
            F::DateAddDate(_, _, _) => "DateAddDate",
            F::DateDiffDate(_, _, _) => "DateDiffDate",
            F::DateSubDate(_, _, _) => "DateSubDate",
            F::TimestampAdd(_, _, _) => "TimestampAdd",
            F::TimestampDiff(_, _, _) => "TimestampDiff",
            F::TimestampSub(_, _, _) => "TimestampSub",
            F::Between(_, _, _) => "Between",
            F::CaseNoValue(_, _) => "CaseNoValue",
            F::DateFromYearMonthDay(_, _, _) => "DateFromYearMonthDay",
            F::If(_, _, _) => "If",
            F::LpadString(_, _, _) => "LpadString",
            F::RegexpReplaceString(_, _, _) => "RegexpReplaceString",
            F::ReplaceString(_, _, _) => "ReplaceString",
            F::RpadString(_, _, _) => "RpadString",
            F::SubstrString(_, _, _) => "SubstrString",
            F::CaseWithValue(_, _, _) => "CaseWithValue",
        }
    }

    pub fn arguments(&self) -> Vec<&Scalar> {
        match self {
            F::CurrentDate | F::CurrentTimestamp | F::Xid => vec![],
            F::Coalesce(varargs)
            | F::ConcatString(varargs)
            | F::Hash(varargs)
            | F::Greatest(varargs)
            | F::Least(varargs) => {
                let mut arguments = vec![];
                for a in varargs {
                    arguments.push(a);
                }
                arguments
            }
            F::AbsDouble(a)
            | F::AbsInt64(a)
            | F::AcosDouble(a)
            | F::AcoshDouble(a)
            | F::AsinDouble(a)
            | F::AsinhDouble(a)
            | F::AtanDouble(a)
            | F::AtanhDouble(a)
            | F::ByteLengthString(a)
            | F::CeilDouble(a)
            | F::CharLengthString(a)
            | F::ChrString(a)
            | F::CosDouble(a)
            | F::CoshDouble(a)
            | F::DateFromTimestamp(a)
            | F::DateFromUnixDate(a)
            | F::DecimalLogarithmDouble(a)
            | F::ExpDouble(a)
            | F::ExtractDateFromTimestamp(a)
            | F::FloorDouble(a)
            | F::IsFalse(a)
            | F::IsInf(a)
            | F::IsNan(a)
            | F::IsNull(a)
            | F::IsTrue(a)
            | F::LengthString(a)
            | F::LowerString(a)
            | F::NaturalLogarithmDouble(a)
            | F::NextVal(a)
            | F::Not(a)
            | F::ReverseString(a)
            | F::RoundDouble(a)
            | F::SignDouble(a)
            | F::SignInt64(a)
            | F::SinDouble(a)
            | F::SinhDouble(a)
            | F::SqrtDouble(a)
            | F::StringFromDate(a)
            | F::StringFromTimestamp(a)
            | F::TanDouble(a)
            | F::TanhDouble(a)
            | F::TimestampFromDate(a)
            | F::TimestampFromString(a)
            | F::TimestampFromUnixMicrosInt64(a)
            | F::TruncDouble(a)
            | F::UnaryMinusDouble(a)
            | F::UnaryMinusInt64(a)
            | F::UnixDate(a)
            | F::UnixMicrosFromTimestamp(a)
            | F::UpperString(a)
            | F::DateTruncDate(a, _)
            | F::ExtractFromDate(a, _)
            | F::ExtractFromTimestamp(a, _)
            | F::TimestampTrunc(a, _)
            | F::LtrimString(a, None)
            | F::RtrimString(a, None)
            | F::TrimString(a, None) => vec![a],
            F::In(a, varargs) => {
                let mut arguments = vec![a];
                for a in varargs {
                    arguments.push(a);
                }
                arguments
            }
            F::AddDouble(a, b)
            | F::AddInt64(a, b)
            | F::And(a, b)
            | F::Atan2Double(a, b)
            | F::DivideDouble(a, b)
            | F::DivInt64(a, b)
            | F::EndsWithString(a, b)
            | F::Equal(a, b)
            | F::FormatDate(a, b)
            | F::FormatTimestamp(a, b)
            | F::Greater(a, b)
            | F::GreaterOrEqual(a, b)
            | F::Ifnull(a, b)
            | F::Is(a, b)
            | F::LeftString(a, b)
            | F::Less(a, b)
            | F::LessOrEqual(a, b)
            | F::LogarithmDouble(a, b)
            | F::LtrimString(a, Some(b))
            | F::ModInt64(a, b)
            | F::MultiplyDouble(a, b)
            | F::MultiplyInt64(a, b)
            | F::NotEqual(a, b)
            | F::Nullif(a, b)
            | F::Or(a, b)
            | F::ParseDate(a, b)
            | F::ParseTimestamp(a, b)
            | F::PowDouble(a, b)
            | F::RegexpContainsString(a, b)
            | F::RegexpExtractString(a, b)
            | F::RepeatString(a, b)
            | F::RightString(a, b)
            | F::RoundWithDigitsDouble(a, b)
            | F::RtrimString(a, Some(b))
            | F::StartsWithString(a, b)
            | F::StringLike(a, b)
            | F::StrposString(a, b)
            | F::SubtractDouble(a, b)
            | F::SubtractInt64(a, b)
            | F::TrimString(a, Some(b))
            | F::TruncWithDigitsDouble(a, b)
            | F::DateAddDate(a, b, _)
            | F::DateDiffDate(a, b, _)
            | F::DateSubDate(a, b, _)
            | F::TimestampAdd(a, b, _)
            | F::TimestampDiff(a, b, _)
            | F::TimestampSub(a, b, _)
            | F::SubstrString(a, b, None) => vec![a, b],
            F::Between(a, b, c)
            | F::DateFromYearMonthDay(a, b, c)
            | F::If(a, b, c)
            | F::LpadString(a, b, c)
            | F::RegexpReplaceString(a, b, c)
            | F::ReplaceString(a, b, c)
            | F::RpadString(a, b, c)
            | F::SubstrString(a, b, Some(c)) => vec![a, b, c],
            F::CaseNoValue(cases, default) => {
                let mut arguments = vec![];
                for (a, b) in cases {
                    arguments.push(a);
                    arguments.push(b);
                }
                arguments.push(default);
                arguments
            }
            F::CaseWithValue(value, cases, default) => {
                let mut arguments = vec![value];
                for (a, b) in cases {
                    arguments.push(a);
                    arguments.push(b);
                }
                arguments.push(default);
                arguments
            }
        }
    }

    pub fn returns(&self) -> DataType {
        match self {
            F::CaseNoValue(_, scalar)
            | F::CaseWithValue(_, _, scalar)
            | F::If(_, scalar, _)
            | F::Ifnull(scalar, _)
            | F::Nullif(scalar, _) => scalar.data_type(),
            F::Coalesce(varargs) | F::Greatest(varargs) | F::Least(varargs) => {
                varargs[0].data_type()
            }
            F::And { .. }
            | F::Between { .. }
            | F::EndsWithString { .. }
            | F::Equal { .. }
            | F::Greater { .. }
            | F::GreaterOrEqual { .. }
            | F::In { .. }
            | F::Is { .. }
            | F::IsFalse { .. }
            | F::IsInf { .. }
            | F::IsNan { .. }
            | F::IsNull { .. }
            | F::IsTrue { .. }
            | F::Less { .. }
            | F::LessOrEqual { .. }
            | F::Not { .. }
            | F::NotEqual { .. }
            | F::Or { .. }
            | F::RegexpContainsString { .. }
            | F::StartsWithString { .. }
            | F::StringLike { .. } => DataType::Bool,
            F::CurrentDate { .. }
            | F::DateAddDate { .. }
            | F::DateFromTimestamp { .. }
            | F::DateFromUnixDate { .. }
            | F::DateFromYearMonthDay { .. }
            | F::DateSubDate { .. }
            | F::DateTruncDate { .. }
            | F::ExtractDateFromTimestamp { .. }
            | F::ParseDate { .. } => DataType::Date,
            F::AbsDouble { .. }
            | F::AcosDouble { .. }
            | F::AcoshDouble { .. }
            | F::AddDouble { .. }
            | F::AsinDouble { .. }
            | F::AsinhDouble { .. }
            | F::Atan2Double { .. }
            | F::AtanDouble { .. }
            | F::AtanhDouble { .. }
            | F::CeilDouble { .. }
            | F::CosDouble { .. }
            | F::CoshDouble { .. }
            | F::DecimalLogarithmDouble { .. }
            | F::DivideDouble { .. }
            | F::ExpDouble { .. }
            | F::FloorDouble { .. }
            | F::LogarithmDouble { .. }
            | F::MultiplyDouble { .. }
            | F::NaturalLogarithmDouble { .. }
            | F::PowDouble { .. }
            | F::RoundDouble { .. }
            | F::RoundWithDigitsDouble { .. }
            | F::SignDouble { .. }
            | F::SinDouble { .. }
            | F::SinhDouble { .. }
            | F::SqrtDouble { .. }
            | F::SubtractDouble { .. }
            | F::TanDouble { .. }
            | F::TanhDouble { .. }
            | F::TruncDouble { .. }
            | F::TruncWithDigitsDouble { .. }
            | F::UnaryMinusDouble { .. } => DataType::F64,
            F::AbsInt64 { .. }
            | F::AddInt64 { .. }
            | F::ByteLengthString { .. }
            | F::CharLengthString { .. }
            | F::DateDiffDate { .. }
            | F::DivInt64 { .. }
            | F::ExtractFromDate { .. }
            | F::ExtractFromTimestamp { .. }
            | F::Hash { .. }
            | F::LengthString { .. }
            | F::ModInt64 { .. }
            | F::MultiplyInt64 { .. }
            | F::NextVal { .. }
            | F::SignInt64 { .. }
            | F::StrposString { .. }
            | F::SubtractInt64 { .. }
            | F::TimestampDiff { .. }
            | F::UnaryMinusInt64 { .. }
            | F::UnixDate { .. }
            | F::UnixMicrosFromTimestamp { .. }
            | F::Xid { .. } => DataType::I64,
            F::ChrString { .. }
            | F::ConcatString { .. }
            | F::FormatDate { .. }
            | F::FormatTimestamp { .. }
            | F::LeftString { .. }
            | F::LowerString { .. }
            | F::LpadString { .. }
            | F::LtrimString { .. }
            | F::RegexpExtractString { .. }
            | F::RegexpReplaceString { .. }
            | F::RepeatString { .. }
            | F::ReplaceString { .. }
            | F::ReverseString { .. }
            | F::RightString { .. }
            | F::RpadString { .. }
            | F::RtrimString { .. }
            | F::StringFromDate { .. }
            | F::StringFromTimestamp { .. }
            | F::SubstrString { .. }
            | F::TrimString { .. }
            | F::UpperString { .. } => DataType::String,
            F::CurrentTimestamp { .. }
            | F::ParseTimestamp { .. }
            | F::TimestampAdd { .. }
            | F::TimestampFromDate { .. }
            | F::TimestampFromString { .. }
            | F::TimestampFromUnixMicrosInt64 { .. }
            | F::TimestampSub { .. }
            | F::TimestampTrunc { .. } => DataType::Timestamp,
        }
    }

    pub fn map(self, f: impl Fn(Scalar) -> Scalar) -> Self {
        match self {
            F::CurrentDate => F::CurrentDate,
            F::CurrentTimestamp => F::CurrentTimestamp,
            F::Xid => F::Xid,
            F::Coalesce(mut varargs) => F::Coalesce(varargs.drain(..).map(f).collect()),
            F::ConcatString(mut varargs) => F::ConcatString(varargs.drain(..).map(f).collect()),
            F::Hash(mut varargs) => F::Hash(varargs.drain(..).map(f).collect()),
            F::Greatest(mut varargs) => F::Greatest(varargs.drain(..).map(f).collect()),
            F::Least(mut varargs) => F::Least(varargs.drain(..).map(f).collect()),
            F::In(a, mut varargs) => F::In(f(a), varargs.drain(..).map(f).collect()),
            F::CaseNoValue(mut cases, default) => F::CaseNoValue(
                cases.drain(..).map(|(a, b)| (f(a), f(b))).collect(),
                f(default),
            ),
            F::CaseWithValue(value, mut cases, default) => F::CaseWithValue(
                f(value),
                cases.drain(..).map(|(a, b)| (f(a), f(b))).collect(),
                f(default),
            ),
            F::AbsDouble(a) => F::AbsDouble(f(a)),
            F::AbsInt64(a) => F::AbsInt64(f(a)),
            F::AcosDouble(a) => F::AcosDouble(f(a)),
            F::AcoshDouble(a) => F::AcoshDouble(f(a)),
            F::AsinDouble(a) => F::AsinDouble(f(a)),
            F::AsinhDouble(a) => F::AsinhDouble(f(a)),
            F::AtanDouble(a) => F::AtanDouble(f(a)),
            F::AtanhDouble(a) => F::AtanhDouble(f(a)),
            F::ByteLengthString(a) => F::ByteLengthString(f(a)),
            F::CeilDouble(a) => F::CeilDouble(f(a)),
            F::CharLengthString(a) => F::CharLengthString(f(a)),
            F::ChrString(a) => F::ChrString(f(a)),
            F::CosDouble(a) => F::CosDouble(f(a)),
            F::CoshDouble(a) => F::CoshDouble(f(a)),
            F::DateFromTimestamp(a) => F::DateFromTimestamp(f(a)),
            F::DateFromUnixDate(a) => F::DateFromUnixDate(f(a)),
            F::DecimalLogarithmDouble(a) => F::DecimalLogarithmDouble(f(a)),
            F::ExpDouble(a) => F::ExpDouble(f(a)),
            F::ExtractDateFromTimestamp(a) => F::ExtractDateFromTimestamp(f(a)),
            F::FloorDouble(a) => F::FloorDouble(f(a)),
            F::IsFalse(a) => F::IsFalse(f(a)),
            F::IsInf(a) => F::IsInf(f(a)),
            F::IsNan(a) => F::IsNan(f(a)),
            F::IsNull(a) => F::IsNull(f(a)),
            F::IsTrue(a) => F::IsTrue(f(a)),
            F::LengthString(a) => F::LengthString(f(a)),
            F::LowerString(a) => F::LowerString(f(a)),
            F::NaturalLogarithmDouble(a) => F::NaturalLogarithmDouble(f(a)),
            F::NextVal(a) => F::NextVal(f(a)),
            F::Not(a) => F::Not(f(a)),
            F::ReverseString(a) => F::ReverseString(f(a)),
            F::RoundDouble(a) => F::RoundDouble(f(a)),
            F::SignDouble(a) => F::SignDouble(f(a)),
            F::SignInt64(a) => F::SignInt64(f(a)),
            F::SinDouble(a) => F::SinDouble(f(a)),
            F::SinhDouble(a) => F::SinhDouble(f(a)),
            F::SqrtDouble(a) => F::SqrtDouble(f(a)),
            F::StringFromDate(a) => F::StringFromDate(f(a)),
            F::StringFromTimestamp(a) => F::StringFromTimestamp(f(a)),
            F::TanDouble(a) => F::TanDouble(f(a)),
            F::TanhDouble(a) => F::TanhDouble(f(a)),
            F::TimestampFromDate(a) => F::TimestampFromDate(f(a)),
            F::TimestampFromString(a) => F::TimestampFromString(f(a)),
            F::TimestampFromUnixMicrosInt64(a) => F::TimestampFromUnixMicrosInt64(f(a)),
            F::TruncDouble(a) => F::TruncDouble(f(a)),
            F::UnaryMinusDouble(a) => F::UnaryMinusDouble(f(a)),
            F::UnaryMinusInt64(a) => F::UnaryMinusInt64(f(a)),
            F::UnixDate(a) => F::UnixDate(f(a)),
            F::UnixMicrosFromTimestamp(a) => F::UnixMicrosFromTimestamp(f(a)),
            F::UpperString(a) => F::UpperString(f(a)),
            F::DateTruncDate(a, date_part) => F::DateTruncDate(f(a), date_part),
            F::ExtractFromDate(a, date_part) => F::ExtractFromDate(f(a), date_part),
            F::ExtractFromTimestamp(a, date_part) => F::ExtractFromTimestamp(f(a), date_part),
            F::TimestampTrunc(a, date_part) => F::TimestampTrunc(f(a), date_part),
            F::AddDouble(a, b) => F::AddDouble(f(a), f(b)),
            F::AddInt64(a, b) => F::AddInt64(f(a), f(b)),
            F::And(a, b) => F::And(f(a), f(b)),
            F::Atan2Double(a, b) => F::Atan2Double(f(a), f(b)),
            F::DivideDouble(a, b) => F::DivideDouble(f(a), f(b)),
            F::DivInt64(a, b) => F::DivInt64(f(a), f(b)),
            F::EndsWithString(a, b) => F::EndsWithString(f(a), f(b)),
            F::Equal(a, b) => F::Equal(f(a), f(b)),
            F::FormatDate(a, b) => F::FormatDate(f(a), f(b)),
            F::FormatTimestamp(a, b) => F::FormatTimestamp(f(a), f(b)),
            F::Greater(a, b) => F::Greater(f(a), f(b)),
            F::GreaterOrEqual(a, b) => F::GreaterOrEqual(f(a), f(b)),
            F::Ifnull(a, b) => F::Ifnull(f(a), f(b)),
            F::Is(a, b) => F::Is(f(a), f(b)),
            F::LeftString(a, b) => F::LeftString(f(a), f(b)),
            F::Less(a, b) => F::Less(f(a), f(b)),
            F::LessOrEqual(a, b) => F::LessOrEqual(f(a), f(b)),
            F::LogarithmDouble(a, b) => F::LogarithmDouble(f(a), f(b)),
            F::LtrimString(a, b) => F::LtrimString(f(a), b.map(f)),
            F::ModInt64(a, b) => F::ModInt64(f(a), f(b)),
            F::MultiplyDouble(a, b) => F::MultiplyDouble(f(a), f(b)),
            F::MultiplyInt64(a, b) => F::MultiplyInt64(f(a), f(b)),
            F::NotEqual(a, b) => F::NotEqual(f(a), f(b)),
            F::Nullif(a, b) => F::Nullif(f(a), f(b)),
            F::Or(a, b) => F::Or(f(a), f(b)),
            F::ParseDate(a, b) => F::ParseDate(f(a), f(b)),
            F::ParseTimestamp(a, b) => F::ParseTimestamp(f(a), f(b)),
            F::PowDouble(a, b) => F::PowDouble(f(a), f(b)),
            F::RegexpContainsString(a, b) => F::RegexpContainsString(f(a), f(b)),
            F::RegexpExtractString(a, b) => F::RegexpExtractString(f(a), f(b)),
            F::RepeatString(a, b) => F::RepeatString(f(a), f(b)),
            F::RightString(a, b) => F::RightString(f(a), f(b)),
            F::RoundWithDigitsDouble(a, b) => F::RoundWithDigitsDouble(f(a), f(b)),
            F::RtrimString(a, b) => F::RtrimString(f(a), b.map(f)),
            F::StartsWithString(a, b) => F::StartsWithString(f(a), f(b)),
            F::StringLike(a, b) => F::StringLike(f(a), f(b)),
            F::StrposString(a, b) => F::StrposString(f(a), f(b)),
            F::SubtractDouble(a, b) => F::SubtractDouble(f(a), f(b)),
            F::SubtractInt64(a, b) => F::SubtractInt64(f(a), f(b)),
            F::TrimString(a, b) => F::TrimString(f(a), b.map(f)),
            F::TruncWithDigitsDouble(a, b) => F::TruncWithDigitsDouble(f(a), f(b)),
            F::DateAddDate(a, b, date_part) => F::DateAddDate(f(a), f(b), date_part),
            F::DateDiffDate(a, b, date_part) => F::DateDiffDate(f(a), f(b), date_part),
            F::DateSubDate(a, b, date_part) => F::DateSubDate(f(a), f(b), date_part),
            F::TimestampAdd(a, b, date_part) => F::TimestampAdd(f(a), f(b), date_part),
            F::TimestampDiff(a, b, date_part) => F::TimestampDiff(f(a), f(b), date_part),
            F::TimestampSub(a, b, date_part) => F::TimestampSub(f(a), f(b), date_part),
            F::Between(a, b, c) => F::Between(f(a), f(b), f(c)),
            F::DateFromYearMonthDay(a, b, c) => F::DateFromYearMonthDay(f(a), f(b), f(c)),
            F::If(a, b, c) => F::If(f(a), f(b), f(c)),
            F::LpadString(a, b, c) => F::LpadString(f(a), f(b), f(c)),
            F::RegexpReplaceString(a, b, c) => F::RegexpReplaceString(f(a), f(b), f(c)),
            F::ReplaceString(a, b, c) => F::ReplaceString(f(a), f(b), f(c)),
            F::RpadString(a, b, c) => F::RpadString(f(a), f(b), f(c)),
            F::SubstrString(a, b, c) => F::SubstrString(f(a), f(b), c.map(f)),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct AggregateExpr {
    pub function: AggregateFunction,
    pub distinct: bool,
    pub input: Column,
    pub output: Column,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum AggregateFunction {
    AnyValue,
    Count,
    LogicalAnd,
    LogicalOr,
    Max,
    Min,
    Sum,
}

impl AggregateFunction {
    pub fn from(name: &str) -> Self {
        match name {
            "ZetaSQL:any_value" => AggregateFunction::AnyValue,
            "ZetaSQL:avg" => panic!("avg should be converted into sum / count"),
            "ZetaSQL:count" => AggregateFunction::Count,
            "ZetaSQL:logical_and" => AggregateFunction::LogicalAnd,
            "ZetaSQL:logical_or" => AggregateFunction::LogicalOr,
            "ZetaSQL:max" => AggregateFunction::Max,
            "ZetaSQL:min" => AggregateFunction::Min,
            "ZetaSQL:sum" => AggregateFunction::Sum,
            _ => panic!("{} is not supported", name),
        }
    }

    pub fn data_type(&self, column_type: DataType) -> DataType {
        match self {
            AggregateFunction::AnyValue
            | AggregateFunction::Max
            | AggregateFunction::Min
            | AggregateFunction::Sum => column_type.clone(),
            AggregateFunction::Count => DataType::I64,
            AggregateFunction::LogicalAnd | AggregateFunction::LogicalOr => DataType::Bool,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Procedure {
    CreateTable(Scalar),
    DropTable(Scalar),
    CreateIndex(Scalar),
    DropIndex(Scalar),
}

impl Procedure {
    fn collect_references(&self, set: &mut HashSet<Column>) {
        match self {
            Procedure::CreateTable(argument)
            | Procedure::DropTable(argument)
            | Procedure::CreateIndex(argument)
            | Procedure::DropIndex(argument) => argument.collect_references(set),
        }
    }
}

impl fmt::Display for Procedure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Procedure::CreateTable(id) => write!(f, "create_table {}", id),
            Procedure::DropTable(id) => write!(f, "drop_table {}", id),
            Procedure::CreateIndex(id) => write!(f, "create_index {}", id),
            Procedure::DropIndex(id) => write!(f, "drop_index {}", id),
        }
    }
}

pub struct PreOrderTraversal<'it> {
    deferred: Vec<&'it Expr>,
}

impl<'it> Iterator for PreOrderTraversal<'it> {
    type Item = &'it Expr;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.deferred.pop()?;
        for i in 0..next.len() {
            self.deferred.push(&next[next.len() - 1 - i]);
        }
        Some(next)
    }
}
