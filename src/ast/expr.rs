use crate::column::Column;
use crate::values::*;
use catalog::Index;
use kernel::*;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::ops;

// Expr plan nodes combine inputs in a Plan tree.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Expr {
    Leaf {
        gid: usize,
    },
    // LogicalSingleGet acts as the input to a SELECT with no FROM clause.
    LogicalSingleGet,
    // LogicalGet { table } implements the FROM clause.
    LogicalGet {
        projects: Vec<Column>,
        // TODO pushing predicates into scan operators is probably not worth it,
        // because there is no benefit to actually implementing predicate pushdown in the execution layer.
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
        aggregate: Vec<(AggregateFn, Column, Column)>,
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
    LogicalRewrite {
        sql: String,
    },
    TableFreeScan,
    SeqScan {
        projects: Vec<Column>,
        // TODO pushing predicates into scan operators is probably not worth it,
        // because there is no benefit to actually implementing predicate pushdown in the execution layer.
        predicates: Vec<Scalar>,
        table: Table,
    },
    IndexScan {
        include_existing: bool,
        projects: Vec<Column>,
        // TODO pushing predicates into scan operators is probably not worth it,
        // because there is no benefit to actually implementing predicate pushdown in the execution layer.
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
        join: Join,
        partition_left: Vec<Scalar>,
        partition_right: Vec<Scalar>,
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
        group_by: Vec<Column>,
        aggregate: Vec<(AggregateFn, Column, Column)>,
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
    Insert {
        table: Table,
        indexes: Vec<Index>,
        input: Box<Expr>,
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
            | Expr::LogicalCall { .. } => true,
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
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Delete { .. }
            | Expr::Script { .. }
            | Expr::Assign { .. }
            | Expr::Call { .. } => false,
        }
    }

    pub fn is_leaf(&self) -> bool {
        match self {
            Expr::Leaf { .. } => true,
            _ => false,
        }
    }

    pub fn has_side_effects(&self) -> bool {
        match self {
            Expr::LogicalInsert { .. }
            | Expr::LogicalUpdate { .. }
            | Expr::LogicalDelete { .. }
            | Expr::LogicalCreateDatabase { .. }
            | Expr::LogicalCreateTable { .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalDrop { .. } => true,
            Expr::Leaf { .. }
            | Expr::LogicalSingleGet { .. }
            | Expr::LogicalGet { .. }
            | Expr::LogicalFilter { .. }
            | Expr::LogicalOut { .. }
            | Expr::LogicalMap { .. }
            | Expr::LogicalJoin { .. }
            | Expr::LogicalDependentJoin { .. }
            | Expr::LogicalWith { .. }
            | Expr::LogicalCreateTempTable { .. }
            | Expr::LogicalGetWith { .. }
            | Expr::LogicalAggregate { .. }
            | Expr::LogicalLimit { .. }
            | Expr::LogicalSort { .. }
            | Expr::LogicalUnion { .. }
            | Expr::LogicalValues { .. }
            | Expr::LogicalScript { .. }
            | Expr::LogicalAssign { .. }
            | Expr::LogicalCall { .. }
            | Expr::LogicalRewrite { .. } => false,
            Expr::TableFreeScan
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
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Delete { .. }
            | Expr::Script { .. }
            | Expr::Assign { .. }
            | Expr::Call { .. } => panic!("{} is a physical operator", self.name()),
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
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Delete { .. }
            | Expr::LogicalCreateTempTable { .. }
            | Expr::CreateTempTable { .. }
            | Expr::LogicalAssign { .. }
            | Expr::Assign { .. }
            | Expr::LogicalCall { .. }
            | Expr::Call { .. } => 1,
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

    pub fn map(self, mut visitor: impl FnMut(Expr) -> Expr) -> Expr {
        match self {
            Expr::LogicalFilter { predicates, input } => {
                let input = Box::new(visitor(*input));
                Expr::LogicalFilter { predicates, input }
            }
            Expr::LogicalOut { projects, input } => Expr::LogicalOut {
                projects,
                input: Box::new(visitor(*input)),
            },
            Expr::LogicalMap {
                include_existing,
                projects,
                input,
            } => {
                let input = Box::new(visitor(*input));
                Expr::LogicalMap {
                    include_existing,
                    projects,
                    input,
                }
            }
            Expr::LogicalJoin { join, left, right } => {
                let left = Box::new(visitor(*left));
                let right = Box::new(visitor(*right));
                Expr::LogicalJoin { join, left, right }
            }
            Expr::LogicalDependentJoin {
                parameters,
                predicates,
                subquery,
                domain,
            } => {
                let subquery = Box::new(visitor(*subquery));
                let domain = Box::new(visitor(*domain));
                Expr::LogicalDependentJoin {
                    parameters,
                    predicates,
                    subquery,
                    domain,
                }
            }
            Expr::LogicalWith {
                name,
                columns,
                left,
                right,
            } => {
                let left = Box::new(visitor(*left));
                let right = Box::new(visitor(*right));
                Expr::LogicalWith {
                    name,
                    columns,
                    left,
                    right,
                }
            }
            Expr::LogicalCreateTempTable {
                name,
                columns,
                input,
            } => {
                let input = Box::new(visitor(*input));
                Expr::LogicalCreateTempTable {
                    name,
                    columns,
                    input,
                }
            }
            Expr::LogicalAggregate {
                group_by,
                aggregate,
                input,
            } => {
                let input = Box::new(visitor(*input));
                Expr::LogicalAggregate {
                    group_by,
                    aggregate,
                    input,
                }
            }
            Expr::LogicalLimit {
                limit,
                offset,
                input,
            } => {
                let input = Box::new(visitor(*input));
                Expr::LogicalLimit {
                    limit,
                    offset,
                    input,
                }
            }
            Expr::LogicalSort { order_by, input } => {
                let input = Box::new(visitor(*input));
                Expr::LogicalSort { order_by, input }
            }
            Expr::LogicalUnion { left, right } => {
                let left = Box::new(visitor(*left));
                let right = Box::new(visitor(*right));
                Expr::LogicalUnion { left, right }
            }
            Expr::LogicalInsert { table, input } => {
                let input = Box::new(visitor(*input));
                Expr::LogicalInsert { table, input }
            }
            Expr::LogicalValues {
                columns,
                values,
                input,
            } => {
                let input = Box::new(visitor(*input));
                Expr::LogicalValues {
                    columns,
                    values,
                    input,
                }
            }
            Expr::LogicalUpdate { table, tid, input } => {
                let input = Box::new(visitor(*input));
                Expr::LogicalUpdate { table, tid, input }
            }
            Expr::LogicalDelete { table, tid, input } => Expr::LogicalDelete {
                table,
                tid,
                input: Box::new(visitor(*input)),
            },
            Expr::Filter { predicates, input } => Expr::Filter {
                predicates,
                input: Box::new(visitor(*input)),
            },
            Expr::Out { projects, input } => Expr::Out {
                projects,
                input: Box::new(visitor(*input)),
            },
            Expr::Map {
                include_existing,
                projects,
                input,
            } => Expr::Map {
                include_existing,
                projects,
                input: Box::new(visitor(*input)),
            },
            Expr::NestedLoop { join, left, right } => Expr::NestedLoop {
                join,
                left: Box::new(visitor(*left)),
                right: Box::new(visitor(*right)),
            },
            Expr::HashJoin {
                join,
                partition_left,
                partition_right,
                left,
                right,
            } => Expr::HashJoin {
                join,
                partition_left,
                partition_right,
                left: Box::new(visitor(*left)),
                right: Box::new(visitor(*right)),
            },
            Expr::CreateTempTable {
                name,
                columns,
                input,
            } => Expr::CreateTempTable {
                name,
                columns,
                input: Box::new(visitor(*input)),
            },
            Expr::GetTempTable { name, columns } => Expr::GetTempTable { name, columns },
            Expr::IndexScan {
                include_existing,
                projects,
                predicates,
                lookup,
                index,
                table,
                input,
            } => Expr::IndexScan {
                include_existing,
                projects,
                predicates,
                lookup,
                index,
                table,
                input: Box::new(visitor(*input)),
            },
            Expr::Aggregate {
                group_by,
                aggregate,
                input,
            } => Expr::Aggregate {
                group_by,
                aggregate,
                input: Box::new(visitor(*input)),
            },
            Expr::Limit {
                limit,
                offset,
                input,
            } => Expr::Limit {
                limit,
                offset,
                input: Box::new(visitor(*input)),
            },
            Expr::Sort { order_by, input } => Expr::Sort {
                order_by,
                input: Box::new(visitor(*input)),
            },
            Expr::Union { left, right } => Expr::Union {
                left: Box::new(visitor(*left)),
                right: Box::new(visitor(*right)),
            },
            Expr::Insert {
                table,
                indexes,
                input,
            } => Expr::Insert {
                table,
                indexes,
                input: Box::new(visitor(*input)),
            },
            Expr::Values {
                columns,
                values,
                input,
            } => Expr::Values {
                columns,
                values,
                input: Box::new(visitor(*input)),
            },
            Expr::Delete { table, tid, input } => Expr::Delete {
                table,
                tid,
                input: Box::new(visitor(*input)),
            },
            Expr::LogicalScript { statements } => Expr::LogicalScript {
                statements: map(visitor, statements),
            },
            Expr::Script { statements } => Expr::Script {
                statements: map(visitor, statements),
            },
            Expr::LogicalAssign {
                variable,
                value,
                input,
            } => Expr::LogicalAssign {
                variable,
                value,
                input: Box::new(visitor(*input)),
            },
            Expr::Assign {
                variable,
                value,
                input,
            } => Expr::Assign {
                variable,
                value,
                input: Box::new(visitor(*input)),
            },
            Expr::LogicalCall { procedure, input } => Expr::LogicalCall {
                procedure,
                input: Box::new(visitor(*input)),
            },
            Expr::Call { procedure, input } => Expr::Call {
                procedure,
                input: Box::new(visitor(*input)),
            },
            Expr::Leaf { .. }
            | Expr::LogicalSingleGet { .. }
            | Expr::LogicalGet { .. }
            | Expr::LogicalGetWith { .. }
            | Expr::LogicalCreateDatabase { .. }
            | Expr::LogicalCreateTable { .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalDrop { .. }
            | Expr::LogicalRewrite { .. }
            | Expr::TableFreeScan { .. }
            | Expr::SeqScan { .. } => self,
        }
    }

    pub fn bottom_up_rewrite(self, visitor: &mut impl FnMut(Expr) -> Expr) -> Expr {
        let operator = self.map(|child| child.bottom_up_rewrite(visitor));
        visitor(operator)
    }

    pub fn top_down_rewrite(self, visitor: &mut impl FnMut(Expr) -> Expr) -> Expr {
        let expr = visitor(self);
        expr.map(|child| child.top_down_rewrite(visitor))
    }

    pub fn iter(&self) -> ExprIterator {
        ExprIterator {
            parent: self,
            offset: 0,
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
            | Expr::LogicalCall { input, .. } => input.attributes(),
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
                for (_, _, c) in aggregate {
                    set.insert(c.clone());
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
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Delete { .. }
            | Expr::Script { .. }
            | Expr::Assign { .. }
            | Expr::Call { .. } => panic!(
                "attributes is not implemented for physical operator {}",
                &self.name()
            ),
        }
    }

    pub fn references(&self) -> HashSet<Column> {
        let mut set = HashSet::new();
        match self {
            Expr::LogicalGet { predicates, .. } | Expr::LogicalFilter { predicates, .. } => {
                for p in predicates {
                    set.extend(p.references());
                }
            }
            Expr::LogicalMap { projects, .. } => {
                for (x, _) in projects {
                    set.extend(x.references());
                }
            }
            Expr::LogicalJoin { join, .. } => {
                for p in join.predicates() {
                    set.extend(p.references());
                }
            }
            Expr::LogicalDependentJoin { predicates, .. } => {
                for p in predicates {
                    set.extend(p.references());
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
                for (f, c, _) in aggregate {
                    set.insert(c.clone());
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
                        set.extend(x.references());
                    }
                }
            }
            Expr::LogicalAssign { value, .. } => {
                set.extend(value.references());
            }
            Expr::LogicalCall { procedure, .. } => {
                set.extend(procedure.references());
            }
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
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Delete { .. }
            | Expr::Script { .. }
            | Expr::Assign { .. }
            | Expr::Call { .. } => unimplemented!(
                "free is not implemented for physical operator {}",
                self.name()
            ),
        }
        for i in 0..self.len() {
            set.extend(self[i].references());
        }
        set
    }

    pub fn subst(self, map: &HashMap<Column, Column>) -> Self {
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
                table,
            } => Expr::LogicalGet {
                projects: projects.iter().map(subst_c).collect(),
                predicates: predicates.iter().map(subst_x).collect(),
                table: table.clone(),
            },
            Expr::LogicalFilter { predicates, input } => Expr::LogicalFilter {
                predicates: predicates.iter().map(subst_x).collect(),
                input: Box::new(input.subst(map)),
            },
            Expr::LogicalMap {
                include_existing,
                projects,
                input,
            } => Expr::LogicalMap {
                include_existing,
                projects: projects
                    .iter()
                    .map(|(x, c)| (subst_x(x), subst_c(c)))
                    .collect(),
                input: Box::new(input.subst(map)),
            },
            Expr::LogicalJoin { join, left, right } => Expr::LogicalJoin {
                join: join.replace(join.predicates().iter().map(subst_x).collect()),
                left: Box::new(left.subst(map)),
                right: Box::new(right.subst(map)),
            },
            Expr::LogicalDependentJoin {
                parameters,
                predicates,
                subquery,
                domain,
            } => Expr::LogicalDependentJoin {
                parameters: parameters.iter().map(subst_c).collect(),
                predicates: predicates.iter().map(subst_x).collect(),
                subquery: Box::new(subquery.subst(map)),
                domain: Box::new(domain.subst(map)),
            },
            Expr::LogicalWith {
                name,
                columns,
                left,
                right,
            } => Expr::LogicalWith {
                name: name.clone(),
                columns: columns.iter().map(subst_c).collect(),
                left: Box::new(left.subst(map)),
                right: Box::new(right.subst(map)),
            },
            Expr::LogicalCreateTempTable {
                name,
                columns,
                input,
            } => Expr::LogicalCreateTempTable {
                name: name.clone(),
                columns: columns.iter().map(subst_c).collect(),
                input: Box::new(input.subst(map)),
            },
            Expr::LogicalGetWith { name, columns } => Expr::LogicalGetWith {
                name: name.clone(),
                columns: columns.iter().map(subst_c).collect(),
            },
            Expr::LogicalAggregate {
                group_by,
                aggregate,
                input,
            } => Expr::LogicalAggregate {
                group_by: group_by.iter().map(subst_c).collect(),
                aggregate: aggregate
                    .iter()
                    .map(|(f, c_in, c_out)| (f.clone(), subst_c(c_in), subst_c(c_out)))
                    .collect(),
                input: Box::new(input.subst(map)),
            },
            Expr::LogicalSort { order_by, input } => Expr::LogicalSort {
                order_by: order_by.iter().map(subst_o).collect(),
                input: Box::new(input.subst(map)),
            },
            Expr::LogicalValues {
                columns,
                values,
                input,
            } => Expr::LogicalValues {
                columns: columns.iter().map(subst_c).collect(),
                values: values
                    .iter()
                    .map(|row| row.iter().map(subst_x).collect())
                    .collect(),
                input: Box::new(input.subst(map)),
            },
            Expr::LogicalAssign {
                variable,
                value,
                input,
            } => Expr::LogicalAssign {
                variable,
                value,
                input: Box::new(input.subst(map)),
            },
            Expr::LogicalCall { procedure, input } => Expr::LogicalCall {
                procedure,
                input: Box::new(input.subst(map)),
            },
            Expr::LogicalOut { projects, input } => Expr::LogicalOut {
                projects: projects.iter().map(subst_c).collect(),
                input: Box::new(input.subst(map)),
            },
            Expr::LogicalUpdate { table, tid, input } => Expr::LogicalUpdate {
                table,
                tid: subst_c(&tid),
                input: Box::new(input.subst(map)),
            },
            Expr::LogicalDelete { table, tid, input } => Expr::LogicalDelete {
                table,
                tid: subst_c(&tid),
                input: Box::new(input.subst(map)),
            },
            Expr::Leaf { .. }
            | Expr::LogicalSingleGet { .. }
            | Expr::LogicalLimit { .. }
            | Expr::LogicalUnion { .. }
            | Expr::LogicalInsert { .. }
            | Expr::LogicalCreateDatabase { .. }
            | Expr::LogicalCreateTable { .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalDrop { .. }
            | Expr::LogicalScript { .. }
            | Expr::LogicalRewrite { .. } => self.map(|child| child.subst(map)),
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
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Delete { .. }
            | Expr::Script { .. }
            | Expr::Assign { .. }
            | Expr::Call { .. } => panic!(
                "subst is not implemented for physical operator {}",
                self.name()
            ),
        }
    }
}

impl ops::Index<usize> for Expr {
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
            | Expr::Insert { input, .. }
            | Expr::Values { input, .. }
            | Expr::Delete { input, .. }
            | Expr::LogicalCreateTempTable { input, .. }
            | Expr::CreateTempTable { input, .. }
            | Expr::LogicalAssign { input, .. }
            | Expr::Assign { input, .. }
            | Expr::LogicalCall { input, .. }
            | Expr::Call { input, .. } => match index {
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

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
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
            Join::Inner { .. } => Join::Inner(predicates),
            Join::Right { .. } => Join::Right(predicates),
            Join::Outer { .. } => Join::Outer(predicates),
            Join::Semi { .. } => Join::Semi(predicates),
            Join::Anti { .. } => Join::Anti(predicates),
            Join::Single { .. } => Join::Single(predicates),
            Join::Mark(mark, _) => Join::Mark(mark.clone(), predicates),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
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

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Name {
    pub catalog_id: i64,
    pub path: Vec<String>,
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path.join("."))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
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

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
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

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Scalar {
    Literal(Value),
    Column(Column),
    Parameter(String, DataType),
    Call(Box<Function>),
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

    pub fn references(&self) -> HashSet<Column> {
        let mut free = HashSet::new();
        self.collect_references(&mut free);
        free
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

// Functions appear in scalar expressions.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Function {
    CurrentDate,
    CurrentTimestamp,
    Rand,
    // Unary logical functions.
    IsNull(Scalar),
    Not(Scalar),
    // Unary mathematical functions.
    UnaryMinus(Scalar),
    // Binary logical functions.
    And(Scalar, Scalar),
    Or(Scalar, Scalar),
    Is(Scalar, Scalar),
    Equal(Scalar, Scalar),
    Greater(Scalar, Scalar),
    GreaterOrEqual(Scalar, Scalar),
    Less(Scalar, Scalar),
    LessOrEqual(Scalar, Scalar),
    Like(Scalar, Scalar),
    NotEqual(Scalar, Scalar),
    // Binary mathematical functions.
    Add(Scalar, Scalar, DataType),
    Divide(Scalar, Scalar, DataType),
    Multiply(Scalar, Scalar, DataType),
    Subtract(Scalar, Scalar, DataType),
    // Other binary functions.
    Coalesce(Scalar, Scalar, DataType),
    // Ternary functions.
    CaseNoValue(Scalar, Scalar, Scalar, DataType),
    // System functions.
    NextVal(Scalar),
    Xid,
}

impl Function {
    pub fn from(
        function: &zetasql::FunctionRefProto,
        signature: &zetasql::FunctionSignatureProto,
        mut arguments: Vec<Scalar>,
    ) -> Self {
        let name = function.name.as_ref().unwrap().as_str();
        let returns = DataType::from(
            signature
                .return_type
                .as_ref()
                .unwrap()
                .r#type
                .as_ref()
                .unwrap(),
        );
        match arguments.len() {
            0 => match name {
                "ZetaSQL:current_date" => Function::CurrentDate,
                "ZetaSQL:current_timestamp" => Function::CurrentTimestamp,
                "ZetaSQL:rand" => Function::Rand,
                other => panic!("{} is not a no-arg function", other),
            },
            1 => {
                let argument = arguments.pop().unwrap();
                match name {
                    "ZetaSQL:$is_null" => Function::IsNull(argument),
                    "ZetaSQL:$not" => Function::Not(argument),
                    "ZetaSQL:$unary_minus" => Function::UnaryMinus(argument),
                    "system:next_val" => Function::NextVal(argument),
                    other => panic!("{} is not a unary function", other),
                }
            }
            2 => {
                let right = arguments.pop().unwrap();
                let left = arguments.pop().unwrap();
                match name {
                    "ZetaSQL:$and" => Function::And(left, right),
                    "ZetaSQL:$equal" => Function::Equal(left, right),
                    "ZetaSQL:$greater" => Function::Greater(left, right),
                    "ZetaSQL:$greater_or_equal" => Function::GreaterOrEqual(left, right),
                    "ZetaSQL:$less" => Function::Less(left, right),
                    "ZetaSQL:$less_or_equal" => Function::LessOrEqual(left, right),
                    "ZetaSQL:$like" => Function::Like(left, right),
                    "ZetaSQL:$not_equal" => Function::NotEqual(left, right),
                    "ZetaSQL:$or" => Function::Or(left, right),
                    "ZetaSQL:$add" => Function::Add(left, right, returns),
                    "ZetaSQL:$divide" => Function::Divide(left, right, returns),
                    "ZetaSQL:$multiply" => Function::Multiply(left, right, returns),
                    "ZetaSQL:$subtract" => Function::Subtract(left, right, returns),
                    "ZetaSQL:coalesce" => Function::Coalesce(left, right, returns),
                    other => panic!("{} is not a binary function", other),
                }
            }
            3 => {
                let c = arguments.pop().unwrap();
                let b = arguments.pop().unwrap();
                let a = arguments.pop().unwrap();
                match name {
                    "ZetaSQL:$case_no_value" => Function::CaseNoValue(a, b, c, returns),
                    other => panic!("{} is not a ternary function", other),
                }
            }
            other => panic!("function {} has arity {}", name, other),
        }
        // "ZetaSQL:abs" => Function::Abs,
        // "ZetaSQL:acos" => Function::Acos,
        // "ZetaSQL:acosh" => Function::Acosh,
        // "ZetaSQL:asin" => Function::Asin,
        // "ZetaSQL:asinh" => Function::Asinh,
        // "ZetaSQL:atan" => Function::Atan,
        // "ZetaSQL:atan2" => Function::Atan2,
        // "ZetaSQL:atanh" => Function::Atanh,
        // "ZetaSQL:$between" => Function::Between,
        // "ZetaSQL:$bitwise_and" => Function::BitwiseAnd,
        // "ZetaSQL:$bitwise_left_shift" => Function::BitwiseLeftShift,
        // "ZetaSQL:$bitwise_not" => Function::BitwiseNot,
        // "ZetaSQL:$bitwise_or" => Function::BitwiseOr,
        // "ZetaSQL:$bitwise_right_shift" => Function::BitwiseRightShift,
        // "ZetaSQL:$bitwise_xor" => Function::BitwiseXor,
        // "ZetaSQL:byte_length" => Function::ByteLength,
        // "ZetaSQL:$case_no_value" => Function::CaseNoValue,
        // "ZetaSQL:$case_with_value" => Function::CaseWithValue,
        // "ZetaSQL:ceil" => Function::Ceil,
        // "ZetaSQL:char_length" => Function::CharLength,
        // "ZetaSQL:concat" => Function::Concat,
        // "ZetaSQL:cos" => Function::Cos,
        // "ZetaSQL:cosh" => Function::Cosh,
        // "ZetaSQL:date" => Function::Date,
        // "ZetaSQL:date_add" => Function::DateAdd,
        // "ZetaSQL:date_diff" => Function::DateDiff,
        // "ZetaSQL:date_from_unix_date" => Function::DateFromUnixDate,
        // "ZetaSQL:date_sub" => Function::DateSub,
        // "ZetaSQL:date_trunc" => Function::DateTrunc,
        // "ZetaSQL:div" => Function::Div,
        // "ZetaSQL:ends_with" => Function::EndsWith,
        // "ZetaSQL:exp" => Function::Exp,
        // "ZetaSQL:$extract" => Function::Extract,
        // "ZetaSQL:$extract_date" => Function::ExtractDate,
        // "ZetaSQL:floor" => Function::Floor,
        // "ZetaSQL:from_base64" => Function::FromBase64,
        // "ZetaSQL:from_hex" => Function::FromHex,
        // "ZetaSQL:greatest" => Function::Greatest,
        // "ZetaSQL:if" => Function::If,
        // "ZetaSQL:ifnull" => Function::Ifnull,
        // "ZetaSQL:$in" => Function::In,
        // "ZetaSQL:is_inf" => Function::IsInf,
        // "ZetaSQL:is_nan" => Function::IsNan,
        // "ZetaSQL:$is_null" => Function::IsNull,
        // "ZetaSQL:least" => Function::Least,
        // "ZetaSQL:length" => Function::Length,
        // "ZetaSQL:ln" => Function::Ln,
        // "ZetaSQL:log" => Function::Log,
        // "ZetaSQL:log10" => Function::Log10,
        // "ZetaSQL:lower" => Function::Lower,
        // "ZetaSQL:lpad" => Function::Lpad,
        // "ZetaSQL:ltrim" => Function::Ltrim,
        // "ZetaSQL:mod" => Function::Mod,
        // "ZetaSQL:pow" => Function::Pow,
        // "ZetaSQL:regexp_contains" => Function::RegexpContains,
        // "ZetaSQL:regexp_extract" => Function::RegexpExtract,
        // "ZetaSQL:regexp_extract_all" => Function::RegexpExtractAll,
        // "ZetaSQL:regexp_replace" => Function::RegexpReplace,
        // "ZetaSQL:repeat" => Function::Repeat,
        // "ZetaSQL:replace" => Function::Replace,
        // "ZetaSQL:reverse" => Function::Reverse,
        // "ZetaSQL:round" => Function::Round,
        // "ZetaSQL:rpad" => Function::Rpad,
        // "ZetaSQL:rtrim" => Function::Rtrim,
        // "ZetaSQL:sign" => Function::Sign,
        // "ZetaSQL:sin" => Function::Sin,
        // "ZetaSQL:sinh" => Function::Sinh,
        // "ZetaSQL:split" => Function::Split,
        // "ZetaSQL:sqrt" => Function::Sqrt,
        // "ZetaSQL:starts_with" => Function::StartsWith,
        // "ZetaSQL:strpos" => Function::Strpos,
        // "ZetaSQL:substr" => Function::Substr,
        // "ZetaSQL:tan" => Function::Tan,
        // "ZetaSQL:tanh" => Function::Tanh,
        // "ZetaSQL:timestamp" => Function::Timestamp,
        // "ZetaSQL:timestamp_add" => Function::TimestampAdd,
        // "ZetaSQL:timestamp_diff" => Function::TimestampDiff,
        // "ZetaSQL:timestamp_from_unix_micros" => Function::TimestampFromUnixMicros,
        // "ZetaSQL:timestamp_from_unix_millis" => Function::TimestampFromUnixMillis,
        // "ZetaSQL:timestamp_from_unix_seconds" => Function::TimestampFromUnixSeconds,
        // "ZetaSQL:timestamp_micros" => Function::TimestampMicros,
        // "ZetaSQL:timestamp_millis" => Function::TimestampMillis,
        // "ZetaSQL:timestamp_seconds" => Function::TimestampSeconds,
        // "ZetaSQL:timestamp_sub" => Function::TimestampSub,
        // "ZetaSQL:timestamp_trunc" => Function::TimestampTrunc,
        // "ZetaSQL:to_base64" => Function::ToBase64,
        // "ZetaSQL:to_hex" => Function::ToHex,
        // "ZetaSQL:trim" => Function::Trim,
        // "ZetaSQL:trunc" => Function::Trunc,
        // "ZetaSQL:unix_date" => Function::UnixDate,
        // "ZetaSQL:unix_seconds" => Function::UnixSecondsFromTimestamp,
        // "ZetaSQL:unix_millis" => Function::UnixMillisFromTimestamp,
        // "ZetaSQL:unix_micros" => Function::UnixMicrosFromTimestamp,
        // "ZetaSQL:upper" => Function::Upper,
    }

    pub fn name(&self) -> &str {
        match self {
            Function::CurrentDate => "CurrentDate",
            Function::CurrentTimestamp => "CurrentTimestamp",
            Function::Rand => "Rand",
            Function::IsNull(_) => "IsNull",
            Function::Not(_) => "Not",
            Function::UnaryMinus(_) => "UnaryMinus",
            Function::And(_, _) => "And",
            Function::Is(_, _) => "Is",
            Function::Equal(_, _) => "Equal",
            Function::Greater(_, _) => "Greater",
            Function::GreaterOrEqual(_, _) => "GreaterOrEqual",
            Function::Less(_, _) => "Less",
            Function::LessOrEqual(_, _) => "LessOrEqual",
            Function::Like(_, _) => "Like",
            Function::NotEqual(_, _) => "NotEqual",
            Function::Or(_, _) => "Or",
            Function::Add(_, _, _) => "Add",
            Function::Divide(_, _, _) => "Divide",
            Function::Multiply(_, _, _) => "Multiply",
            Function::Subtract(_, _, _) => "Subtract",
            Function::Coalesce(_, _, _) => "Coalesce",
            Function::CaseNoValue(_, _, _, _) => "CaseNoValue",
            Function::NextVal(_) => "NextVal",
            Function::Xid => "Xid",
        }
    }

    pub fn arguments(&self) -> Vec<&Scalar> {
        match self {
            Function::CurrentDate | Function::CurrentTimestamp | Function::Rand | Function::Xid => {
                vec![]
            }
            Function::IsNull(argument)
            | Function::Not(argument)
            | Function::UnaryMinus(argument)
            | Function::NextVal(argument) => vec![argument],
            Function::Is(left, right)
            | Function::Equal(left, right)
            | Function::Greater(left, right)
            | Function::GreaterOrEqual(left, right)
            | Function::Less(left, right)
            | Function::LessOrEqual(left, right)
            | Function::Like(left, right)
            | Function::NotEqual(left, right)
            | Function::Or(left, right)
            | Function::And(left, right)
            | Function::Add(left, right, _)
            | Function::Divide(left, right, _)
            | Function::Multiply(left, right, _)
            | Function::Subtract(left, right, _)
            | Function::Coalesce(left, right, _) => vec![left, right],
            Function::CaseNoValue(test, if_true, if_false, _) => vec![test, if_true, if_false],
        }
    }

    pub fn returns(&self) -> DataType {
        match self {
            Function::CurrentDate => DataType::Date,
            Function::CurrentTimestamp => DataType::Timestamp,
            Function::Rand => DataType::F64,
            Function::IsNull(_)
            | Function::Not(_)
            | Function::And(_, _)
            | Function::Is(_, _)
            | Function::Equal(_, _)
            | Function::Greater(_, _)
            | Function::GreaterOrEqual(_, _)
            | Function::Less(_, _)
            | Function::LessOrEqual(_, _)
            | Function::Like(_, _)
            | Function::NotEqual(_, _)
            | Function::Or(_, _) => DataType::Bool,
            Function::UnaryMinus(argument) => argument.data_type(),
            Function::Add(_, _, returns)
            | Function::Multiply(_, _, returns)
            | Function::Subtract(_, _, returns)
            | Function::Coalesce(_, _, returns)
            | Function::Divide(_, _, returns)
            | Function::CaseNoValue(_, _, _, returns) => returns.clone(),
            Function::NextVal(_) => DataType::I64,
            Function::Xid => DataType::I64,
        }
    }

    pub fn map(self, f: impl Fn(Scalar) -> Scalar) -> Self {
        match self {
            Function::CurrentDate | Function::CurrentTimestamp | Function::Rand | Function::Xid => {
                self
            }
            Function::IsNull(argument) => Function::IsNull(f(argument)),
            Function::Not(argument) => Function::Not(f(argument)),
            Function::And(left, right) => Function::And(f(left), f(right)),
            Function::Is(left, right) => Function::Is(f(left), f(right)),
            Function::Equal(left, right) => Function::Equal(f(left), f(right)),
            Function::Greater(left, right) => Function::Greater(f(left), f(right)),
            Function::GreaterOrEqual(left, right) => Function::GreaterOrEqual(f(left), f(right)),
            Function::Less(left, right) => Function::Less(f(left), f(right)),
            Function::LessOrEqual(left, right) => Function::LessOrEqual(f(left), f(right)),
            Function::Like(left, right) => Function::Like(f(left), f(right)),
            Function::NotEqual(left, right) => Function::NotEqual(f(left), f(right)),
            Function::Or(left, right) => Function::Or(f(left), f(right)),
            Function::UnaryMinus(argument) => Function::UnaryMinus(f(argument)),
            Function::Add(left, right, returns) => Function::Add(f(left), f(right), returns),
            Function::Divide(left, right, returns) => Function::Divide(f(left), f(right), returns),
            Function::Multiply(left, right, returns) => {
                Function::Multiply(f(left), f(right), returns)
            }
            Function::Subtract(left, right, returns) => {
                Function::Subtract(f(left), f(right), returns)
            }
            Function::Coalesce(left, right, returns) => {
                Function::Coalesce(f(left), f(right), returns)
            }
            Function::CaseNoValue(test, if_true, if_false, returns) => {
                Function::CaseNoValue(f(test), f(if_true), f(if_false), returns)
            }
            Function::NextVal(argument) => Function::NextVal(f(argument)),
        }
    }
}

// Aggregate functions appear in GROUP BY expressions.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum AggregateFn {
    AnyValue,
    Count,
    LogicalAnd,
    LogicalOr,
    Max,
    Min,
    Sum,
}

impl AggregateFn {
    pub fn from(name: &str) -> Self {
        match name {
            "ZetaSQL:any_value" => AggregateFn::AnyValue,
            "ZetaSQL:avg" => panic!("avg should be converted into sum / count"),
            "ZetaSQL:count" => AggregateFn::Count,
            "ZetaSQL:logical_and" => AggregateFn::LogicalAnd,
            "ZetaSQL:logical_or" => AggregateFn::LogicalOr,
            "ZetaSQL:max" => AggregateFn::Max,
            "ZetaSQL:min" => AggregateFn::Min,
            "ZetaSQL:sum" => AggregateFn::Sum,
            _ => panic!("{} is not supported", name),
        }
    }

    pub fn data_type(&self, column_type: DataType) -> DataType {
        match self {
            AggregateFn::AnyValue | AggregateFn::Max | AggregateFn::Min | AggregateFn::Sum => {
                column_type.clone()
            }
            AggregateFn::Count => DataType::I64,
            AggregateFn::LogicalAnd | AggregateFn::LogicalOr => DataType::Bool,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Procedure {
    CreateTable(Scalar),
    DropTable(Scalar),
    CreateIndex(Scalar),
    DropIndex(Scalar),
}

impl Procedure {
    fn references(&self) -> HashSet<Column> {
        match self {
            Procedure::CreateTable(argument)
            | Procedure::DropTable(argument)
            | Procedure::CreateIndex(argument)
            | Procedure::DropIndex(argument) => argument.references(),
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

pub struct ExprIterator<'it> {
    parent: &'it Expr,
    offset: usize,
}

impl<'it> Iterator for ExprIterator<'it> {
    type Item = &'it Expr;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset < self.parent.len() {
            self.offset += 1;
            Some(&self.parent[self.offset - 1])
        } else {
            None
        }
    }
}

fn map(mut visitor: impl FnMut(Expr) -> Expr, list: Vec<Expr>) -> Vec<Expr> {
    let mut mapped = Vec::with_capacity(list.len());
    for expr in list {
        mapped.push(visitor(expr))
    }
    mapped
}
