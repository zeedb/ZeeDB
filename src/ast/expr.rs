use crate::data_type;
use crate::values::*;
use arrow::datatypes::*;
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt;
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
    // LogicalGetWith { table } reads the subquery that was created by With.
    LogicalGetWith {
        name: String,
        columns: Vec<Column>,
    },
    // LogicalAggregate { group_by, aggregate } implements the GROUP BY clause.
    LogicalAggregate {
        group_by: Vec<Column>,
        aggregate: Vec<(AggregateFn, Column)>,
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
    // LogicalIntersect implements SELECT _ INTERSECT SELECT _.
    LogicalIntersect {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    // LogicalExcept implements SELECT _ EXCEPT SELECT _.
    LogicalExcept {
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
    // LogicalUpdate { sets } implements the UPDATE operation.
    // (column, None) indicates set column to default value.
    LogicalUpdate {
        table: Table,
        pid: Column,
        tid: Column,
        input: Box<Expr>,
    },
    // LogicalDelete { table } implements the DELETE operation.
    LogicalDelete {
        pid: Column,
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
        predicates: Vec<Scalar>,
        table: Table,
    },
    IndexScan {
        projects: Vec<Column>,
        predicates: Vec<Scalar>,
        index_predicates: Vec<(Column, Scalar)>,
        table: Table,
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
    LookupJoin {
        join: Join,
        projects: Vec<Column>,
        index_predicates: Vec<(Column, Scalar)>,
        table: Table,
        input: Box<Expr>,
    },
    CreateTempTable {
        name: String,
        columns: Vec<Column>,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    GetTempTable {
        name: String,
        columns: Vec<Column>,
    },
    Aggregate {
        group_by: Vec<Column>,
        aggregate: Vec<(AggregateFn, Column)>,
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
    Intersect {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Except {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Insert {
        table: Table,
        input: Box<Expr>,
    },
    Values {
        columns: Vec<Column>,
        values: Vec<Vec<Scalar>>,
        input: Box<Expr>,
    },
    Update {
        table: Table,
        pid: Column,
        tid: Column,
        input: Box<Expr>,
    },
    Delete {
        pid: Column,
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
            | Expr::LogicalUnion { .. }
            | Expr::LogicalIntersect { .. }
            | Expr::LogicalExcept { .. }
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
            | Expr::LookupJoin { .. }
            | Expr::CreateTempTable { .. }
            | Expr::GetTempTable { .. }
            | Expr::Aggregate { .. }
            | Expr::Limit { .. }
            | Expr::Sort { .. }
            | Expr::Union { .. }
            | Expr::Intersect { .. }
            | Expr::Except { .. }
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Update { .. }
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
            | Expr::LogicalGetWith { .. }
            | Expr::LogicalAggregate { .. }
            | Expr::LogicalLimit { .. }
            | Expr::LogicalSort { .. }
            | Expr::LogicalUnion { .. }
            | Expr::LogicalIntersect { .. }
            | Expr::LogicalExcept { .. }
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
            | Expr::LookupJoin { .. }
            | Expr::CreateTempTable { .. }
            | Expr::GetTempTable { .. }
            | Expr::Aggregate { .. }
            | Expr::Limit { .. }
            | Expr::Sort { .. }
            | Expr::Union { .. }
            | Expr::Intersect { .. }
            | Expr::Except { .. }
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Update { .. }
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
            | Expr::LogicalIntersect { .. }
            | Expr::LogicalExcept { .. }
            | Expr::NestedLoop { .. }
            | Expr::HashJoin { .. }
            | Expr::CreateTempTable { .. }
            | Expr::Union { .. }
            | Expr::Intersect { .. }
            | Expr::Except { .. } => 2,
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
            | Expr::LookupJoin { .. }
            | Expr::Aggregate { .. }
            | Expr::Limit { .. }
            | Expr::Sort { .. }
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Update { .. }
            | Expr::Delete { .. }
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
            | Expr::IndexScan { .. }
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
            Expr::LogicalIntersect { left, right } => {
                let left = Box::new(visitor(*left));
                let right = Box::new(visitor(*right));
                Expr::LogicalIntersect { left, right }
            }
            Expr::LogicalExcept { left, right } => {
                let left = Box::new(visitor(*left));
                let right = Box::new(visitor(*right));
                Expr::LogicalExcept { left, right }
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
            Expr::LogicalUpdate {
                table,
                pid,
                tid,
                input,
            } => {
                let input = Box::new(visitor(*input));
                Expr::LogicalUpdate {
                    table,
                    pid,
                    tid,
                    input,
                }
            }
            Expr::LogicalDelete { pid, tid, input } => Expr::LogicalDelete {
                pid,
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
                left,
                right,
            } => Expr::CreateTempTable {
                name,
                columns,
                left: Box::new(visitor(*left)),
                right: Box::new(visitor(*right)),
            },
            Expr::GetTempTable { name, columns } => Expr::GetTempTable { name, columns },
            Expr::LookupJoin {
                join,
                projects,
                table,
                index_predicates,
                input,
            } => Expr::LookupJoin {
                join,
                projects,
                table,
                index_predicates,
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
            Expr::Intersect { left, right } => Expr::Intersect {
                left: Box::new(visitor(*left)),
                right: Box::new(visitor(*right)),
            },
            Expr::Except { left, right } => Expr::Except {
                left: Box::new(visitor(*left)),
                right: Box::new(visitor(*right)),
            },
            Expr::Insert { table, input } => Expr::Insert {
                table,
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
            Expr::Update {
                table,
                pid,
                tid,

                input,
            } => Expr::Update {
                table,
                pid,
                tid,

                input: Box::new(visitor(*input)),
            },
            Expr::Delete { pid, tid, input } => Expr::Delete {
                pid,
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
            | Expr::SeqScan { .. }
            | Expr::IndexScan { .. } => self,
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
            | Expr::LogicalIntersect { right: input, .. }
            | Expr::LogicalExcept { right: input, .. }
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
                for (_, c) in aggregate {
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
            | Expr::LookupJoin { .. }
            | Expr::CreateTempTable { .. }
            | Expr::GetTempTable { .. }
            | Expr::Aggregate { .. }
            | Expr::Limit { .. }
            | Expr::Sort { .. }
            | Expr::Union { .. }
            | Expr::Intersect { .. }
            | Expr::Except { .. }
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Update { .. }
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
                for (f, _) in aggregate {
                    set.extend(f.references());
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
            | Expr::LogicalIntersect { .. }
            | Expr::LogicalExcept { .. }
            | Expr::LogicalInsert { .. }
            | Expr::LogicalUpdate { .. }
            | Expr::LogicalDelete { .. }
            | Expr::LogicalCreateDatabase { .. }
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
            | Expr::LookupJoin { .. }
            | Expr::CreateTempTable { .. }
            | Expr::GetTempTable { .. }
            | Expr::Aggregate { .. }
            | Expr::Limit { .. }
            | Expr::Sort { .. }
            | Expr::Union { .. }
            | Expr::Intersect { .. }
            | Expr::Except { .. }
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Update { .. }
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
            nulls_first: o.nulls_first,
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
                    .map(|(f, c)| (f.clone(), subst_c(c)))
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
            Expr::LogicalUpdate {
                table,
                pid,
                tid,
                input,
            } => Expr::LogicalUpdate {
                table,
                pid: subst_c(&pid),
                tid: subst_c(&tid),
                input: Box::new(input.subst(map)),
            },
            Expr::LogicalDelete { pid, tid, input } => Expr::LogicalDelete {
                pid: subst_c(&pid),
                tid: subst_c(&tid),
                input: Box::new(input.subst(map)),
            },
            Expr::Leaf { .. }
            | Expr::LogicalSingleGet { .. }
            | Expr::LogicalLimit { .. }
            | Expr::LogicalUnion { .. }
            | Expr::LogicalIntersect { .. }
            | Expr::LogicalExcept { .. }
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
            | Expr::LookupJoin { .. }
            | Expr::CreateTempTable { .. }
            | Expr::GetTempTable { .. }
            | Expr::Aggregate { .. }
            | Expr::Limit { .. }
            | Expr::Sort { .. }
            | Expr::Union { .. }
            | Expr::Intersect { .. }
            | Expr::Except { .. }
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Update { .. }
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
            | Expr::LogicalUnion { left, right }
            | Expr::LogicalIntersect { left, right }
            | Expr::LogicalExcept { left, right }
            | Expr::NestedLoop { left, right, .. }
            | Expr::HashJoin { left, right, .. }
            | Expr::CreateTempTable { left, right, .. }
            | Expr::Union { left, right }
            | Expr::Intersect { left, right }
            | Expr::Except { left, right } => match index {
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
            | Expr::LookupJoin { input, .. }
            | Expr::Aggregate { input, .. }
            | Expr::Limit { input, .. }
            | Expr::Sort { input, .. }
            | Expr::Insert { input, .. }
            | Expr::Values { input, .. }
            | Expr::Update { input, .. }
            | Expr::Delete { input, .. }
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
            | Expr::IndexScan { .. }
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
    pub columns: Vec<Column>,
}

impl Table {
    pub fn from(table: &zetasql::ResolvedTableScanProto) -> Self {
        match table {
            zetasql::ResolvedTableScanProto {
                parent: Some(zetasql::ResolvedScanProto { column_list, .. }),
                table:
                    Some(zetasql::TableRefProto {
                        serialization_id: Some(id),
                        name: Some(name),
                        ..
                    }),
                ..
            } => {
                let mut columns = Vec::with_capacity(column_list.len());
                for c in column_list {
                    columns.push(Column::from(c));
                }
                Table {
                    id: *id,
                    name: name.clone(),
                    columns,
                }
            }
            other => panic!("{:?}", other),
        }
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct Column {
    pub created: Phase,
    pub id: i64,
    pub name: String,
    pub table: Option<String>,
    pub data: DataType,
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum Phase {
    Parse,
    Convert,
    Plan,
}

impl Column {
    pub fn from(column: &zetasql::ResolvedColumnProto) -> Self {
        Column {
            created: Phase::Parse,
            id: column.column_id.unwrap(),
            name: column.name.clone().unwrap(),
            table: column.table_name.clone(),
            data: data_type::from_proto(column.r#type.as_ref().unwrap()),
        }
    }

    pub fn into_query_field(&self) -> Field {
        Field::new(self.canonical_name().as_str(), self.data.clone(), true)
    }

    pub fn into_table_field(&self) -> Field {
        Field::new(self.name.as_str(), self.data.clone(), true)
    }

    pub fn canonical_name(&self) -> String {
        format!("{}#{}", self.name, self.id)
    }
}

impl cmp::PartialOrd for Column {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl cmp::Ord for Column {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        let table = self.table.cmp(&other.table);
        let name = self.name.cmp(&other.name);
        let id = self.id.cmp(&other.id);
        table.then(name).then(id)
    }
}

impl fmt::Debug for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(table) = &self.table {
            write!(f, "{}.", table)?;
        }
        write!(f, "{}#", self.name)?;
        write!(f, "{}", self.id)?;
        if self.created == Phase::Plan {
            write!(f, "'")?;
        }
        Ok(())
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(table) = &self.table {
            write!(f, "{}.", table)?;
        }
        write!(f, "{}", self.name)?;
        if self.created == Phase::Plan {
            write!(f, "'")?;
        }
        Ok(())
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
    pub nulls_first: bool,
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
    pub fn data(&self) -> DataType {
        match self {
            Scalar::Literal(value) => value.data().clone(),
            Scalar::Column(column) => column.data.clone(),
            Scalar::Parameter(_, data) => data.clone(),
            Scalar::Call(function) => function.returns().clone(),
            Scalar::Cast(_, data) => data.clone(),
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
            Scalar::Cast(uncast, data) => {
                Scalar::Cast(Box::new(uncast.inline(expr, column)), data.clone())
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
    Not(Scalar),
    // Unary mathematical functions.
    UnaryMinus(Scalar),
    // Binary logical functions.
    And(Scalar, Scalar),
    Equal(Scalar, Scalar),
    Greater(Scalar, Scalar),
    GreaterOrEqual(Scalar, Scalar),
    Less(Scalar, Scalar),
    LessOrEqual(Scalar, Scalar),
    Like(Scalar, Scalar),
    NotEqual(Scalar, Scalar),
    Or(Scalar, Scalar),
    // Binary mathematical functions.
    Add(Scalar, Scalar, DataType),
    Divide(Scalar, Scalar, DataType),
    Multiply(Scalar, Scalar, DataType),
    Subtract(Scalar, Scalar, DataType),
    // System functions.
    Default(Column, DataType),
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
                    "ZetaSQL:$not" => Function::Not(argument),
                    "ZetaSQL:$unary_minus" => Function::UnaryMinus(argument),
                    "system:next_val" => Function::NextVal(argument),
                    other => panic!("{} is not a unary function", other),
                }
            }
            2 => {
                let right = arguments.pop().unwrap();
                let left = arguments.pop().unwrap();
                let returns = crate::data_type::from_proto(
                    signature
                        .return_type
                        .as_ref()
                        .unwrap()
                        .r#type
                        .as_ref()
                        .unwrap(),
                );
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
                    other => panic!("{} is not a binary function", other),
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
        // "ZetaSQL:coalesce" => Function::Coalesce,
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
            Function::Not(_) => "Not",
            Function::UnaryMinus(_) => "UnaryMinus",
            Function::And(_, _) => "And",
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
            Function::Default(_, _) => "Default",
            Function::NextVal(_) => "NextVal",
            Function::Xid => "Xid",
        }
    }

    pub fn arguments(&self) -> Vec<&Scalar> {
        match self {
            Function::CurrentDate
            | Function::CurrentTimestamp
            | Function::Rand
            | Function::Default(_, _)
            | Function::Xid => vec![],
            Function::Not(argument)
            | Function::UnaryMinus(argument)
            | Function::NextVal(argument) => vec![argument],
            Function::Equal(left, right)
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
            | Function::Subtract(left, right, _) => vec![left, right],
        }
    }

    pub fn returns(&self) -> DataType {
        match self {
            Function::CurrentDate => DataType::Date32(DateUnit::Day),
            Function::CurrentTimestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            Function::Rand => DataType::Float64,
            Function::Not(_)
            | Function::And(_, _)
            | Function::Equal(_, _)
            | Function::Greater(_, _)
            | Function::GreaterOrEqual(_, _)
            | Function::Less(_, _)
            | Function::LessOrEqual(_, _)
            | Function::Like(_, _)
            | Function::NotEqual(_, _)
            | Function::Or(_, _) => DataType::Boolean,
            Function::UnaryMinus(argument) => argument.data(),
            Function::Add(_, _, returns)
            | Function::Multiply(_, _, returns)
            | Function::Subtract(_, _, returns)
            | Function::Divide(_, _, returns)
            | Function::Default(_, returns) => returns.clone(),
            Function::NextVal(_) => DataType::Int64,
            Function::Xid => DataType::UInt64,
        }
    }

    pub fn map(self, f: impl Fn(Scalar) -> Scalar) -> Self {
        match self {
            Function::CurrentDate
            | Function::CurrentTimestamp
            | Function::Rand
            | Function::Xid
            | Function::Default(_, _) => self,
            Function::Not(argument) => Function::Not(f(argument)),
            Function::And(left, right) => Function::And(f(left), f(right)),
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
            Function::NextVal(argument) => Function::NextVal(f(argument)),
        }
    }
}

// Aggregate functions appear in GROUP BY expressions.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum AggregateFn {
    AnyValue(Column),
    ArrayAgg(Distinct, IgnoreNulls, Column),
    ArrayConcatAgg(Column),
    Avg(Distinct, Column),
    BitAnd(Distinct, Column),
    BitOr(Distinct, Column),
    BitXor(Distinct, Column),
    Count(Distinct, Column),
    CountStar,
    LogicalAnd(Column),
    LogicalOr(Column),
    Max(Column),
    Min(Column),
    StringAgg(Distinct, Column),
    Sum(Distinct, Column), // TODO what happens to string_agg(_, ', ')
}

impl AggregateFn {
    pub fn from(
        name: String,
        distinct: bool,
        ignore_nulls: bool,
        argument: Option<Column>,
    ) -> Self {
        let distinct = Distinct(distinct);
        let ignore_nulls = IgnoreNulls(ignore_nulls);
        match name.as_str() {
            "ZetaSQL:any_value" => AggregateFn::AnyValue(argument.unwrap()),
            "ZetaSQL:array_agg" => AggregateFn::ArrayAgg(distinct, ignore_nulls, argument.unwrap()),
            "ZetaSQL:array_concat_agg" => AggregateFn::ArrayConcatAgg(argument.unwrap()),
            "ZetaSQL:avg" => AggregateFn::Avg(distinct, argument.unwrap()),
            "ZetaSQL:bit_and" => AggregateFn::BitAnd(distinct, argument.unwrap()),
            "ZetaSQL:bit_or" => AggregateFn::BitOr(distinct, argument.unwrap()),
            "ZetaSQL:bit_xor" => AggregateFn::BitXor(distinct, argument.unwrap()),
            "ZetaSQL:count" => AggregateFn::Count(distinct, argument.unwrap()),
            "ZetaSQL:$count_star" => AggregateFn::CountStar,
            "ZetaSQL:logical_and" => AggregateFn::LogicalAnd(argument.unwrap()),
            "ZetaSQL:logical_or" => AggregateFn::LogicalOr(argument.unwrap()),
            "ZetaSQL:max" => AggregateFn::Max(argument.unwrap()),
            "ZetaSQL:min" => AggregateFn::Min(argument.unwrap()),
            "ZetaSQL:string_agg" => AggregateFn::StringAgg(distinct, argument.unwrap()),
            "ZetaSQL:sum" => AggregateFn::Sum(distinct, argument.unwrap()),
            _ => panic!("{} is not supported", name),
        }
    }

    fn references(&self) -> Option<Column> {
        match self {
            AggregateFn::AnyValue(c) => Some(c.clone()),
            AggregateFn::ArrayAgg(_, _, c) => Some(c.clone()),
            AggregateFn::ArrayConcatAgg(c) => Some(c.clone()),
            AggregateFn::Avg(_, c) => Some(c.clone()),
            AggregateFn::BitAnd(_, c) => Some(c.clone()),
            AggregateFn::BitOr(_, c) => Some(c.clone()),
            AggregateFn::BitXor(_, c) => Some(c.clone()),
            AggregateFn::Count(_, c) => Some(c.clone()),
            AggregateFn::CountStar => None,
            AggregateFn::LogicalAnd(c) => Some(c.clone()),
            AggregateFn::LogicalOr(c) => Some(c.clone()),
            AggregateFn::Max(c) => Some(c.clone()),
            AggregateFn::Min(c) => Some(c.clone()),
            AggregateFn::StringAgg(_, c) => Some(c.clone()),
            AggregateFn::Sum(_, c) => Some(c.clone()),
        }
    }
}

impl fmt::Display for AggregateFn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateFn::AnyValue(column) => write!(f, "(AnyValue {})", column),
            AggregateFn::ArrayAgg(Distinct(true), IgnoreNulls(true), column) => {
                write!(f, "(ArrayAgg (Distinct {}))", column)
            }
            AggregateFn::ArrayAgg(Distinct(true), IgnoreNulls(false), column) => {
                write!(f, "(ArrayAgg (Distinct (IgnoreNulls {})))", column)
            }
            AggregateFn::ArrayAgg(Distinct(false), IgnoreNulls(true), column) => {
                write!(f, "(ArrayAgg IgnoreNulls({}))", column)
            }
            AggregateFn::ArrayAgg(Distinct(false), IgnoreNulls(false), column) => {
                write!(f, "(ArrayAgg (IgnoreNulls {}))", column)
            }
            AggregateFn::ArrayConcatAgg(column) => write!(f, "(ArrayConcatAgg {})", column),
            AggregateFn::Avg(Distinct(true), column) => write!(f, "(Avg (Distinct {}))", column),
            AggregateFn::Avg(Distinct(false), column) => write!(f, "(Avg {})", column),
            AggregateFn::BitAnd(Distinct(true), column) => {
                write!(f, "(BitAnd (Distinct {}))", column)
            }
            AggregateFn::BitAnd(Distinct(false), column) => write!(f, "(BitAnd {})", column),
            AggregateFn::BitOr(Distinct(true), column) => {
                write!(f, "(BitOr (Distinct {}))", column)
            }
            AggregateFn::BitOr(Distinct(false), column) => write!(f, "(BitOr {})", column),
            AggregateFn::BitXor(Distinct(true), column) => {
                write!(f, "(BitXor (Distinct {}))", column)
            }
            AggregateFn::BitXor(Distinct(false), column) => write!(f, "(Avg {})", column),
            AggregateFn::Count(Distinct(true), column) => {
                write!(f, "(Count (Distinct {}))", column)
            }
            AggregateFn::Count(Distinct(false), column) => write!(f, "(Count {})", column),
            AggregateFn::CountStar => write!(f, "(CountStar)"),
            AggregateFn::LogicalAnd(column) => write!(f, "(LogicalAnd {})", column),
            AggregateFn::LogicalOr(column) => write!(f, "(LogicalOr {})", column),
            AggregateFn::Max(column) => write!(f, "(Max {})", column),
            AggregateFn::Min(column) => write!(f, "(Min {})", column),
            AggregateFn::StringAgg(Distinct(true), column) => {
                write!(f, "(StringAgg (Distinct {}))", column)
            }
            AggregateFn::StringAgg(Distinct(false), column) => write!(f, "(Avg {})", column),
            AggregateFn::Sum(Distinct(true), column) => write!(f, "(Sum (Distinct {}))", column),
            AggregateFn::Sum(Distinct(false), column) => write!(f, "(Sum {})", column),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Distinct(bool);

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct IgnoreNulls(bool);

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
