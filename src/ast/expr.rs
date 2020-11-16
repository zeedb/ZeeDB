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
    Leaf(usize),
    // LogicalSingleGet acts as the input to a SELECT with no FROM clause.
    LogicalSingleGet,
    // LogicalGet(table) implements the FROM clause.
    LogicalGet {
        projects: Vec<Column>,
        predicates: Vec<Scalar>,
        table: Table,
    },
    // LogicalFilter(predicates) implements the WHERE/HAVING clauses.
    LogicalFilter(Vec<Scalar>, Box<Expr>),
    // LogicalMap(columns) implements the SELECT clause.
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
    // LogicalWith(table) implements with subquery as  _.
    // The with-subquery is always on the left.
    LogicalWith(String, Vec<Column>, Box<Expr>, Box<Expr>),
    // LogicalGetWith(table) reads the subquery that was created by With.
    LogicalGetWith(String, Vec<Column>),
    // LogicalAggregate(group_by, aggregate) implements the GROUP BY clause.
    LogicalAggregate {
        group_by: Vec<Column>,
        aggregate: Vec<(AggregateFn, Column)>,
        input: Box<Expr>,
    },
    // LogicalLimit(n) implements the LIMIT / OFFSET / TOP clause.
    LogicalLimit {
        limit: usize,
        offset: usize,
        input: Box<Expr>,
    },
    // LogicalSort(columns) implements the ORDER BY clause.
    LogicalSort(Vec<OrderBy>, Box<Expr>),
    // LogicalUnion implements SELECT _ UNION ALL SELECT _.
    LogicalUnion(Box<Expr>, Box<Expr>),
    // LogicalIntersect implements SELECT _ INTERSECT SELECT _.
    LogicalIntersect(Box<Expr>, Box<Expr>),
    // LogicalExcept implements SELECT _ EXCEPT SELECT _.
    LogicalExcept(Box<Expr>, Box<Expr>),
    // LogicalInsert(table, columns) implements the INSERT operation.
    LogicalInsert(Table, Vec<Column>, Box<Expr>),
    // LogicalValues(rows, columns) implements VALUES expressions.
    LogicalValues(Vec<Column>, Vec<Vec<Scalar>>, Box<Expr>),
    // LogicalUpdate(sets) implements the UPDATE operation.
    // (column, None) indicates set column to default value.
    LogicalUpdate(Vec<(Column, Option<Column>)>, Box<Expr>),
    // LogicalDelete(table) implements the DELETE operation.
    LogicalDelete(Table, Box<Expr>),
    // LogicalCreateDatabase(database) implements the CREATE DATABASE operation.
    LogicalCreateDatabase(Name),
    // LogicalCreateTable implements the CREATE TABLE operation.
    LogicalCreateTable {
        name: Name,
        columns: Vec<(String, DataType)>,
        partition_by: Vec<i64>,
        cluster_by: Vec<i64>,
        primary_key: Vec<i64>,
        input: Option<Box<Expr>>,
    },
    // LogicalCreateIndex implements the CREATE INDEX operation.
    LogicalCreateIndex {
        name: Name,
        table: Name,
        columns: Vec<String>,
    },
    // LogicalAlterTable implements the ALTER TABLE operation.
    LogicalAlterTable {
        name: Name,
        actions: Vec<Alter>,
    },
    // LogicalDrop implements the DROP DATABASE/TABLE/INDEX operation.
    LogicalDrop {
        object: ObjectType,
        name: Name,
    },
    // LogicalRename implements the RENAME operation.
    LogicalRename {
        object: ObjectType,
        from: Name,
        to: Name,
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
    Filter(Vec<Scalar>, Box<Expr>),
    Map {
        include_existing: bool,
        projects: Vec<(Scalar, Column)>,
        input: Box<Expr>,
    },
    NestedLoop(Join, Box<Expr>, Box<Expr>),
    HashJoin {
        join: Join,
        equi_predicates: Vec<(Scalar, Scalar)>,
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
    CreateTempTable(String, Vec<Column>, Box<Expr>, Box<Expr>),
    GetTempTable(String, Vec<Column>),
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
    Sort(Vec<OrderBy>, Box<Expr>),
    Union(Box<Expr>, Box<Expr>),
    Intersect(Box<Expr>, Box<Expr>),
    Except(Box<Expr>, Box<Expr>),
    Insert(Table, Vec<Column>, Box<Expr>),
    Values(Vec<Column>, Vec<Vec<Scalar>>, Box<Expr>),
    Update(Vec<(Column, Option<Column>)>, Box<Expr>),
    Delete(Table, Box<Expr>),
    CreateDatabase(Name),
    CreateTable {
        name: Name,
        columns: Vec<(String, DataType)>,
        partition_by: Vec<i64>,
        cluster_by: Vec<i64>,
        primary_key: Vec<i64>,
        input: Option<Box<Expr>>,
    },
    CreateIndex {
        name: Name,
        table: Name,
        columns: Vec<String>,
    },
    AlterTable {
        name: Name,
        actions: Vec<Alter>,
    },
    Drop {
        object: ObjectType,
        name: Name,
    },
    Rename {
        object: ObjectType,
        from: Name,
        to: Name,
    },
}

impl Expr {
    pub fn is_logical(&self) -> bool {
        match self {
            Expr::LogicalJoin { .. }
            | Expr::LogicalDependentJoin { .. }
            | Expr::LogicalWith(_, _, _, _)
            | Expr::LogicalUnion(_, _)
            | Expr::LogicalIntersect(_, _)
            | Expr::LogicalExcept(_, _)
            | Expr::LogicalFilter(_, _)
            | Expr::LogicalMap { .. }
            | Expr::LogicalAggregate { .. }
            | Expr::LogicalLimit { .. }
            | Expr::LogicalSort(_, _)
            | Expr::LogicalInsert(_, _, _)
            | Expr::LogicalValues(_, _, _)
            | Expr::LogicalUpdate(_, _)
            | Expr::LogicalDelete(_, _)
            | Expr::LogicalSingleGet
            | Expr::LogicalGet { .. }
            | Expr::LogicalGetWith(_, _)
            | Expr::LogicalCreateDatabase(_)
            | Expr::LogicalCreateTable { .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalAlterTable { .. }
            | Expr::LogicalDrop { .. }
            | Expr::LogicalRename { .. } => true,
            Expr::Leaf(_)
            | Expr::TableFreeScan { .. }
            | Expr::SeqScan { .. }
            | Expr::IndexScan { .. }
            | Expr::Filter { .. }
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
            | Expr::CreateDatabase { .. }
            | Expr::CreateTable { .. }
            | Expr::CreateIndex { .. }
            | Expr::AlterTable { .. }
            | Expr::Drop { .. }
            | Expr::Rename { .. } => false,
        }
    }

    pub fn is_leaf(&self) -> bool {
        match self {
            Expr::Leaf(_) => true,
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
            | Expr::LogicalAlterTable { .. }
            | Expr::LogicalDrop { .. }
            | Expr::LogicalRename { .. } => true,
            _ if self.is_logical() => self.iter().any(|expr| expr.has_side_effects()),
            _ => panic!("{} is not logical", self.name()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Expr::LogicalJoin { .. }
            | Expr::LogicalDependentJoin { .. }
            | Expr::LogicalWith(_, _, _, _)
            | Expr::LogicalUnion(_, _)
            | Expr::LogicalIntersect(_, _)
            | Expr::LogicalExcept(_, _)
            | Expr::NestedLoop { .. }
            | Expr::HashJoin { .. }
            | Expr::CreateTempTable { .. }
            | Expr::Union { .. }
            | Expr::Intersect { .. }
            | Expr::Except { .. } => 2,
            Expr::LogicalFilter(_, _)
            | Expr::LogicalMap { .. }
            | Expr::LogicalAggregate { .. }
            | Expr::LogicalLimit { .. }
            | Expr::LogicalSort(_, _)
            | Expr::LogicalInsert(_, _, _)
            | Expr::LogicalValues(_, _, _)
            | Expr::LogicalUpdate(_, _)
            | Expr::LogicalDelete(_, _)
            | Expr::LogicalCreateTable { input: Some(_), .. }
            | Expr::Filter { .. }
            | Expr::Map { .. }
            | Expr::LookupJoin { .. }
            | Expr::Aggregate { .. }
            | Expr::Limit { .. }
            | Expr::Sort { .. }
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Update { .. }
            | Expr::Delete { .. }
            | Expr::CreateTable { input: Some(_), .. } => 1,
            Expr::Leaf(_)
            | Expr::LogicalSingleGet
            | Expr::LogicalGet { .. }
            | Expr::LogicalGetWith(_, _)
            | Expr::LogicalCreateDatabase(_)
            | Expr::LogicalCreateTable { input: None, .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalAlterTable { .. }
            | Expr::LogicalDrop { .. }
            | Expr::LogicalRename { .. }
            | Expr::TableFreeScan
            | Expr::SeqScan { .. }
            | Expr::IndexScan { .. }
            | Expr::GetTempTable { .. }
            | Expr::CreateDatabase { .. }
            | Expr::CreateTable { input: None, .. }
            | Expr::CreateIndex { .. }
            | Expr::AlterTable { .. }
            | Expr::Drop { .. }
            | Expr::Rename { .. } => 0,
        }
    }

    pub fn map(self, mut visitor: impl FnMut(Expr) -> Expr) -> Expr {
        match self {
            Expr::LogicalFilter(predicates, input) => {
                let input = Box::new(visitor(*input));
                Expr::LogicalFilter(predicates, input)
            }
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
            Expr::LogicalWith(name, columns, left, right) => {
                let left = Box::new(visitor(*left));
                let right = Box::new(visitor(*right));
                Expr::LogicalWith(name, columns, left, right)
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
            Expr::LogicalSort(sorts, input) => {
                let input = Box::new(visitor(*input));
                Expr::LogicalSort(sorts, input)
            }
            Expr::LogicalUnion(left, right) => {
                let left = Box::new(visitor(*left));
                let right = Box::new(visitor(*right));
                Expr::LogicalUnion(left, right)
            }
            Expr::LogicalIntersect(left, right) => {
                let left = Box::new(visitor(*left));
                let right = Box::new(visitor(*right));
                Expr::LogicalIntersect(left, right)
            }
            Expr::LogicalExcept(left, right) => {
                let left = Box::new(visitor(*left));
                let right = Box::new(visitor(*right));
                Expr::LogicalExcept(left, right)
            }
            Expr::LogicalInsert(table, columns, input) => {
                let input = Box::new(visitor(*input));
                Expr::LogicalInsert(table, columns, input)
            }
            Expr::LogicalValues(columns, rows, input) => {
                let input = Box::new(visitor(*input));
                Expr::LogicalValues(columns, rows, input)
            }
            Expr::LogicalUpdate(updates, input) => {
                let input = Box::new(visitor(*input));
                Expr::LogicalUpdate(updates, input)
            }
            Expr::LogicalDelete(table, input) => {
                let input = Box::new(visitor(*input));
                Expr::LogicalDelete(table, input)
            }
            Expr::LogicalSingleGet => Expr::LogicalSingleGet,
            Expr::LogicalGet {
                projects,
                predicates,
                table,
            } => Expr::LogicalGet {
                projects,
                predicates,
                table,
            },
            Expr::LogicalGetWith(name, columns) => Expr::LogicalGetWith(name, columns),
            Expr::LogicalCreateDatabase(name) => Expr::LogicalCreateDatabase(name),
            Expr::LogicalCreateTable {
                name,
                columns,
                partition_by,
                cluster_by,
                primary_key,
                input,
            } => Expr::LogicalCreateTable {
                name,
                columns,
                partition_by,
                cluster_by,
                primary_key,
                input: input.map(|input| Box::new(visitor(*input))),
            },
            Expr::LogicalCreateIndex {
                name,
                table,
                columns,
            } => Expr::LogicalCreateIndex {
                name,
                table,
                columns,
            },
            Expr::LogicalAlterTable { name, actions } => Expr::LogicalAlterTable { name, actions },
            Expr::LogicalDrop { object, name } => Expr::LogicalDrop { object, name },
            Expr::LogicalRename { object, from, to } => Expr::LogicalRename { object, from, to },
            Expr::TableFreeScan => Expr::TableFreeScan,
            Expr::SeqScan {
                projects,
                predicates,
                table,
            } => Expr::SeqScan {
                projects,
                predicates,
                table,
            },
            Expr::IndexScan {
                projects,
                predicates,
                table,
                index_predicates,
            } => Expr::IndexScan {
                projects,
                predicates,
                table,
                index_predicates,
            },
            Expr::Filter(predicates, input) => Expr::Filter(predicates, Box::new(visitor(*input))),
            Expr::Map {
                include_existing,
                projects,
                input,
            } => Expr::Map {
                include_existing,
                projects,
                input: Box::new(visitor(*input)),
            },
            Expr::NestedLoop(join, left, right) => {
                Expr::NestedLoop(join, Box::new(visitor(*left)), Box::new(visitor(*right)))
            }
            Expr::HashJoin {
                join,
                equi_predicates,
                left,
                right,
            } => Expr::HashJoin {
                join,
                equi_predicates,
                left: Box::new(visitor(*left)),
                right: Box::new(visitor(*right)),
            },
            Expr::CreateTempTable(name, columns, left, right) => Expr::CreateTempTable(
                name,
                columns,
                Box::new(visitor(*left)),
                Box::new(visitor(*right)),
            ),
            Expr::GetTempTable(name, columns) => Expr::GetTempTable(name, columns),
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
            Expr::Sort(order_by, input) => Expr::Sort(order_by, Box::new(visitor(*input))),
            Expr::Union(left, right) => {
                Expr::Union(Box::new(visitor(*left)), Box::new(visitor(*right)))
            }
            Expr::Intersect(left, right) => {
                Expr::Intersect(Box::new(visitor(*left)), Box::new(visitor(*right)))
            }
            Expr::Except(left, right) => {
                Expr::Except(Box::new(visitor(*left)), Box::new(visitor(*right)))
            }
            Expr::Insert(table, columns, input) => {
                Expr::Insert(table, columns, Box::new(visitor(*input)))
            }
            Expr::Values(columns, rows, input) => {
                Expr::Values(columns, rows, Box::new(visitor(*input)))
            }
            Expr::Update(updates, input) => Expr::Update(updates, Box::new(visitor(*input))),
            Expr::Delete(table, input) => Expr::Delete(table, Box::new(visitor(*input))),
            Expr::CreateDatabase(name) => Expr::CreateDatabase(name),
            Expr::CreateTable {
                name,
                columns,
                partition_by,
                cluster_by,
                primary_key,
                input,
            } => Expr::CreateTable {
                name,
                columns,
                partition_by,
                cluster_by,
                primary_key,
                input: input.map(|input| Box::new(visitor(*input))),
            },
            Expr::CreateIndex {
                name,
                table,
                columns,
            } => Expr::CreateIndex {
                name,
                table,
                columns,
            },
            Expr::AlterTable { name, actions } => Expr::AlterTable { name, actions },
            Expr::Drop { object, name } => Expr::Drop { object, name },
            Expr::Rename { object, from, to } => Expr::Rename { object, from, to },
            Expr::Leaf(id) => Expr::Leaf(id),
        }
    }

    pub fn bottom_up_rewrite(self, visitor: &impl Fn(Expr) -> Expr) -> Expr {
        let operator = self.map(|child| child.bottom_up_rewrite(visitor));
        visitor(operator)
    }

    pub fn top_down_rewrite(self, visitor: &impl Fn(Expr) -> Expr) -> Expr {
        let expr = visitor(self);
        expr.map(|child| child.top_down_rewrite(visitor))
    }

    pub fn iter(&self) -> ExprIterator {
        ExprIterator {
            parent: self,
            offset: 0,
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
            | Expr::LogicalWith(_, _, left, right)
            | Expr::LogicalUnion(left, right)
            | Expr::LogicalIntersect(left, right)
            | Expr::LogicalExcept(left, right)
            | Expr::NestedLoop(_, left, right)
            | Expr::HashJoin { left, right, .. }
            | Expr::CreateTempTable(_, _, left, right)
            | Expr::Union(left, right)
            | Expr::Intersect(left, right)
            | Expr::Except(left, right) => match index {
                0 => left,
                1 => right,
                _ => panic!("{} is out of bounds [0,2)", index),
            },
            Expr::LogicalFilter(_, input)
            | Expr::LogicalMap { input, .. }
            | Expr::LogicalAggregate { input, .. }
            | Expr::LogicalLimit { input, .. }
            | Expr::LogicalSort(_, input)
            | Expr::LogicalInsert(_, _, input)
            | Expr::LogicalValues(_, _, input)
            | Expr::LogicalUpdate(_, input)
            | Expr::LogicalDelete(_, input)
            | Expr::LogicalCreateTable {
                input: Some(input), ..
            }
            | Expr::Filter(_, input)
            | Expr::Map { input, .. }
            | Expr::LookupJoin { input, .. }
            | Expr::Aggregate { input, .. }
            | Expr::Limit { input, .. }
            | Expr::Sort(_, input)
            | Expr::Insert(_, _, input)
            | Expr::Values(_, _, input)
            | Expr::Update(_, input)
            | Expr::Delete(_, input)
            | Expr::CreateTable {
                input: Some(input), ..
            } => match index {
                0 => input,
                _ => panic!("{} is out of bounds [0,1)", index),
            },
            Expr::Leaf(_)
            | Expr::LogicalSingleGet { .. }
            | Expr::LogicalGet { .. }
            | Expr::LogicalGetWith { .. }
            | Expr::LogicalCreateDatabase { .. }
            | Expr::LogicalCreateTable { input: None, .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalAlterTable { .. }
            | Expr::LogicalDrop { .. }
            | Expr::LogicalRename { .. }
            | Expr::TableFreeScan { .. }
            | Expr::SeqScan { .. }
            | Expr::IndexScan { .. }
            | Expr::GetTempTable { .. }
            | Expr::CreateDatabase { .. }
            | Expr::CreateTable { input: None, .. }
            | Expr::CreateIndex { .. }
            | Expr::AlterTable { .. }
            | Expr::Drop { .. }
            | Expr::Rename { .. } => panic!("{} has no inputs", self.name()),
        }
    }
}

pub trait Scope {
    fn attributes(&self) -> HashSet<Column>;
    fn references(&self) -> HashSet<Column>;
    fn subst(self, map: &HashMap<Column, Column>) -> Self;
}

impl Scope for Expr {
    fn attributes(&self) -> HashSet<Column> {
        match self {
            Expr::LogicalGet { projects, .. } => projects.iter().map(|c| c.clone()).collect(),
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
            Expr::LogicalFilter(_, input)
            | Expr::LogicalWith(_, _, _, input)
            | Expr::LogicalLimit { input, .. }
            | Expr::LogicalSort(_, input)
            | Expr::LogicalUnion(_, input)
            | Expr::LogicalIntersect(_, input)
            | Expr::LogicalExcept(_, input)
            | Expr::LogicalJoin {
                join: Join::Semi(_),
                right: input,
                ..
            }
            | Expr::LogicalJoin {
                join: Join::Anti(_),
                right: input,
                ..
            }
            | Expr::LogicalJoin {
                join: Join::Single(_),
                right: input,
                ..
            } => input.attributes(),
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
                join: Join::Inner(_),
                left,
                right,
                ..
            }
            | Expr::LogicalJoin {
                join: Join::Right(_),
                left,
                right,
                ..
            }
            | Expr::LogicalJoin {
                join: Join::Outer(_),
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
            Expr::LogicalGetWith(_, columns) | Expr::LogicalValues(columns, _, _) => {
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
            any if any.is_logical() => HashSet::new(),
            any => unimplemented!(
                "attributes is not implemented for physical operator {}",
                any.name()
            ),
        }
    }

    fn references(&self) -> HashSet<Column> {
        let mut set = HashSet::new();
        match self {
            Expr::LogicalGet { predicates, .. } | Expr::LogicalFilter(predicates, _) => {
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
            Expr::LogicalWith(_, columns, _, _) | Expr::LogicalGetWith(_, columns) => {
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
            Expr::LogicalSort(order_by, _) => {
                for o in order_by {
                    set.insert(o.column.clone());
                }
            }
            Expr::LogicalValues(columns, rows, _) => {
                set.extend(columns.clone());
                for row in rows {
                    for x in row {
                        set.extend(x.references());
                    }
                }
            }
            any if any.is_logical() => {}
            any => unimplemented!(
                "free is not implemented for physical operator {}",
                any.name()
            ),
        }
        for i in 0..self.len() {
            set.extend(self[i].references());
        }
        set
    }

    fn subst(self, map: &HashMap<Column, Column>) -> Self {
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
            Expr::LogicalFilter(predicates, input) => Expr::LogicalFilter(
                predicates.iter().map(subst_x).collect(),
                Box::new(input.subst(map)),
            ),
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
            Expr::LogicalWith(name, columns, left, right) => Expr::LogicalWith(
                name.clone(),
                columns.iter().map(subst_c).collect(),
                Box::new(left.subst(map)),
                Box::new(right.subst(map)),
            ),
            Expr::LogicalGetWith(name, columns) => {
                Expr::LogicalGetWith(name.clone(), columns.iter().map(subst_c).collect())
            }
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
            Expr::LogicalSort(order_by, input) => Expr::LogicalSort(
                order_by.iter().map(subst_o).collect(),
                Box::new(input.subst(map)),
            ),
            Expr::LogicalValues(columns, rows, input) => Expr::LogicalValues(
                columns.iter().map(subst_c).collect(),
                rows.iter()
                    .map(|row| row.iter().map(subst_x).collect())
                    .collect(),
                Box::new(input.subst(map)),
            ),
            any if any.is_logical() => any.map(|child| child.subst(map)),
            any => unimplemented!(
                "subst is not implemented for physical operator {}",
                any.name()
            ),
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
pub enum Alter {
    AddColumn { name: String, data: DataType },
    DropColumn { name: String },
}

impl fmt::Display for Alter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Alter::AddColumn { name, data } => {
                write!(f, "(AddColumn {}:{})", name, data_type::to_string(data))
            }
            Alter::DropColumn { name } => write!(f, "(DropColumn {})", name),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Scalar {
    Literal(Value, DataType),
    Column(Column),
    Call(Function, Vec<Scalar>, DataType),
    Cast(Box<Scalar>, DataType),
}

impl Scalar {
    pub fn data(&self) -> DataType {
        match self {
            Scalar::Literal(_, data) => data.clone(),
            Scalar::Column(column) => column.data.clone(),
            Scalar::Call(_, _, returns) => returns.clone(),
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
            Scalar::Literal(_, _) => {}
            Scalar::Column(column) => {
                free.insert(column.clone());
            }
            Scalar::Call(_, arguments, _) => {
                for a in arguments {
                    a.collect_references(free);
                }
            }
            Scalar::Cast(scalar, _) => scalar.collect_references(free),
        }
    }

    pub fn subst(self, map: &HashMap<Column, Column>) -> Self {
        match self {
            Scalar::Column(c) if map.contains_key(&c) => Scalar::Column(map[&c].clone()),
            Scalar::Call(f, args, t) => {
                Scalar::Call(f, args.iter().map(|x| x.clone().subst(map)).collect(), t)
            }
            Scalar::Cast(x, t) => Scalar::Cast(Box::new(x.subst(map)), t),
            _ => self,
        }
    }

    pub fn inline(&self, expr: &Scalar, column: &Column) -> Scalar {
        match self {
            Scalar::Column(c) if c == column => expr.clone(),
            Scalar::Call(f, arguments, returns) => {
                let mut inline_arguments = Vec::with_capacity(arguments.len());
                for a in arguments {
                    inline_arguments.push(a.inline(expr, column));
                }
                Scalar::Call(f.clone(), inline_arguments, returns.clone())
            }
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
    Abs,
    Acos,
    Acosh,
    Add,
    And,
    Asin,
    Asinh,
    Atan,
    Atan2,
    Atanh,
    Between,
    BitCount,
    BitwiseAnd,
    BitwiseLeftShift,
    BitwiseNot,
    BitwiseOr,
    BitwiseRightShift,
    BitwiseXor,
    ByteLength,
    CaseNoValue,
    CaseWithValue,
    Ceil,
    CharLength,
    Coalesce,
    Concat,
    Cos,
    Cosh,
    CurrentDate,
    CurrentTimestamp,
    Date,
    DateAdd,
    DateDiff,
    DateFromUnixDate,
    DateSub,
    DateTrunc,
    Div,
    Divide,
    EndsWith,
    Equal,
    Exp,
    Extract,
    ExtractDate,
    Floor,
    FromBase64,
    FromHex,
    Greater,
    GreaterOrEqual,
    Greatest,
    If,
    Ifnull,
    In,
    IsInf,
    IsNan,
    IsNull,
    Least,
    Length,
    Less,
    LessOrEqual,
    Like,
    Ln,
    Log,
    Log10,
    Lower,
    Lpad,
    Ltrim,
    Mod,
    Multiply,
    Not,
    NotEqual,
    Or,
    Pow,
    Rand,
    RegexpContains,
    RegexpExtract,
    RegexpExtractAll,
    RegexpReplace,
    Repeat,
    Replace,
    Reverse,
    Round,
    Rpad,
    Rtrim,
    Sign,
    Sin,
    Sinh,
    Split,
    Sqrt,
    StartsWith,
    Strpos,
    Substr,
    Subtract,
    Tan,
    Tanh,
    Timestamp,
    TimestampAdd,
    TimestampDiff,
    TimestampFromUnixMicros,
    TimestampFromUnixMillis,
    TimestampFromUnixSeconds,
    TimestampMicros,
    TimestampMillis,
    TimestampSeconds,
    TimestampSub,
    TimestampTrunc,
    ToBase64,
    ToHex,
    Trim,
    Trunc,
    UnaryMinus,
    UnixDate,
    UnixMicrosFromTimestamp,
    UnixMillisFromTimestamp,
    UnixSecondsFromTimestamp,
    Upper,
}

impl Function {
    pub fn from(name: String) -> Self {
        match name.as_str() {
            "ZetaSQL:abs" => Function::Abs,
            "ZetaSQL:acos" => Function::Acos,
            "ZetaSQL:acosh" => Function::Acosh,
            "ZetaSQL:$add" => Function::Add,
            "ZetaSQL:$and" => Function::And,
            "ZetaSQL:asin" => Function::Asin,
            "ZetaSQL:asinh" => Function::Asinh,
            "ZetaSQL:atan" => Function::Atan,
            "ZetaSQL:atan2" => Function::Atan2,
            "ZetaSQL:atanh" => Function::Atanh,
            "ZetaSQL:$between" => Function::Between,
            "ZetaSQL:$bitwise_and" => Function::BitwiseAnd,
            "ZetaSQL:$bitwise_left_shift" => Function::BitwiseLeftShift,
            "ZetaSQL:$bitwise_not" => Function::BitwiseNot,
            "ZetaSQL:$bitwise_or" => Function::BitwiseOr,
            "ZetaSQL:$bitwise_right_shift" => Function::BitwiseRightShift,
            "ZetaSQL:$bitwise_xor" => Function::BitwiseXor,
            "ZetaSQL:byte_length" => Function::ByteLength,
            "ZetaSQL:$case_no_value" => Function::CaseNoValue,
            "ZetaSQL:$case_with_value" => Function::CaseWithValue,
            "ZetaSQL:ceil" => Function::Ceil,
            "ZetaSQL:char_length" => Function::CharLength,
            "ZetaSQL:coalesce" => Function::Coalesce,
            "ZetaSQL:concat" => Function::Concat,
            "ZetaSQL:cos" => Function::Cos,
            "ZetaSQL:cosh" => Function::Cosh,
            "ZetaSQL:current_date" => Function::CurrentDate,
            "ZetaSQL:current_timestamp" => Function::CurrentTimestamp,
            "ZetaSQL:date" => Function::Date,
            "ZetaSQL:date_add" => Function::DateAdd,
            "ZetaSQL:date_diff" => Function::DateDiff,
            "ZetaSQL:date_from_unix_date" => Function::DateFromUnixDate,
            "ZetaSQL:date_sub" => Function::DateSub,
            "ZetaSQL:date_trunc" => Function::DateTrunc,
            "ZetaSQL:div" => Function::Div,
            "ZetaSQL:$divide" => Function::Divide,
            "ZetaSQL:ends_with" => Function::EndsWith,
            "ZetaSQL:$equal" => Function::Equal,
            "ZetaSQL:exp" => Function::Exp,
            "ZetaSQL:$extract" => Function::Extract,
            "ZetaSQL:$extract_date" => Function::ExtractDate,
            "ZetaSQL:floor" => Function::Floor,
            "ZetaSQL:from_base64" => Function::FromBase64,
            "ZetaSQL:from_hex" => Function::FromHex,
            "ZetaSQL:$greater" => Function::Greater,
            "ZetaSQL:$greater_or_equal" => Function::GreaterOrEqual,
            "ZetaSQL:greatest" => Function::Greatest,
            "ZetaSQL:if" => Function::If,
            "ZetaSQL:ifnull" => Function::Ifnull,
            "ZetaSQL:$in" => Function::In,
            "ZetaSQL:is_inf" => Function::IsInf,
            "ZetaSQL:is_nan" => Function::IsNan,
            "ZetaSQL:$is_null" => Function::IsNull,
            "ZetaSQL:least" => Function::Least,
            "ZetaSQL:length" => Function::Length,
            "ZetaSQL:$less" => Function::Less,
            "ZetaSQL:$less_or_equal" => Function::LessOrEqual,
            "ZetaSQL:$like" => Function::Like,
            "ZetaSQL:ln" => Function::Ln,
            "ZetaSQL:log" => Function::Log,
            "ZetaSQL:log10" => Function::Log10,
            "ZetaSQL:lower" => Function::Lower,
            "ZetaSQL:lpad" => Function::Lpad,
            "ZetaSQL:ltrim" => Function::Ltrim,
            "ZetaSQL:mod" => Function::Mod,
            "ZetaSQL:$multiply" => Function::Multiply,
            "ZetaSQL:$not" => Function::Not,
            "ZetaSQL:$not_equal" => Function::NotEqual,
            "ZetaSQL:$or" => Function::Or,
            "ZetaSQL:pow" => Function::Pow,
            "ZetaSQL:rand" => Function::Rand,
            "ZetaSQL:regexp_contains" => Function::RegexpContains,
            "ZetaSQL:regexp_extract" => Function::RegexpExtract,
            "ZetaSQL:regexp_extract_all" => Function::RegexpExtractAll,
            "ZetaSQL:regexp_replace" => Function::RegexpReplace,
            "ZetaSQL:repeat" => Function::Repeat,
            "ZetaSQL:replace" => Function::Replace,
            "ZetaSQL:reverse" => Function::Reverse,
            "ZetaSQL:round" => Function::Round,
            "ZetaSQL:rpad" => Function::Rpad,
            "ZetaSQL:rtrim" => Function::Rtrim,
            "ZetaSQL:sign" => Function::Sign,
            "ZetaSQL:sin" => Function::Sin,
            "ZetaSQL:sinh" => Function::Sinh,
            "ZetaSQL:split" => Function::Split,
            "ZetaSQL:sqrt" => Function::Sqrt,
            "ZetaSQL:starts_with" => Function::StartsWith,
            "ZetaSQL:strpos" => Function::Strpos,
            "ZetaSQL:substr" => Function::Substr,
            "ZetaSQL:$subtract" => Function::Subtract,
            "ZetaSQL:tan" => Function::Tan,
            "ZetaSQL:tanh" => Function::Tanh,
            "ZetaSQL:timestamp" => Function::Timestamp,
            "ZetaSQL:timestamp_add" => Function::TimestampAdd,
            "ZetaSQL:timestamp_diff" => Function::TimestampDiff,
            "ZetaSQL:timestamp_from_unix_micros" => Function::TimestampFromUnixMicros,
            "ZetaSQL:timestamp_from_unix_millis" => Function::TimestampFromUnixMillis,
            "ZetaSQL:timestamp_from_unix_seconds" => Function::TimestampFromUnixSeconds,
            "ZetaSQL:timestamp_micros" => Function::TimestampMicros,
            "ZetaSQL:timestamp_millis" => Function::TimestampMillis,
            "ZetaSQL:timestamp_seconds" => Function::TimestampSeconds,
            "ZetaSQL:timestamp_sub" => Function::TimestampSub,
            "ZetaSQL:timestamp_trunc" => Function::TimestampTrunc,
            "ZetaSQL:to_base64" => Function::ToBase64,
            "ZetaSQL:to_hex" => Function::ToHex,
            "ZetaSQL:trim" => Function::Trim,
            "ZetaSQL:trunc" => Function::Trunc,
            "ZetaSQL:$unary_minus" => Function::UnaryMinus,
            "ZetaSQL:unix_date" => Function::UnixDate,
            "ZetaSQL:unix_seconds" => Function::UnixSecondsFromTimestamp,
            "ZetaSQL:unix_millis" => Function::UnixMillisFromTimestamp,
            "ZetaSQL:unix_micros" => Function::UnixMicrosFromTimestamp,
            "ZetaSQL:upper" => Function::Upper,
            other => panic!("{} is not supported", other),
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
