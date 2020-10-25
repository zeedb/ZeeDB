use std::cmp;
use std::collections::HashSet;
use std::fmt;
use std::ops;

// Operator plan nodes combine inputs in a Plan tree.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Operator<T> {
    // LogicalSingleGet acts as the input to a SELECT with no FROM clause.
    LogicalSingleGet,
    // LogicalGet(table) implements the FROM clause.
    LogicalGet {
        projects: Vec<Column>,
        predicates: Vec<Scalar>,
        table: Table,
    },
    // LogicalFilter(predicates) implements the WHERE/HAVING clauses.
    LogicalFilter(Vec<Scalar>, T),
    // LogicalMap(columns) implements the SELECT clause.
    LogicalMap(Vec<(Scalar, Column)>, T),
    // LogicalJoin implements the JOIN operator, and correlated subqueries if parameters is non-empty.
    LogicalJoin {
        parameters: Vec<Column>,
        join: Join,
        left: T,
        right: T,
    },
    // LogicalProject implements the *relational* projection operator, which removes duplicates from a multiset.
    LogicalProject(Vec<Column>, T),
    // LogicalWith(table) implements with subquery as  _.
    // The with-subquery is always on the left.
    LogicalWith(String, Vec<Column>, T, T),
    // LogicalGetWith(table) reads the subquery that was created by With.
    LogicalGetWith(String, Vec<Column>),
    // LogicalAggregate(group_by, aggregate) implements the GROUP BY clause.
    LogicalAggregate {
        group_by: Vec<Column>,
        aggregate: Vec<(AggregateFn, Column)>,
        input: T,
    },
    // LogicalLimit(n) implements the LIMIT / OFFSET / TOP clause.
    LogicalLimit {
        limit: usize,
        offset: usize,
        input: T,
    },
    // LogicalSort(columns) implements the ORDER BY clause.
    LogicalSort(Vec<OrderBy>, T),
    // LogicalUnion implements SELECT _ UNION ALL SELECT _.
    LogicalUnion(T, T),
    // LogicalIntersect implements SELECT _ INTERSECT SELECT _.
    LogicalIntersect(T, T),
    // LogicalExcept implements SELECT _ EXCEPT SELECT _.
    LogicalExcept(T, T),
    // LogicalInsert(table, columns) implements the INSERT operation.
    LogicalInsert(Table, Vec<Column>, T),
    // LogicalValues(rows, columns) implements VALUES expressions.
    LogicalValues(Vec<Column>, Vec<Vec<Scalar>>, T),
    // LogicalUpdate(sets) implements the UPDATE operation.
    // (column, None) indicates set column to default value.
    LogicalUpdate(Vec<(Column, Option<Column>)>, T),
    // LogicalDelete(table) implements the DELETE operation.
    LogicalDelete(Table, T),
    // LogicalCreateDatabase(database) implements the CREATE DATABASE operation.
    LogicalCreateDatabase(Name),
    // LogicalCreateTable implements the CREATE TABLE operation.
    LogicalCreateTable {
        name: Name,
        columns: Vec<(String, encoding::Type)>,
        partition_by: Vec<i64>,
        cluster_by: Vec<i64>,
        primary_key: Vec<i64>,
        input: Option<T>,
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
        table: Table,
        equals: Vec<(Column, Scalar)>,
    },
    Filter(Vec<Scalar>, T),
    Map(Vec<(Scalar, Column)>, T),
    NestedLoop(Join, T, T),
    HashJoin(Join, Vec<(Scalar, Scalar)>, T, T),
    CreateTempTable(String, Vec<Column>, T, T),
    GetTempTable(String, Vec<Column>),
    Aggregate {
        group_by: Vec<Column>,
        aggregate: Vec<(AggregateFn, Column)>,
        input: T,
    },
    Project(Vec<Column>, T),
    Limit {
        limit: usize,
        offset: usize,
        input: T,
    },
    Sort(Vec<OrderBy>, T),
    Union(T, T),
    Intersect(T, T),
    Except(T, T),
    Insert(Table, Vec<Column>, T),
    Values(Vec<Column>, Vec<Vec<Scalar>>, T),
    Update(Vec<(Column, Option<Column>)>, T),
    Delete(Table, T),
    CreateDatabase(Name),
    CreateTable {
        name: Name,
        columns: Vec<(String, encoding::Type)>,
        partition_by: Vec<i64>,
        cluster_by: Vec<i64>,
        primary_key: Vec<i64>,
        input: Option<T>,
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

impl<T> Operator<T> {
    pub fn is_logical(&self) -> bool {
        match self {
            Operator::LogicalJoin { .. }
            | Operator::LogicalProject { .. }
            | Operator::LogicalWith(_, _, _, _)
            | Operator::LogicalUnion(_, _)
            | Operator::LogicalIntersect(_, _)
            | Operator::LogicalExcept(_, _)
            | Operator::LogicalFilter(_, _)
            | Operator::LogicalMap { .. }
            | Operator::LogicalAggregate { .. }
            | Operator::LogicalLimit { .. }
            | Operator::LogicalSort(_, _)
            | Operator::LogicalInsert(_, _, _)
            | Operator::LogicalValues(_, _, _)
            | Operator::LogicalUpdate(_, _)
            | Operator::LogicalDelete(_, _)
            | Operator::LogicalSingleGet
            | Operator::LogicalGet { .. }
            | Operator::LogicalGetWith(_, _)
            | Operator::LogicalCreateDatabase(_)
            | Operator::LogicalCreateTable { .. }
            | Operator::LogicalCreateIndex { .. }
            | Operator::LogicalAlterTable { .. }
            | Operator::LogicalDrop { .. }
            | Operator::LogicalRename { .. } => true,
            Operator::TableFreeScan { .. }
            | Operator::SeqScan { .. }
            | Operator::IndexScan { .. }
            | Operator::Filter { .. }
            | Operator::Map { .. }
            | Operator::NestedLoop { .. }
            | Operator::HashJoin { .. }
            | Operator::CreateTempTable { .. }
            | Operator::GetTempTable { .. }
            | Operator::Aggregate { .. }
            | Operator::Project { .. }
            | Operator::Limit { .. }
            | Operator::Sort { .. }
            | Operator::Union { .. }
            | Operator::Intersect { .. }
            | Operator::Except { .. }
            | Operator::Insert { .. }
            | Operator::Values { .. }
            | Operator::Update { .. }
            | Operator::Delete { .. }
            | Operator::CreateDatabase { .. }
            | Operator::CreateTable { .. }
            | Operator::CreateIndex { .. }
            | Operator::AlterTable { .. }
            | Operator::Drop { .. }
            | Operator::Rename { .. } => false,
        }
    }

    pub fn has_side_effects(&self) -> bool {
        match self {
            Operator::LogicalInsert { .. }
            | Operator::LogicalUpdate { .. }
            | Operator::LogicalDelete { .. }
            | Operator::LogicalCreateDatabase { .. }
            | Operator::LogicalCreateTable { .. }
            | Operator::LogicalCreateIndex { .. }
            | Operator::LogicalAlterTable { .. }
            | Operator::LogicalDrop { .. }
            | Operator::LogicalRename { .. } => true,
            _ if self.is_logical() => false,
            _ => panic!(
                "has_side_effecs is not implemented for physical operator {}",
                self.name()
            ),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Operator::LogicalJoin { .. }
            | Operator::LogicalWith(_, _, _, _)
            | Operator::LogicalUnion(_, _)
            | Operator::LogicalIntersect(_, _)
            | Operator::LogicalExcept(_, _)
            | Operator::NestedLoop { .. }
            | Operator::HashJoin { .. }
            | Operator::CreateTempTable { .. }
            | Operator::Union { .. }
            | Operator::Intersect { .. }
            | Operator::Except { .. } => 2,
            Operator::LogicalFilter(_, _)
            | Operator::LogicalMap { .. }
            | Operator::LogicalProject { .. }
            | Operator::LogicalAggregate { .. }
            | Operator::LogicalLimit { .. }
            | Operator::LogicalSort(_, _)
            | Operator::LogicalInsert(_, _, _)
            | Operator::LogicalValues(_, _, _)
            | Operator::LogicalUpdate(_, _)
            | Operator::LogicalDelete(_, _)
            | Operator::LogicalCreateTable { input: Some(_), .. }
            | Operator::Filter { .. }
            | Operator::Map { .. }
            | Operator::Aggregate { .. }
            | Operator::Project { .. }
            | Operator::Limit { .. }
            | Operator::Sort { .. }
            | Operator::Insert { .. }
            | Operator::Values { .. }
            | Operator::Update { .. }
            | Operator::Delete { .. }
            | Operator::CreateTable { input: Some(_), .. } => 1,
            Operator::LogicalSingleGet
            | Operator::LogicalGet { .. }
            | Operator::LogicalGetWith(_, _)
            | Operator::LogicalCreateDatabase(_)
            | Operator::LogicalCreateTable { input: None, .. }
            | Operator::LogicalCreateIndex { .. }
            | Operator::LogicalAlterTable { .. }
            | Operator::LogicalDrop { .. }
            | Operator::LogicalRename { .. }
            | Operator::TableFreeScan
            | Operator::SeqScan { .. }
            | Operator::IndexScan { .. }
            | Operator::GetTempTable { .. }
            | Operator::CreateDatabase { .. }
            | Operator::CreateTable { input: None, .. }
            | Operator::CreateIndex { .. }
            | Operator::AlterTable { .. }
            | Operator::Drop { .. }
            | Operator::Rename { .. } => 0,
        }
    }

    pub fn map<U>(self, mut visitor: impl FnMut(T) -> U) -> Operator<U> {
        match self {
            Operator::LogicalFilter(predicates, input) => {
                let input = visitor(input);
                Operator::LogicalFilter(predicates, input)
            }
            Operator::LogicalMap(projects, input) => {
                let input = visitor(input);
                Operator::LogicalMap(projects, input)
            }
            Operator::LogicalJoin {
                parameters,
                join,
                left,
                right,
            } => {
                let left = visitor(left);
                let right = visitor(right);
                Operator::LogicalJoin {
                    parameters,
                    join,
                    left,
                    right,
                }
            }
            Operator::LogicalProject(projects, input) => {
                let input = visitor(input);
                Operator::LogicalProject(projects, input)
            }
            Operator::LogicalWith(name, columns, left, right) => {
                let left = visitor(left);
                let right = visitor(right);
                Operator::LogicalWith(name, columns, left, right)
            }
            Operator::LogicalAggregate {
                group_by,
                aggregate,
                input,
            } => {
                let input = visitor(input);
                Operator::LogicalAggregate {
                    group_by,
                    aggregate,
                    input,
                }
            }
            Operator::LogicalLimit {
                limit,
                offset,
                input,
            } => {
                let input = visitor(input);
                Operator::LogicalLimit {
                    limit,
                    offset,
                    input,
                }
            }
            Operator::LogicalSort(sorts, input) => {
                let input = visitor(input);
                Operator::LogicalSort(sorts, input)
            }
            Operator::LogicalUnion(left, right) => {
                let left = visitor(left);
                let right = visitor(right);
                Operator::LogicalUnion(left, right)
            }
            Operator::LogicalIntersect(left, right) => {
                let left = visitor(left);
                let right = visitor(right);
                Operator::LogicalIntersect(left, right)
            }
            Operator::LogicalExcept(left, right) => {
                let left = visitor(left);
                let right = visitor(right);
                Operator::LogicalExcept(left, right)
            }
            Operator::LogicalInsert(table, columns, input) => {
                let input = visitor(input);
                Operator::LogicalInsert(table, columns, input)
            }
            Operator::LogicalValues(columns, rows, input) => {
                let input = visitor(input);
                Operator::LogicalValues(columns, rows, input)
            }
            Operator::LogicalUpdate(updates, input) => {
                let input = visitor(input);
                Operator::LogicalUpdate(updates, input)
            }
            Operator::LogicalDelete(table, input) => {
                let input = visitor(input);
                Operator::LogicalDelete(table, input)
            }
            Operator::LogicalSingleGet => Operator::LogicalSingleGet,
            Operator::LogicalGet {
                projects,
                predicates,
                table,
            } => Operator::LogicalGet {
                projects,
                predicates,
                table,
            },
            Operator::LogicalGetWith(name, columns) => Operator::LogicalGetWith(name, columns),
            Operator::LogicalCreateDatabase(name) => Operator::LogicalCreateDatabase(name),
            Operator::LogicalCreateTable {
                name,
                columns,
                partition_by,
                cluster_by,
                primary_key,
                input,
            } => Operator::LogicalCreateTable {
                name,
                columns,
                partition_by,
                cluster_by,
                primary_key,
                input: input.map(visitor),
            },
            Operator::LogicalCreateIndex {
                name,
                table,
                columns,
            } => Operator::LogicalCreateIndex {
                name,
                table,
                columns,
            },
            Operator::LogicalAlterTable { name, actions } => {
                Operator::LogicalAlterTable { name, actions }
            }
            Operator::LogicalDrop { object, name } => Operator::LogicalDrop { object, name },
            Operator::LogicalRename { object, from, to } => {
                Operator::LogicalRename { object, from, to }
            }
            Operator::TableFreeScan => Operator::TableFreeScan,
            Operator::SeqScan {
                projects,
                predicates,
                table,
            } => Operator::SeqScan {
                projects,
                predicates,
                table,
            },
            Operator::IndexScan {
                projects,
                predicates,
                table,
                equals,
            } => Operator::IndexScan {
                projects,
                predicates,
                table,
                equals,
            },
            Operator::Filter(predicates, input) => Operator::Filter(predicates, visitor(input)),
            Operator::Map(projects, input) => Operator::Map(projects, visitor(input)),
            Operator::NestedLoop(join, left, right) => {
                Operator::NestedLoop(join, visitor(left), visitor(right))
            }
            Operator::HashJoin(join, equals, left, right) => {
                Operator::HashJoin(join, equals, visitor(left), visitor(right))
            }
            Operator::CreateTempTable(name, columns, left, right) => {
                Operator::CreateTempTable(name, columns, visitor(left), visitor(right))
            }
            Operator::GetTempTable(name, columns) => Operator::GetTempTable(name, columns),
            Operator::Aggregate {
                group_by,
                aggregate,
                input,
            } => Operator::Aggregate {
                group_by,
                aggregate,
                input: visitor(input),
            },
            Operator::Project(projects, input) => Operator::Project(projects, visitor(input)),
            Operator::Limit {
                limit,
                offset,
                input,
            } => Operator::Limit {
                limit,
                offset,
                input: visitor(input),
            },
            Operator::Sort(order_by, input) => Operator::Sort(order_by, visitor(input)),
            Operator::Union(left, right) => Operator::Union(visitor(left), visitor(right)),
            Operator::Intersect(left, right) => Operator::Intersect(visitor(left), visitor(right)),
            Operator::Except(left, right) => Operator::Except(visitor(left), visitor(right)),
            Operator::Insert(table, columns, input) => {
                Operator::Insert(table, columns, visitor(input))
            }
            Operator::Values(columns, rows, input) => {
                Operator::Values(columns, rows, visitor(input))
            }
            Operator::Update(updates, input) => Operator::Update(updates, visitor(input)),
            Operator::Delete(table, input) => Operator::Delete(table, visitor(input)),
            Operator::CreateDatabase(name) => Operator::CreateDatabase(name),
            Operator::CreateTable {
                name,
                columns,
                partition_by,
                cluster_by,
                primary_key,
                input,
            } => Operator::CreateTable {
                name,
                columns,
                partition_by,
                cluster_by,
                primary_key,
                input: input.map(visitor),
            },
            Operator::CreateIndex {
                name,
                table,
                columns,
            } => Operator::CreateIndex {
                name,
                table,
                columns,
            },
            Operator::AlterTable { name, actions } => Operator::AlterTable { name, actions },
            Operator::Drop { object, name } => Operator::Drop { object, name },
            Operator::Rename { object, from, to } => Operator::Rename { object, from, to },
        }
    }
}

impl<T> ops::Index<usize> for Operator<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            Operator::LogicalJoin { left, right, .. }
            | Operator::LogicalWith(_, _, left, right)
            | Operator::LogicalUnion(left, right)
            | Operator::LogicalIntersect(left, right)
            | Operator::LogicalExcept(left, right)
            | Operator::NestedLoop(_, left, right)
            | Operator::HashJoin(_, _, left, right)
            | Operator::CreateTempTable(_, _, left, right)
            | Operator::Union(left, right)
            | Operator::Intersect(left, right)
            | Operator::Except(left, right) => match index {
                0 => left,
                1 => right,
                _ => panic!("{} is out of bounds [0,2)", index),
            },
            Operator::LogicalFilter(_, input)
            | Operator::LogicalProject(_, input)
            | Operator::LogicalMap(_, input)
            | Operator::LogicalAggregate { input, .. }
            | Operator::LogicalLimit { input, .. }
            | Operator::LogicalSort(_, input)
            | Operator::LogicalInsert(_, _, input)
            | Operator::LogicalValues(_, _, input)
            | Operator::LogicalUpdate(_, input)
            | Operator::LogicalDelete(_, input)
            | Operator::LogicalCreateTable {
                input: Some(input), ..
            }
            | Operator::Filter(_, input)
            | Operator::Map(_, input)
            | Operator::Aggregate { input, .. }
            | Operator::Project(_, input)
            | Operator::Limit { input, .. }
            | Operator::Sort(_, input)
            | Operator::Insert(_, _, input)
            | Operator::Values(_, _, input)
            | Operator::Update(_, input)
            | Operator::Delete(_, input)
            | Operator::CreateTable {
                input: Some(input), ..
            } => match index {
                0 => input,
                _ => panic!("{} is out of bounds [0,1)", index),
            },
            Operator::LogicalSingleGet { .. }
            | Operator::LogicalGet { .. }
            | Operator::LogicalGetWith { .. }
            | Operator::LogicalCreateDatabase { .. }
            | Operator::LogicalCreateTable { input: None, .. }
            | Operator::LogicalCreateIndex { .. }
            | Operator::LogicalAlterTable { .. }
            | Operator::LogicalDrop { .. }
            | Operator::LogicalRename { .. }
            | Operator::TableFreeScan { .. }
            | Operator::SeqScan { .. }
            | Operator::IndexScan { .. }
            | Operator::GetTempTable { .. }
            | Operator::CreateDatabase { .. }
            | Operator::CreateTable { input: None, .. }
            | Operator::CreateIndex { .. }
            | Operator::AlterTable { .. }
            | Operator::Drop { .. }
            | Operator::Rename { .. } => panic!("{} has no inputs", self.name()),
        }
    }
}

pub trait Scope {
    fn attributes(&self) -> HashSet<Column>;
    // TODO this isn't actually free, this is all column references.
    fn free(&self) -> HashSet<Column>;
}

impl<T: Scope> Scope for Operator<T> {
    fn attributes(&self) -> HashSet<Column> {
        match self {
            Operator::LogicalGet { projects, .. } => projects.iter().map(|c| c.clone()).collect(),
            Operator::LogicalMap(projects, ..) => projects.iter().map(|(_, c)| c.clone()).collect(),
            Operator::LogicalFilter(_, input)
            | Operator::LogicalWith(_, _, _, input)
            | Operator::LogicalLimit { input, .. }
            | Operator::LogicalSort(_, input)
            | Operator::LogicalUnion(_, input)
            | Operator::LogicalIntersect(_, input)
            | Operator::LogicalExcept(_, input)
            | Operator::LogicalJoin {
                join: Join::Semi(_),
                right: input,
                ..
            }
            | Operator::LogicalJoin {
                join: Join::Anti(_),
                right: input,
                ..
            }
            | Operator::LogicalJoin {
                join: Join::Single(_),
                right: input,
                ..
            } => input.attributes(),
            Operator::LogicalJoin {
                join: Join::Mark(mark, _),
                right,
                ..
            } => {
                let mut set = HashSet::new();
                for c in right.attributes() {
                    set.insert(c.clone());
                }
                set.insert(mark.clone());
                set
            }
            Operator::LogicalJoin {
                join: Join::Inner(_),
                left,
                right,
                ..
            }
            | Operator::LogicalJoin {
                join: Join::Right(_),
                left,
                right,
                ..
            }
            | Operator::LogicalJoin {
                join: Join::Outer(_),
                left,
                right,
                ..
            } => {
                let mut set = HashSet::new();
                for c in left.attributes() {
                    set.insert(c.clone());
                }
                for c in right.attributes() {
                    set.insert(c.clone());
                }
                set
            }
            Operator::LogicalProject(columns, _)
            | Operator::LogicalGetWith(_, columns)
            | Operator::LogicalValues(columns, _, _) => columns.iter().map(|c| c.clone()).collect(),
            Operator::LogicalAggregate {
                group_by,
                aggregate,
                ..
            } => {
                let mut set = HashSet::new();
                for c in group_by {
                    set.insert(c.clone());
                }
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

    fn free(&self) -> HashSet<Column> {
        let mut set = HashSet::new();
        match self {
            Operator::LogicalGet { predicates, .. } | Operator::LogicalFilter(predicates, _) => {
                for p in predicates {
                    set.extend(p.free());
                }
            }
            Operator::LogicalMap(projects, _) => {
                for (x, _) in projects {
                    set.extend(x.free());
                }
            }
            Operator::LogicalJoin { join, .. } => {
                for p in join.predicates() {
                    set.extend(p.free());
                }
            }
            Operator::LogicalProject(projects, _) => {
                set.extend(projects.clone());
            }
            Operator::LogicalWith(_, columns, _, _) | Operator::LogicalGetWith(_, columns) => {
                set.extend(columns.clone());
            }
            Operator::LogicalAggregate {
                group_by,
                aggregate,
                ..
            } => {
                set.extend(group_by.clone());
                for (f, _) in aggregate {
                    set.extend(f.free());
                }
            }
            Operator::LogicalSort(order_by, _) => {
                for o in order_by {
                    set.insert(o.column.clone());
                }
            }
            Operator::LogicalValues(columns, rows, _) => {
                set.extend(columns.clone());
                for row in rows {
                    for x in row {
                        set.extend(x.free());
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
            set.extend(self[i].free());
        }
        set
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
    pub typ: encoding::Type,
}

#[derive(Clone, Eq, PartialEq, Hash)]
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
            typ: encoding::Type::from(column.r#type.as_ref().unwrap()),
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
        write!(f, "{}#{}", self.name, self.id)
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(table) = &self.table {
            write!(f, "{}.", table)?;
        }
        // TODO write # when there are multiple columns with the same name
        write!(f, "{}", self.name)
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
    pub desc: bool,
}

impl fmt::Display for OrderBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.desc {
            write!(f, "-{}", self.column)
        } else {
            write!(f, "{}", self.column)
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Alter {
    AddColumn { name: String, typ: encoding::Type },
    DropColumn { name: String },
}

impl fmt::Display for Alter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Alter::AddColumn { name, typ } => write!(f, "(AddColumn {}:{})", name, typ),
            Alter::DropColumn { name } => write!(f, "(DropColumn {})", name),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Scalar {
    Literal(encoding::Value, encoding::Type),
    Column(Column),
    Call(Function, Vec<Scalar>, encoding::Type),
    Cast(Box<Scalar>, encoding::Type),
}

impl Scalar {
    pub fn typ(&self) -> encoding::Type {
        match self {
            Scalar::Literal(_, typ) => typ.clone(),
            Scalar::Column(column) => column.typ.clone(),
            Scalar::Call(_, _, returns) => returns.clone(),
            Scalar::Cast(_, typ) => typ.clone(),
        }
    }

    pub fn free(&self) -> HashSet<Column> {
        let mut free = HashSet::new();
        self.collect_free(&mut free);
        free
    }

    fn collect_free(&self, free: &mut HashSet<Column>) {
        match self {
            Scalar::Literal(_, _) => {}
            Scalar::Column(column) => {
                free.insert(column.clone());
            }
            Scalar::Call(_, arguments, _) => {
                for a in arguments {
                    a.collect_free(free);
                }
            }
            Scalar::Cast(scalar, _) => scalar.collect_free(free),
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
            Scalar::Cast(uncast, typ) => {
                Scalar::Cast(Box::new(uncast.inline(expr, column)), typ.clone())
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

    fn free(&self) -> Option<Column> {
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
