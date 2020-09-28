use std::fmt;
use std::ops;

// Operator plan nodes combine inputs in a Plan tree.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Operator<T> {
    // LogicalSingleGet acts as the input to a SELECT with no FROM clause.
    LogicalSingleGet,
    // LogicalGet(table) implements the FROM clause.
    LogicalGet(Table),
    // LogicalFilter(predicates) implements the WHERE/HAVING clauses.
    LogicalFilter(Vec<Scalar>, T),
    // LogicalProject(columns) implements the SELECT clause.
    // TODO I think this is a misunderstanding of what project represents.
    LogicalProject(Vec<(Scalar, Column)>, T),
    LogicalJoin(Join, T, T),
    // LogicalWith(table) implements with subquery as  _.
    // The with-subquery is always on the left.
    LogicalWith(String, T, T),
    // LogicalGetWith(table) reads the subquery that was created by With.
    LogicalGetWith(String),
    // LogicalAggregate(group_by, aggregate) implements the GROUP BY clause.
    LogicalAggregate {
        group_by: Vec<Column>,
        aggregate: Vec<(Aggregate, Column)>,
        input: T,
    },
    // LogicalLimit(n) implements the LIMIT / OFFSET / TOP clause.
    LogicalLimit {
        limit: usize,
        offset: usize,
        input: T,
    },
    // LogicalSort(columns) implements the ORDER BY clause.
    LogicalSort(Vec<Sort>, T),
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
    PhysicalTableFreeScan,
    PhysicalSeqScan(Table),
    PhysicalIndexScan {
        table: Table,
        equals: Vec<(Column, Scalar)>,
    },
    PhysicalFilter(Vec<Scalar>, T),
    PhysicalProject(Vec<(Scalar, Column)>, T),
    PhysicalNestedLoop(Join, T, T),
    PhysicalHashJoin(Join, Vec<(Scalar, Scalar)>, T, T),
    PhysicalCreateTempTable(String, T),
    PhysicalGetTempTable(String),
    PhysicalAggregate {
        group_by: Vec<Column>,
        aggregate: Vec<(Aggregate, Column)>,
        input: T,
    },
    PhysicalLimit {
        limit: i64,
        offset: i64,
        input: T,
    },
    PhysicalSort(Vec<Sort>, T),
    PhysicalUnion(T, T),
    PhysicalIntersect(T, T),
    PhysicalExcept(T, T),
    PhysicalInsert(Table, Vec<Column>, T),
    PhysicalValues(Vec<Column>, Vec<Vec<Scalar>>),
    PhysicalUpdate(Vec<(Column, Option<Column>)>, T),
    PhysicalDelete(Table, T),
    PhysicalCreateDatabase(Name),
    PhysicalCreateTable {
        name: Name,
        columns: Vec<(String, encoding::Type)>,
        partition_by: Vec<i64>,
        cluster_by: Vec<i64>,
        primary_key: Vec<i64>,
    },
    PhysicalCreateIndex {
        name: Name,
        table: Name,
        columns: Vec<String>,
    },
    PhysicalAlterTable {
        name: Name,
        actions: Vec<Alter>,
    },
    PhysicalDrop {
        object: ObjectType,
        name: Name,
    },
    PhysicalRename {
        object: ObjectType,
        from: Name,
        to: Name,
    },
}

impl<T> Operator<T> {
    pub fn is_logical(&self) -> bool {
        match self {
            Operator::LogicalJoin(_, _, _)
            | Operator::LogicalWith(_, _, _)
            | Operator::LogicalUnion(_, _)
            | Operator::LogicalIntersect(_, _)
            | Operator::LogicalExcept(_, _)
            | Operator::LogicalFilter(_, _)
            | Operator::LogicalProject(_, _)
            | Operator::LogicalAggregate { .. }
            | Operator::LogicalLimit { .. }
            | Operator::LogicalSort(_, _)
            | Operator::LogicalInsert(_, _, _)
            | Operator::LogicalValues(_, _, _)
            | Operator::LogicalUpdate(_, _)
            | Operator::LogicalDelete(_, _)
            | Operator::LogicalSingleGet
            | Operator::LogicalGet(_)
            | Operator::LogicalGetWith(_)
            | Operator::LogicalCreateDatabase(_)
            | Operator::LogicalCreateTable { .. }
            | Operator::LogicalCreateIndex { .. }
            | Operator::LogicalAlterTable { .. }
            | Operator::LogicalDrop { .. }
            | Operator::LogicalRename { .. } => true,
            _ => false,
        }
    }

    pub fn introduces(&self, column: &Column) -> bool {
        fn contains(predicates: &Vec<Scalar>, column: &Column) -> bool {
            predicates
                .iter()
                .any(|scalar| scalar.columns().any(|c| c == column))
        }
        match self {
            Operator::LogicalGet(table) => table.columns.contains(column),
            Operator::LogicalProject(projects, _) => projects.iter().any(|(_, c)| c == column),
            Operator::LogicalJoin(Join::Mark(mark, predicates), _, _) => {
                mark == column || contains(predicates, column)
            }
            Operator::LogicalJoin(Join::Inner(predicates), _, _)
            | Operator::LogicalJoin(Join::Right(predicates), _, _)
            | Operator::LogicalJoin(Join::Outer(predicates), _, _)
            | Operator::LogicalJoin(Join::Semi(predicates), _, _)
            | Operator::LogicalJoin(Join::Single(predicates), _, _) => contains(predicates, column),
            Operator::LogicalGetWith { .. } => todo!("get_with"),
            Operator::LogicalAggregate { aggregate, .. } => {
                aggregate.iter().any(|(_, c)| c == column)
            }
            _ => false,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Operator::LogicalJoin(_, _, _)
            | Operator::LogicalWith(_, _, _)
            | Operator::LogicalUnion(_, _)
            | Operator::LogicalIntersect(_, _)
            | Operator::LogicalExcept(_, _)
            | Operator::PhysicalNestedLoop { .. }
            | Operator::PhysicalHashJoin { .. }
            | Operator::PhysicalUnion { .. }
            | Operator::PhysicalIntersect { .. }
            | Operator::PhysicalExcept { .. } => 2,
            Operator::LogicalFilter(_, _)
            | Operator::LogicalProject(_, _)
            | Operator::LogicalAggregate { .. }
            | Operator::LogicalLimit { .. }
            | Operator::LogicalSort(_, _)
            | Operator::LogicalInsert(_, _, _)
            | Operator::LogicalValues(_, _, _)
            | Operator::LogicalUpdate(_, _)
            | Operator::LogicalDelete(_, _)
            | Operator::PhysicalFilter { .. }
            | Operator::PhysicalProject { .. }
            | Operator::PhysicalCreateTempTable { .. }
            | Operator::PhysicalAggregate { .. }
            | Operator::PhysicalLimit { .. }
            | Operator::PhysicalSort { .. }
            | Operator::PhysicalInsert { .. }
            | Operator::PhysicalUpdate { .. }
            | Operator::PhysicalDelete { .. } => 1,
            Operator::LogicalSingleGet
            | Operator::LogicalGet(_)
            | Operator::LogicalGetWith(_)
            | Operator::LogicalCreateDatabase(_)
            | Operator::LogicalCreateTable { .. }
            | Operator::LogicalCreateIndex { .. }
            | Operator::LogicalAlterTable { .. }
            | Operator::LogicalDrop { .. }
            | Operator::LogicalRename { .. }
            | Operator::PhysicalTableFreeScan
            | Operator::PhysicalSeqScan { .. }
            | Operator::PhysicalIndexScan { .. }
            | Operator::PhysicalGetTempTable { .. }
            | Operator::PhysicalValues { .. }
            | Operator::PhysicalCreateDatabase { .. }
            | Operator::PhysicalCreateTable { .. }
            | Operator::PhysicalCreateIndex { .. }
            | Operator::PhysicalAlterTable { .. }
            | Operator::PhysicalDrop { .. }
            | Operator::PhysicalRename { .. } => 0,
        }
    }

    pub fn map<U>(self, visitor: impl Fn(T) -> U) -> Operator<U> {
        match self {
            Operator::LogicalFilter(predicates, input) => {
                let input = visitor(input);
                Operator::LogicalFilter(predicates, input)
            }
            Operator::LogicalProject(projects, input) => {
                let input = visitor(input);
                Operator::LogicalProject(projects, input)
            }
            Operator::LogicalJoin(join, left, right) => {
                let left = visitor(left);
                let right = visitor(right);
                Operator::LogicalJoin(join, left, right)
            }
            Operator::LogicalWith(name, left, right) => {
                let left = visitor(left);
                let right = visitor(right);
                Operator::LogicalWith(name, left, right)
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
            Operator::LogicalGet(table) => Operator::LogicalGet(table),
            Operator::LogicalGetWith(name) => Operator::LogicalGetWith(name),
            Operator::LogicalCreateDatabase(name) => Operator::LogicalCreateDatabase(name),
            Operator::LogicalCreateTable {
                name,
                columns,
                partition_by,
                cluster_by,
                primary_key,
            } => Operator::LogicalCreateTable {
                name,
                columns,
                partition_by,
                cluster_by,
                primary_key,
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
            Operator::PhysicalTableFreeScan => Operator::PhysicalTableFreeScan,
            Operator::PhysicalSeqScan(table) => Operator::PhysicalSeqScan(table),
            Operator::PhysicalIndexScan { table, equals } => {
                Operator::PhysicalIndexScan { table, equals }
            }
            Operator::PhysicalFilter(predicates, input) => {
                Operator::PhysicalFilter(predicates, visitor(input))
            }
            Operator::PhysicalProject(projects, input) => {
                Operator::PhysicalProject(projects, visitor(input))
            }
            Operator::PhysicalNestedLoop(join, left, right) => {
                Operator::PhysicalNestedLoop(join, visitor(left), visitor(right))
            }
            Operator::PhysicalHashJoin(join, equals, left, right) => {
                Operator::PhysicalHashJoin(join, equals, visitor(left), visitor(right))
            }
            Operator::PhysicalCreateTempTable(name, input) => {
                Operator::PhysicalCreateTempTable(name, visitor(input))
            }
            Operator::PhysicalGetTempTable(name) => Operator::PhysicalGetTempTable(name),
            Operator::PhysicalAggregate {
                group_by,
                aggregate,
                input,
            } => Operator::PhysicalAggregate {
                group_by,
                aggregate,
                input: visitor(input),
            },
            Operator::PhysicalLimit {
                limit,
                offset,
                input,
            } => Operator::PhysicalLimit {
                limit,
                offset,
                input: visitor(input),
            },
            Operator::PhysicalSort(order_by, input) => {
                Operator::PhysicalSort(order_by, visitor(input))
            }
            Operator::PhysicalUnion(left, right) => {
                Operator::PhysicalUnion(visitor(left), visitor(right))
            }
            Operator::PhysicalIntersect(left, right) => {
                Operator::PhysicalIntersect(visitor(left), visitor(right))
            }
            Operator::PhysicalExcept(left, right) => {
                Operator::PhysicalExcept(visitor(left), visitor(right))
            }
            Operator::PhysicalInsert(table, columns, input) => {
                Operator::PhysicalInsert(table, columns, visitor(input))
            }
            Operator::PhysicalValues(columns, rows) => Operator::PhysicalValues(columns, rows),
            Operator::PhysicalUpdate(updates, input) => {
                Operator::PhysicalUpdate(updates, visitor(input))
            }
            Operator::PhysicalDelete(table, input) => {
                Operator::PhysicalDelete(table, visitor(input))
            }
            Operator::PhysicalCreateDatabase(name) => Operator::PhysicalCreateDatabase(name),
            Operator::PhysicalCreateTable {
                name,
                columns,
                partition_by,
                cluster_by,
                primary_key,
            } => Operator::PhysicalCreateTable {
                name,
                columns,
                partition_by,
                cluster_by,
                primary_key,
            },
            Operator::PhysicalCreateIndex {
                name,
                table,
                columns,
            } => Operator::PhysicalCreateIndex {
                name,
                table,
                columns,
            },
            Operator::PhysicalAlterTable { name, actions } => {
                Operator::PhysicalAlterTable { name, actions }
            }
            Operator::PhysicalDrop { object, name } => Operator::PhysicalDrop { object, name },
            Operator::PhysicalRename { object, from, to } => {
                Operator::PhysicalRename { object, from, to }
            }
        }
    }
}

impl<T> ops::IndexMut<usize> for Operator<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        todo!()
    }
}

impl<T> ops::Index<usize> for Operator<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            Operator::LogicalJoin(_, left, right)
            | Operator::LogicalWith(_, left, right)
            | Operator::LogicalUnion(left, right)
            | Operator::LogicalIntersect(left, right)
            | Operator::LogicalExcept(left, right)
            | Operator::PhysicalNestedLoop(_, left, right)
            | Operator::PhysicalHashJoin(_, _, left, right)
            | Operator::PhysicalUnion(left, right)
            | Operator::PhysicalIntersect(left, right)
            | Operator::PhysicalExcept(left, right) => match index {
                0 => left,
                1 => right,
                _ => panic!("{} is out of bounds [0,2)", index),
            },
            Operator::LogicalFilter(_, input)
            | Operator::LogicalProject(_, input)
            | Operator::LogicalAggregate { input, .. }
            | Operator::LogicalLimit { input, .. }
            | Operator::LogicalSort(_, input)
            | Operator::LogicalInsert(_, _, input)
            | Operator::LogicalValues(_, _, input)
            | Operator::LogicalUpdate(_, input)
            | Operator::LogicalDelete(_, input)
            | Operator::PhysicalFilter(_, input)
            | Operator::PhysicalProject(_, input)
            | Operator::PhysicalCreateTempTable(_, input)
            | Operator::PhysicalAggregate { input, .. }
            | Operator::PhysicalLimit { input, .. }
            | Operator::PhysicalSort(_, input)
            | Operator::PhysicalInsert(_, _, input)
            | Operator::PhysicalUpdate(_, input)
            | Operator::PhysicalDelete(_, input) => match index {
                0 => input,
                _ => panic!("{} is out of bounds [0,1)", index),
            },
            _ => panic!("{} is out of bounds ()", index),
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

impl fmt::Display for Join {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Join::Inner(predicates) => write!(f, "Inner {}", join(predicates)),
            Join::Right(predicates) => write!(f, "Right {}", join(predicates)),
            Join::Outer(predicates) => write!(f, "Outer {}", join(predicates)),
            Join::Semi(predicates) => write!(f, "Semi {}", join(predicates)),
            Join::Anti(predicates) => write!(f, "Anti {}", join(predicates)),
            Join::Single(predicates) => write!(f, "Single {}", join(predicates)),
            Join::Mark(column, predicates) => write!(f, "Mark {} {}", column, join(predicates)),
        }
    }
}

fn join(xs: &Vec<Scalar>) -> String {
    let mut strings = vec![];
    for x in xs {
        strings.push(format!("{}", x));
    }
    strings.join(" ")
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

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Column {
    pub id: i64,
    pub name: String,
    pub table: Option<String>,
    pub typ: encoding::Type,
}

impl Column {
    pub fn from(column: &zetasql::ResolvedColumnProto) -> Self {
        let id = column.column_id.unwrap();
        let name = column.name.clone().unwrap();
        let table = column.table_name.clone();
        let typ = encoding::Type::from(column.r#type.as_ref().unwrap());
        Column {
            id,
            name,
            table,
            typ,
        }
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(table) = &self.table {
            write!(f, "{}.", table)?;
        }
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
pub struct Sort {
    pub column: Column,
    pub desc: bool,
}

impl fmt::Display for Sort {
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
    // TODO eliminate Vec in favor of Function being a tree
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

    pub fn columns(&self) -> ColumnIterator {
        ColumnIterator { stack: vec![self] }
    }

    pub fn can_inline(&self) -> bool {
        match self {
            Scalar::Literal(_, _) => true,
            Scalar::Column(_) => true,
            Scalar::Call(f, _, _) => !f.has_effects(),
            Scalar::Cast(expr, _) => expr.can_inline(),
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
}

impl fmt::Display for Scalar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Scalar::Literal(value, _) => write!(f, "{}", value),
            Scalar::Column(column) => write!(f, "{}", column),
            Scalar::Call(function, arguments, _) => {
                if arguments.is_empty() {
                    write!(f, "({:?})", function)
                } else {
                    write!(f, "({:?} {})", function, join(arguments))
                }
            }
            Scalar::Cast(value, typ) => write!(f, "(Cast {} {})", value, typ),
        }
    }
}

pub struct ColumnIterator<'it> {
    stack: Vec<&'it Scalar>,
}

impl<'it> Iterator for ColumnIterator<'it> {
    type Item = &'it Column;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.stack.pop() {
                None => return None,
                Some(Scalar::Literal(_, _)) => continue,
                Some(Scalar::Column(column)) => return Some(column),
                Some(Scalar::Cast(scalar, _)) => {
                    self.stack.push(scalar);
                    continue;
                }
                Some(Scalar::Call(_, arguments, _)) => {
                    for a in arguments {
                        self.stack.push(a);
                    }
                    continue;
                }
            }
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

    pub fn has_effects(&self) -> bool {
        false
    }
}

// Aggregate functions appear in GROUP BY expressions.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Aggregate {
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

impl Aggregate {
    pub fn from(
        name: String,
        distinct: bool,
        ignore_nulls: bool,
        argument: Option<Column>,
    ) -> Self {
        let distinct = Distinct(distinct);
        let ignore_nulls = IgnoreNulls(ignore_nulls);
        match name.as_str() {
            "ZetaSQL:any_value" => Aggregate::AnyValue(argument.unwrap()),
            "ZetaSQL:array_agg" => Aggregate::ArrayAgg(distinct, ignore_nulls, argument.unwrap()),
            "ZetaSQL:array_concat_agg" => Aggregate::ArrayConcatAgg(argument.unwrap()),
            "ZetaSQL:avg" => Aggregate::Avg(distinct, argument.unwrap()),
            "ZetaSQL:bit_and" => Aggregate::BitAnd(distinct, argument.unwrap()),
            "ZetaSQL:bit_or" => Aggregate::BitOr(distinct, argument.unwrap()),
            "ZetaSQL:bit_xor" => Aggregate::BitXor(distinct, argument.unwrap()),
            "ZetaSQL:count" => Aggregate::Count(distinct, argument.unwrap()),
            "ZetaSQL:$count_star" => Aggregate::CountStar,
            "ZetaSQL:logical_and" => Aggregate::LogicalAnd(argument.unwrap()),
            "ZetaSQL:logical_or" => Aggregate::LogicalOr(argument.unwrap()),
            "ZetaSQL:max" => Aggregate::Max(argument.unwrap()),
            "ZetaSQL:min" => Aggregate::Min(argument.unwrap()),
            "ZetaSQL:string_agg" => Aggregate::StringAgg(distinct, argument.unwrap()),
            "ZetaSQL:sum" => Aggregate::Sum(distinct, argument.unwrap()),
            _ => panic!("{} is not supported", name),
        }
    }
}

impl fmt::Display for Aggregate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Aggregate::AnyValue(column) => write!(f, "(AnyValue {})", column),
            Aggregate::ArrayAgg(Distinct(true), IgnoreNulls(true), column) => {
                write!(f, "(ArrayAgg (Distinct {}))", column)
            }
            Aggregate::ArrayAgg(Distinct(true), IgnoreNulls(false), column) => {
                write!(f, "(ArrayAgg (Distinct (IgnoreNulls {})))", column)
            }
            Aggregate::ArrayAgg(Distinct(false), IgnoreNulls(true), column) => {
                write!(f, "(ArrayAgg IgnoreNulls({}))", column)
            }
            Aggregate::ArrayAgg(Distinct(false), IgnoreNulls(false), column) => {
                write!(f, "(ArrayAgg (IgnoreNulls {}))", column)
            }
            Aggregate::ArrayConcatAgg(column) => write!(f, "(ArrayConcatAgg {})", column),
            Aggregate::Avg(Distinct(true), column) => write!(f, "(Avg (Distinct {}))", column),
            Aggregate::Avg(Distinct(false), column) => write!(f, "(Avg {})", column),
            Aggregate::BitAnd(Distinct(true), column) => {
                write!(f, "(BitAnd (Distinct {}))", column)
            }
            Aggregate::BitAnd(Distinct(false), column) => write!(f, "(BitAnd {})", column),
            Aggregate::BitOr(Distinct(true), column) => write!(f, "(BitOr (Distinct {}))", column),
            Aggregate::BitOr(Distinct(false), column) => write!(f, "(BitOr {})", column),
            Aggregate::BitXor(Distinct(true), column) => {
                write!(f, "(BitXor (Distinct {}))", column)
            }
            Aggregate::BitXor(Distinct(false), column) => write!(f, "(Avg {})", column),
            Aggregate::Count(Distinct(true), column) => write!(f, "(Count (Distinct {}))", column),
            Aggregate::Count(Distinct(false), column) => write!(f, "(Count {})", column),
            Aggregate::CountStar => write!(f, "(CountStar)"),
            Aggregate::LogicalAnd(column) => write!(f, "(LogicalAnd {})", column),
            Aggregate::LogicalOr(column) => write!(f, "(LogicalOr {})", column),
            Aggregate::Max(column) => write!(f, "(Max {})", column),
            Aggregate::Min(column) => write!(f, "(Min {})", column),
            Aggregate::StringAgg(Distinct(true), column) => {
                write!(f, "(StringAgg (Distinct {}))", column)
            }
            Aggregate::StringAgg(Distinct(false), column) => write!(f, "(Avg {})", column),
            Aggregate::Sum(Distinct(true), column) => write!(f, "(Sum (Distinct {}))", column),
            Aggregate::Sum(Distinct(false), column) => write!(f, "(Sum {})", column),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Distinct(bool);

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct IgnoreNulls(bool);
