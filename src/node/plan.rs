use fmt::Debug;
use std::fmt;

#[derive(Debug, Clone)]
pub struct Expr(pub Operator, pub Vec<Expr>);

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.print(f, 0)
    }
}

impl Expr {
    pub fn operator(&self) -> &Operator {
        &self.0
    }

    pub fn input(&self, i: usize) -> &Expr {
        &self.1[i]
    }

    fn print(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        write!(f, "{}", self.0)?;
        for input in &self.1 {
            write!(f, "\n")?;
            for _ in 0..indent + 1 {
                write!(f, "\t")?;
            }
            input.print(f, indent + 1)?;
        }
        Ok(())
    }

    pub fn correlated(&self, column: &Column) -> bool {
        if self.0.introduces(column) {
            return false;
        }
        for input in &self.1 {
            if !input.correlated(column) {
                return false;
            }
        }
        true
    }

    pub fn extract(self) -> Extract<Expr> {
        Extract::new(self.0, self.1)
    }
}

pub enum Extract<T> {
    Leaf(Operator),
    Unary(Operator, T),
    Binary(Operator, T, T),
}

impl<T> Extract<T> {
    pub fn new(op: Operator, mut inputs: Vec<T>) -> Self {
        match inputs.len() {
            0 => Extract::Leaf(op),
            1 => Extract::Unary(op, inputs.remove(0)),
            2 => Extract::Binary(op, inputs.remove(0), inputs.remove(0)),
            n => panic!(n),
        }
    }
}

// Operator plan nodes combine inputs in a Plan tree.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Operator {
    // LogicalSingleGet acts as the input to a SELECT with no FROM clause.
    LogicalSingleGet,
    // LogicalGet(table) implements the FROM clause.
    LogicalGet(Table),
    // LogicalFilter(predicates) implements the WHERE/HAVING clauses.
    LogicalFilter(Vec<Scalar>),
    // LogicalProject(columns) implements the SELECT clause.
    LogicalProject(Vec<(Scalar, Column)>),
    LogicalJoin {
        join: Join,
        predicates: Vec<Scalar>,
        mark: Option<Column>, // TODO embed this in Join::Mark, Join::Semi, Join::Anti
    },
    // LogicalWith(table) implements with subquery as  _.
    // The with-subquery is always on the left.
    LogicalWith(String),
    // LogicalGetWith(table) reads the subquery that was created by With.
    LogicalGetWith(String),
    // LogicalAggregate(group_by, aggregate) implements the GROUP BY clause.
    LogicalAggregate {
        group_by: Vec<Column>,
        aggregate: Vec<(Aggregate, Column)>,
    },
    // LogicalLimit(n) implements the LIMIT / OFFSET / TOP clause.
    LogicalLimit {
        limit: usize,
        offset: usize,
    },
    // LogicalSort(columns) implements the ORDER BY clause.
    LogicalSort(Vec<Sort>),
    // LogicalUnion implements SELECT _ UNION ALL SELECT _.
    LogicalUnion,
    // LogicalIntersect implements SELECT _ INTERSECT SELECT _.
    LogicalIntersect,
    // LogicalExcept implements SELECT _ EXCEPT SELECT _.
    LogicalExcept,
    // LogicalInsert(table, columns) implements the INSERT operation.
    LogicalInsert(Table, Vec<Column>),
    // LogicalValues(rows, columns) implements VALUES expressions.
    LogicalValues(Vec<Column>, Vec<Vec<Scalar>>),
    // LogicalUpdate(sets) implements the UPDATE operation.
    // (column, None) indicates set column to default value.
    LogicalUpdate(Vec<(Column, Option<Column>)>),
    // LogicalDelete(table) implements the DELETE operation.
    LogicalDelete(Table),
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
    PhysicalFilter(Vec<Scalar>),
    PhysicalProject(Vec<(Scalar, Column)>),
    PhysicalNestedLoop {
        join: Join,
        predicates: Vec<Scalar>,
        mark: Option<Column>, // TODO embed this in Join::Mark, Join::Semi, Join::Anti
    },
    PhysicalHashJoin {
        join: Join,
        mark: Option<Column>,
        equals: Vec<(Scalar, Scalar)>,
        predicates: Vec<Scalar>,
    },
    PhysicalCreateTempTable(String),
    PhysicalGetTempTable(String),
    PhysicalAggregate {
        group_by: Vec<Column>,
        aggregate: Vec<(Aggregate, Column)>,
    },
    PhysicalLimit {
        limit: i64,
        offset: i64,
    },
    PhysicalSort(Vec<Sort>),
    PhysicalUnion,
    PhysicalIntersect,
    PhysicalExcept,
    PhysicalInsert(Table, Vec<Column>),
    PhysicalValues(Vec<Column>, Vec<Vec<Scalar>>),
    PhysicalUpdate(Vec<(Column, Option<Column>)>),
    PhysicalDelete(Table),
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

impl Operator {
    pub fn reflect(&self) -> OperatorType {
        match self {
            Operator::LogicalSingleGet { .. } => OperatorType::LogicalSingleGet,
            Operator::LogicalGet { .. } => OperatorType::LogicalGet,
            Operator::LogicalFilter { .. } => OperatorType::LogicalFilter,
            Operator::LogicalProject { .. } => OperatorType::LogicalProject,
            Operator::LogicalJoin { .. } => OperatorType::LogicalJoin,
            Operator::LogicalWith { .. } => OperatorType::LogicalWith,
            Operator::LogicalGetWith { .. } => OperatorType::LogicalGetWith,
            Operator::LogicalAggregate { .. } => OperatorType::LogicalAggregate,
            Operator::LogicalLimit { .. } => OperatorType::LogicalLimit,
            Operator::LogicalSort { .. } => OperatorType::LogicalSort,
            Operator::LogicalUnion { .. } => OperatorType::LogicalUnion,
            Operator::LogicalIntersect { .. } => OperatorType::LogicalIntersect,
            Operator::LogicalExcept { .. } => OperatorType::LogicalExcept,
            Operator::LogicalInsert { .. } => OperatorType::LogicalInsert,
            Operator::LogicalValues { .. } => OperatorType::LogicalValues,
            Operator::LogicalUpdate { .. } => OperatorType::LogicalUpdate,
            Operator::LogicalDelete { .. } => OperatorType::LogicalDelete,
            Operator::LogicalCreateDatabase { .. } => OperatorType::LogicalCreateDatabase,
            Operator::LogicalCreateTable { .. } => OperatorType::LogicalCreateTable,
            Operator::LogicalCreateIndex { .. } => OperatorType::LogicalCreateIndex,
            Operator::LogicalAlterTable { .. } => OperatorType::LogicalAlterTable,
            Operator::LogicalDrop { .. } => OperatorType::LogicalDrop,
            Operator::LogicalRename { .. } => OperatorType::LogicalRename,
            Operator::PhysicalTableFreeScan { .. } => OperatorType::PhysicalTableFreeScan,
            Operator::PhysicalSeqScan { .. } => OperatorType::PhysicalSeqScan,
            Operator::PhysicalIndexScan { .. } => OperatorType::PhysicalIndexScan,
            Operator::PhysicalFilter { .. } => OperatorType::PhysicalFilter,
            Operator::PhysicalProject { .. } => OperatorType::PhysicalProject,
            Operator::PhysicalNestedLoop { .. } => OperatorType::PhysicalNestedLoop,
            Operator::PhysicalHashJoin { .. } => OperatorType::PhysicalHashJoin,
            Operator::PhysicalCreateTempTable { .. } => OperatorType::PhysicalCreateTempTable,
            Operator::PhysicalGetTempTable { .. } => OperatorType::PhysicalGetTempTable,
            Operator::PhysicalAggregate { .. } => OperatorType::PhysicalAggregate,
            Operator::PhysicalLimit { .. } => OperatorType::PhysicalLimit,
            Operator::PhysicalSort { .. } => OperatorType::PhysicalSort,
            Operator::PhysicalUnion { .. } => OperatorType::PhysicalUnion,
            Operator::PhysicalIntersect { .. } => OperatorType::PhysicalIntersect,
            Operator::PhysicalExcept { .. } => OperatorType::PhysicalExcept,
            Operator::PhysicalInsert { .. } => OperatorType::PhysicalInsert,
            Operator::PhysicalValues { .. } => OperatorType::PhysicalValues,
            Operator::PhysicalUpdate { .. } => OperatorType::PhysicalUpdate,
            Operator::PhysicalDelete { .. } => OperatorType::PhysicalDelete,
            Operator::PhysicalCreateDatabase { .. } => OperatorType::PhysicalCreateDatabase,
            Operator::PhysicalCreateTable { .. } => OperatorType::PhysicalCreateTable,
            Operator::PhysicalCreateIndex { .. } => OperatorType::PhysicalCreateIndex,
            Operator::PhysicalAlterTable { .. } => OperatorType::PhysicalAlterTable,
            Operator::PhysicalDrop { .. } => OperatorType::PhysicalDrop,
            Operator::PhysicalRename { .. } => OperatorType::PhysicalRename,
        }
    }

    fn introduces(&self, column: &Column) -> bool {
        match self {
            Operator::LogicalGet(table) => table.columns.contains(column),
            Operator::LogicalProject(projects) => projects.iter().any(|(_, c)| c == column),
            Operator::LogicalJoin {
                mark: Some(mark), ..
            } => mark == column,
            Operator::LogicalGetWith { .. } => todo!(),
            Operator::LogicalAggregate { aggregate, .. } => {
                aggregate.iter().any(|(_, c)| c == column)
            }
            _ => false,
        }
    }
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operator::LogicalSingleGet => write!(f, "LogicalSingleGet"),
            Operator::LogicalGet(table) => write!(f, "LogicalGet {}", table.name),
            Operator::LogicalFilter(predicates) => write!(f, "LogicalFilter {}", join(predicates)),
            Operator::LogicalProject(projects) => {
                let mut strings = vec![];
                for (x, c) in projects {
                    strings.push(format!("{}:{}", c, x));
                }
                write!(f, "LogicalProject {}", strings.join(" "))
            }
            Operator::LogicalJoin {
                join: Join::Inner,
                predicates,
                ..
            } => write!(f, "LogicalInnerJoin {}", join(predicates)),
            Operator::LogicalJoin {
                join: Join::Right,
                predicates,
                ..
            } => write!(f, "LogicalRightJoin {}", join(predicates)),
            Operator::LogicalJoin {
                join: Join::Outer,
                predicates,
                ..
            } => write!(f, "LogicalOuterJoin {}", join(predicates)),
            Operator::LogicalJoin {
                join: Join::Semi,
                predicates,
                ..
            } => write!(f, "LogicalSemiJoin {}", join(predicates)),
            Operator::LogicalJoin {
                join: Join::Anti,
                predicates,
                ..
            } => write!(f, "LogicalAntiJoin {}", join(predicates)),
            Operator::LogicalJoin {
                join: Join::Single,
                predicates,
                ..
            } => write!(f, "LogicalSingleJoin {}", join(predicates)),
            Operator::LogicalJoin {
                join: Join::Mark,
                predicates,
                mark,
            } => write!(
                f,
                "LogicalMarkJoin {} {}",
                mark.as_ref().unwrap(),
                join(predicates)
            ),
            Operator::LogicalWith(name) => write!(f, "LogicalWith {}", name),
            Operator::LogicalGetWith(name) => write!(f, "LogicalGetWith {}", name),
            Operator::LogicalAggregate {
                group_by,
                aggregate,
            } => {
                write!(f, "LogicalAggregate")?;
                for column in group_by {
                    write!(f, " {}", column)?;
                }
                for (aggregate, column) in aggregate {
                    write!(f, " {}:{}", column, aggregate)?;
                }
                Ok(())
            }
            Operator::LogicalLimit { limit, offset } => {
                write!(f, "LogicalLimit {} {}", limit, offset)
            }
            Operator::LogicalSort(order_by) => {
                write!(f, "LogicalSort")?;
                for sort in order_by {
                    if sort.desc {
                        write!(f, " (Desc {})", sort.column)?;
                    } else {
                        write!(f, " {}", sort.column)?;
                    }
                }
                Ok(())
            }
            Operator::LogicalUnion => write!(f, "LogicalUnion"),
            Operator::LogicalIntersect => write!(f, "LogicalIntersect"),
            Operator::LogicalExcept => write!(f, "LogicalExcept"),
            Operator::LogicalInsert(table, columns) => {
                write!(f, "LogicalInsert {}", table.name)?;
                for c in columns {
                    write!(f, " {}", c)?;
                }
                Ok(())
            }
            Operator::LogicalValues(columns, rows) => {
                write!(f, "LogicalValues")?;
                for column in columns {
                    write!(f, " {}", column)?;
                }
                for row in rows {
                    write!(f, " [{}]", join(row))?;
                }
                Ok(())
            }
            Operator::LogicalUpdate(updates) => {
                write!(f, "LogicalUpdate")?;
                for (target, value) in updates {
                    match value {
                        Some(value) => write!(f, " {}:{}", target, value)?,
                        None => write!(f, " {}:_", target)?,
                    }
                }
                Ok(())
            }
            Operator::LogicalDelete(table) => write!(f, "LogicalDelete {}", table.name),
            Operator::LogicalCreateDatabase(name) => {
                write!(f, "LogicalCreateDatabase {}", name.path.join("."))
            }
            Operator::LogicalCreateTable {
                name,
                columns,
                partition_by,
                cluster_by,
                primary_key,
            } => {
                write!(f, "LogicalCreateTable {}", name)?;
                for (name, typ) in columns {
                    write!(f, " {}:{}", name, typ)?;
                }
                if !partition_by.is_empty() {
                    write!(f, " (PartitionBy")?;
                    for p in partition_by {
                        write!(f, " {}", p)?;
                    }
                    write!(f, ")")?;
                }
                if !cluster_by.is_empty() {
                    write!(f, " (ClusterBy")?;
                    for p in cluster_by {
                        write!(f, " {}", p)?;
                    }
                    write!(f, ")")?;
                }
                if !primary_key.is_empty() {
                    write!(f, " (PrimaryKey")?;
                    for p in primary_key {
                        write!(f, " {}", p)?;
                    }
                    write!(f, ")")?;
                }
                Ok(())
            }
            Operator::LogicalCreateIndex {
                name,
                table,
                columns,
            } => write!(
                f,
                "LogicalCreateIndex {} {} {}",
                name,
                table,
                columns.join(" ")
            ),
            Operator::LogicalAlterTable { name, actions } => {
                write!(f, "LogicalAlterTable {}", name)?;
                for a in actions {
                    write!(f, " {}", a)?;
                }
                Ok(())
            }
            Operator::LogicalDrop { object, name } => {
                write!(f, "LogicalDrop {:?} {}", object, name)
            }
            Operator::LogicalRename { object, from, to } => {
                write!(f, "LogicalRename {:?} {} {}", object, from, to)
            }
            Operator::PhysicalTableFreeScan => write!(f, "TableFreeScan"),
            Operator::PhysicalSeqScan(table) => write!(f, "SeqScan {}", table),
            Operator::PhysicalIndexScan { table, equals } => {
                write!(f, "IndexScan {}", table)?;
                for (column, scalar) in equals {
                    write!(f, " {}:{}", column.name, scalar)?;
                }
                Ok(())
            }
            Operator::PhysicalFilter(predicates) => {
                write!(f, "Filter")?;
                for scalar in predicates {
                    write!(f, " {}", scalar)?;
                }
                Ok(())
            }
            Operator::PhysicalProject(project) => {
                write!(f, "Project")?;
                for (scalar, column) in project {
                    write!(f, " {}:{}", column, scalar)?;
                }
                Ok(())
            }
            Operator::PhysicalNestedLoop {
                join: Join::Inner,
                predicates,
                ..
            } => write!(f, "NestedLoop InnerJoin {}", join(predicates)),
            Operator::PhysicalNestedLoop {
                join: Join::Right,
                predicates,
                ..
            } => write!(f, "NestedLoop RightJoin {}", join(predicates)),
            Operator::PhysicalNestedLoop {
                join: Join::Outer,
                predicates,
                ..
            } => write!(f, "NestedLoop OuterJoin {}", join(predicates)),
            Operator::PhysicalNestedLoop {
                join: Join::Semi,
                predicates,
                ..
            } => write!(f, "NestedLoop SemiJoin {}", join(predicates)),
            Operator::PhysicalNestedLoop {
                join: Join::Anti,
                predicates,
                ..
            } => write!(f, "NestedLoop AntiJoin {}", join(predicates)),
            Operator::PhysicalNestedLoop {
                join: Join::Single,
                predicates,
                ..
            } => write!(f, "NestedLoop SingleJoin {}", join(predicates)),
            Operator::PhysicalNestedLoop {
                join: Join::Mark,
                predicates,
                mark,
            } => write!(
                f,
                "NestedLoop MarkJoin {} {}",
                mark.as_ref().unwrap(),
                join(predicates)
            ),
            Operator::PhysicalHashJoin {
                join,
                mark,
                equals,
                predicates,
            } => {
                write!(f, "HashJoin {}", join)?;
                if let Some(mark) = mark {
                    write!(f, " {}", mark)?;
                }
                for (left, right) in equals {
                    write!(f, "{}={}", left, right)?;
                }
                for scalar in predicates {
                    write!(f, " {}", scalar)?;
                }
                Ok(())
            }
            Operator::PhysicalCreateTempTable(name) => write!(f, "CreateTempTable {}", name),
            Operator::PhysicalGetTempTable(name) => write!(f, "GetTempTable {}", name),
            Operator::PhysicalAggregate {
                group_by,
                aggregate,
            } => {
                write!(f, "Aggregate")?;
                for column in group_by {
                    write!(f, " {}", column)?;
                }
                for (aggregate, column) in aggregate {
                    write!(f, " {}:{}", column, aggregate)?;
                }
                Ok(())
            }
            Operator::PhysicalLimit { limit, offset } => write!(f, "Limit {} {}", limit, offset),
            Operator::PhysicalSort(order_by) => {
                write!(f, "Sort")?;
                for sort in order_by {
                    if sort.desc {
                        write!(f, " (Desc {})", sort.column)?;
                    } else {
                        write!(f, " {}", sort.column)?;
                    }
                }
                Ok(())
            }
            Operator::PhysicalUnion => write!(f, "Union"),
            Operator::PhysicalIntersect => write!(f, "Intersect"),
            Operator::PhysicalExcept => write!(f, "Except"),
            Operator::PhysicalInsert(table, columns) => {
                write!(f, "Insert {}", table)?;
                for column in columns {
                    write!(f, " {}", column)?;
                }
                Ok(())
            }
            Operator::PhysicalValues(columns, values) => {
                write!(f, "Values")?;
                for column in columns {
                    write!(f, " {}", column)?;
                }
                for row in values {
                    write!(f, " [{}]", join(row))?;
                }
                Ok(())
            }
            Operator::PhysicalUpdate(updates) => {
                write!(f, "Update")?;
                for (target, value) in updates {
                    match value {
                        Some(value) => write!(f, " {}:{}", target, value)?,
                        None => write!(f, " {}:_", target)?,
                    }
                }
                Ok(())
            }
            Operator::PhysicalDelete(table) => write!(f, "Delete {}", table),
            Operator::PhysicalCreateDatabase(name) => write!(f, "CreateDatabase {}", name),
            Operator::PhysicalCreateTable {
                name,
                columns,
                partition_by,
                cluster_by,
                primary_key,
            } => {
                write!(f, "CreateTable {}", name)?;
                for (name, typ) in columns {
                    write!(f, " {}:{}", name, typ)?;
                }
                if !partition_by.is_empty() {
                    write!(f, " (PartitionBy")?;
                    for p in partition_by {
                        write!(f, " {}", p)?;
                    }
                    write!(f, ")")?;
                }
                if !cluster_by.is_empty() {
                    write!(f, " (ClusterBy")?;
                    for p in cluster_by {
                        write!(f, " {}", p)?;
                    }
                    write!(f, ")")?;
                }
                if !primary_key.is_empty() {
                    write!(f, " (PrimaryKey")?;
                    for p in primary_key {
                        write!(f, " {}", p)?;
                    }
                    write!(f, ")")?;
                }
                Ok(())
            }
            Operator::PhysicalCreateIndex {
                name,
                table,
                columns,
            } => write!(f, "CreateIndex {} {} {}", name, table, columns.join(" ")),
            Operator::PhysicalAlterTable { name, actions } => {
                write!(f, "AlterTable {}", name)?;
                for a in actions {
                    write!(f, " {}", a)?;
                }
                Ok(())
            }
            Operator::PhysicalDrop { object, name } => write!(f, "Drop {:?} {}", object, name),
            Operator::PhysicalRename { object, from, to } => {
                write!(f, "Rename {:?} {} {}", object, from, to)
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum OperatorType {
    LogicalSingleGet,
    LogicalGet,
    LogicalFilter,
    LogicalProject,
    LogicalJoin,
    LogicalWith,
    LogicalGetWith,
    LogicalAggregate,
    LogicalLimit,
    LogicalSort,
    LogicalUnion,
    LogicalIntersect,
    LogicalExcept,
    LogicalValues,
    LogicalInsert,
    LogicalUpdate,
    LogicalDelete,
    LogicalCreateDatabase,
    LogicalCreateTable,
    LogicalCreateIndex,
    LogicalAlterTable,
    LogicalDrop,
    LogicalRename,
    PhysicalTableFreeScan,
    PhysicalSeqScan,
    PhysicalIndexScan,
    PhysicalFilter,
    PhysicalProject,
    PhysicalNestedLoop,
    PhysicalIndexLoop,
    PhysicalHashJoin,
    PhysicalCreateTempTable,
    PhysicalGetTempTable,
    PhysicalAggregate,
    PhysicalLimit,
    PhysicalSort,
    PhysicalUnion,
    PhysicalIntersect,
    PhysicalExcept,
    PhysicalValues,
    PhysicalInsert,
    PhysicalUpdate,
    PhysicalDelete,
    PhysicalCreateDatabase,
    PhysicalCreateTable,
    PhysicalCreateIndex,
    PhysicalAlterTable,
    PhysicalDrop,
    PhysicalRename,
}

impl OperatorType {
    pub fn is_logical(&self) -> bool {
        match self {
            OperatorType::LogicalSingleGet
            | OperatorType::LogicalGet
            | OperatorType::LogicalFilter
            | OperatorType::LogicalProject
            | OperatorType::LogicalJoin
            | OperatorType::LogicalWith
            | OperatorType::LogicalGetWith
            | OperatorType::LogicalAggregate
            | OperatorType::LogicalLimit
            | OperatorType::LogicalSort
            | OperatorType::LogicalUnion
            | OperatorType::LogicalIntersect
            | OperatorType::LogicalExcept
            | OperatorType::LogicalInsert
            | OperatorType::LogicalValues
            | OperatorType::LogicalUpdate
            | OperatorType::LogicalDelete
            | OperatorType::LogicalCreateDatabase
            | OperatorType::LogicalCreateTable
            | OperatorType::LogicalCreateIndex
            | OperatorType::LogicalAlterTable
            | OperatorType::LogicalDrop
            | OperatorType::LogicalRename => true,
            _ => false,
        }
    }

    pub fn is_physical(&self) -> bool {
        !self.is_logical()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Join {
    // Inner implements the default SQL join.
    Inner,
    // Right is used for both left and right joins.
    // When parsing a left join, we convert it to a right join by reversing the order of left and right.
    // The left side is the build side of the join.
    Right,
    // Outer includes non-matching rows from both sides.
    Outer,
    // Semi indicates a semi-join like `select 1 from customer where store_id in (select store_id from store)`.
    // Note that the left side is the build side of the join, which is the reverse of the usual convention.
    Semi,
    // Anti indicates an anti-join like `select 1 from customer where store_id not in (select store_id from store)`.
    // Note that like Semi, the left side is the build side of the join.
    Anti,
    // Single implements "Single Join" from http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
    // The correlated subquery is always on the left.
    Single,
    // Mark implements "Mark Join" from http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
    // The correlated subquery is always on the left.
    Mark,
}

impl fmt::Display for Join {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
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

    pub fn correlated(&self, expr: &Expr) -> bool {
        match self {
            Scalar::Literal(_, _) => false,
            Scalar::Column(column) => expr.correlated(column),
            Scalar::Call(_, arguments, _) => arguments.iter().any(|a| a.correlated(expr)),
            Scalar::Cast(scalar, _) => scalar.correlated(expr),
        }
    }

    pub fn columns(&self) -> Vec<Column> {
        fn visit(node: &Scalar, found: &mut Vec<Column>) {
            match node {
                Scalar::Literal(_, _) => (),
                Scalar::Column(column) => found.push(column.clone()),
                Scalar::Call(_, arguments, _) => {
                    for a in arguments {
                        visit(a, found)
                    }
                }
                Scalar::Cast(scalar, _) => visit(scalar, found),
            }
        }
        let mut found = vec![];
        visit(self, &mut found);
        found
    }

    pub fn has_columns(&self) -> bool {
        match self {
            Scalar::Literal(_, _) => false,
            Scalar::Column(_) => true,
            Scalar::Call(_, arguments, _) => arguments.iter().any(|a| a.has_columns()),
            Scalar::Cast(scalar, _) => scalar.has_columns(),
        }
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
