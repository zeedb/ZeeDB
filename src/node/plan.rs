use fmt::Debug;
use std::fmt;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Expr(pub Operator<Box<Expr>>);

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

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.print(f, 0)
    }
}

impl Expr {
    pub fn correlated(&self, column: &Column) -> bool {
        for expr in self.iter() {
            if expr.0.introduces(column) {
                return false;
            }
        }
        true
    }

    pub fn bottom_up_rewrite(self, visitor: &impl Fn(Expr) -> Expr) -> Expr {
        match self.0 {
            Operator::LogicalFilter(predicates, input) => {
                let input = input.bottom_up_rewrite(visitor);
                visitor(Expr(Operator::LogicalFilter(predicates, Box::new(input))))
            }
            Operator::LogicalProject(projects, input) => {
                let input = input.bottom_up_rewrite(visitor);
                visitor(Expr(Operator::LogicalProject(projects, Box::new(input))))
            }
            Operator::LogicalJoin(join, left, right) => {
                let left = left.bottom_up_rewrite(visitor);
                let right = right.bottom_up_rewrite(visitor);
                visitor(Expr(Operator::LogicalJoin(
                    join,
                    Box::new(left),
                    Box::new(right),
                )))
            }
            Operator::LogicalWith(name, left, right) => {
                let left = left.bottom_up_rewrite(visitor);
                let right = right.bottom_up_rewrite(visitor);
                visitor(Expr(Operator::LogicalWith(
                    name,
                    Box::new(left),
                    Box::new(right),
                )))
            }
            Operator::LogicalAggregate {
                group_by,
                aggregate,
                input,
            } => {
                let input = input.bottom_up_rewrite(visitor);
                visitor(Expr(Operator::LogicalAggregate {
                    group_by,
                    aggregate,
                    input: Box::new(input),
                }))
            }
            Operator::LogicalLimit {
                limit,
                offset,
                input,
            } => {
                let input = input.bottom_up_rewrite(visitor);
                visitor(Expr(Operator::LogicalLimit {
                    limit,
                    offset,
                    input: Box::new(input),
                }))
            }
            Operator::LogicalSort(sorts, input) => {
                let input = input.bottom_up_rewrite(visitor);
                visitor(Expr(Operator::LogicalSort(sorts, Box::new(input))))
            }
            Operator::LogicalUnion(left, right) => {
                let left = left.bottom_up_rewrite(visitor);
                let right = right.bottom_up_rewrite(visitor);
                visitor(Expr(Operator::LogicalUnion(
                    Box::new(left),
                    Box::new(right),
                )))
            }
            Operator::LogicalIntersect(left, right) => {
                let left = left.bottom_up_rewrite(visitor);
                let right = right.bottom_up_rewrite(visitor);
                visitor(Expr(Operator::LogicalIntersect(
                    Box::new(left),
                    Box::new(right),
                )))
            }
            Operator::LogicalExcept(left, right) => {
                let left = left.bottom_up_rewrite(visitor);
                let right = right.bottom_up_rewrite(visitor);
                visitor(Expr(Operator::LogicalExcept(
                    Box::new(left),
                    Box::new(right),
                )))
            }
            Operator::LogicalInsert(table, columns, input) => {
                let input = input.bottom_up_rewrite(visitor);
                visitor(Expr(Operator::LogicalInsert(
                    table,
                    columns,
                    Box::new(input),
                )))
            }
            Operator::LogicalValues(columns, rows, input) => {
                let input = input.bottom_up_rewrite(visitor);
                visitor(Expr(Operator::LogicalValues(
                    columns,
                    rows,
                    Box::new(input),
                )))
            }
            Operator::LogicalUpdate(updates, input) => {
                let input = input.bottom_up_rewrite(visitor);
                visitor(Expr(Operator::LogicalUpdate(updates, Box::new(input))))
            }
            Operator::LogicalDelete(table, input) => {
                let input = input.bottom_up_rewrite(visitor);
                visitor(Expr(Operator::LogicalDelete(table, Box::new(input))))
            }
            _ => visitor(self),
        }
    }

    pub fn top_down_rewrite(self, visitor: &impl Fn(Expr) -> Expr) -> Expr {
        match visitor(self).0 {
            Operator::LogicalFilter(predicates, input) => {
                let input = input.bottom_up_rewrite(visitor);
                Expr(Operator::LogicalFilter(predicates, Box::new(input)))
            }
            Operator::LogicalProject(projects, input) => {
                let input = input.bottom_up_rewrite(visitor);
                Expr(Operator::LogicalProject(projects, Box::new(input)))
            }
            Operator::LogicalJoin(join, left, right) => {
                let left = left.bottom_up_rewrite(visitor);
                let right = right.bottom_up_rewrite(visitor);
                Expr(Operator::LogicalJoin(join, Box::new(left), Box::new(right)))
            }
            Operator::LogicalWith(name, left, right) => {
                let left = left.bottom_up_rewrite(visitor);
                let right = right.bottom_up_rewrite(visitor);
                Expr(Operator::LogicalWith(name, Box::new(left), Box::new(right)))
            }
            Operator::LogicalAggregate {
                group_by,
                aggregate,
                input,
            } => {
                let input = input.bottom_up_rewrite(visitor);
                Expr(Operator::LogicalAggregate {
                    group_by,
                    aggregate,
                    input: Box::new(input),
                })
            }
            Operator::LogicalLimit {
                limit,
                offset,
                input,
            } => {
                let input = input.bottom_up_rewrite(visitor);
                Expr(Operator::LogicalLimit {
                    limit,
                    offset,
                    input: Box::new(input),
                })
            }
            Operator::LogicalSort(sorts, input) => {
                let input = input.bottom_up_rewrite(visitor);
                Expr(Operator::LogicalSort(sorts, Box::new(input)))
            }
            Operator::LogicalUnion(left, right) => {
                let left = left.bottom_up_rewrite(visitor);
                let right = right.bottom_up_rewrite(visitor);
                Expr(Operator::LogicalUnion(Box::new(left), Box::new(right)))
            }
            Operator::LogicalIntersect(left, right) => {
                let left = left.bottom_up_rewrite(visitor);
                let right = right.bottom_up_rewrite(visitor);
                Expr(Operator::LogicalIntersect(Box::new(left), Box::new(right)))
            }
            Operator::LogicalExcept(left, right) => {
                let left = left.bottom_up_rewrite(visitor);
                let right = right.bottom_up_rewrite(visitor);
                Expr(Operator::LogicalExcept(Box::new(left), Box::new(right)))
            }
            Operator::LogicalInsert(table, columns, input) => {
                let input = input.bottom_up_rewrite(visitor);
                Expr(Operator::LogicalInsert(table, columns, Box::new(input)))
            }
            Operator::LogicalValues(columns, rows, input) => {
                let input = input.bottom_up_rewrite(visitor);
                Expr(Operator::LogicalValues(columns, rows, Box::new(input)))
            }
            Operator::LogicalUpdate(updates, input) => {
                let input = input.bottom_up_rewrite(visitor);
                Expr(Operator::LogicalUpdate(updates, Box::new(input)))
            }
            Operator::LogicalDelete(table, input) => {
                let input = input.bottom_up_rewrite(visitor);
                Expr(Operator::LogicalDelete(table, Box::new(input)))
            }
            other => Expr(other),
        }
    }

    fn iter(&self) -> ExprIterator {
        ExprIterator { stack: vec![self] }
    }

    fn print(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        let newline = |f: &mut fmt::Formatter<'_>| -> fmt::Result {
            write!(f, "\n")?;
            for _ in 0..indent + 1 {
                write!(f, "\t")?;
            }
            Ok(())
        };
        match &self.0 {
            Operator::LogicalSingleGet => write!(f, "LogicalSingleGet"),
            Operator::LogicalGet(table) => write!(f, "LogicalGet {}", table.name),
            Operator::LogicalFilter(predicates, input) => {
                write!(f, "LogicalFilter {}", join(predicates))?;
                newline(f)?;
                input.print(f, indent + 1)
            }
            Operator::LogicalProject(projects, input) => {
                let mut strings = vec![];
                for (x, c) in projects {
                    strings.push(format!("{}:{}", c, x));
                }
                write!(f, "LogicalProject {}", strings.join(" "))?;
                newline(f)?;
                input.print(f, indent + 1)
            }
            Operator::LogicalJoin(join, left, right) => {
                write!(f, "LogicalJoin {}", join)?;
                newline(f)?;
                left.print(f, indent + 1)?;
                newline(f)?;
                right.print(f, indent + 1)
            }
            Operator::LogicalWith(name, left, right) => {
                write!(f, "LogicalWith {}", name)?;
                newline(f)?;
                left.print(f, indent + 1)?;
                newline(f)?;
                right.print(f, indent + 1)
            }
            Operator::LogicalGetWith(name) => write!(f, "LogicalGetWith {}", name),
            Operator::LogicalAggregate {
                group_by,
                aggregate,
                input,
            } => {
                write!(f, "LogicalAggregate")?;
                for column in group_by {
                    write!(f, " {}", column)?;
                }
                for (aggregate, column) in aggregate {
                    write!(f, " {}:{}", column, aggregate)?;
                }
                newline(f)?;
                input.print(f, indent + 1)
            }
            Operator::LogicalLimit {
                limit,
                offset,
                input,
            } => {
                write!(f, "LogicalLimit {} {}", limit, offset)?;
                newline(f)?;
                input.print(f, indent + 1)
            }
            Operator::LogicalSort(order_by, input) => {
                write!(f, "LogicalSort")?;
                for sort in order_by {
                    if sort.desc {
                        write!(f, " (Desc {})", sort.column)?;
                    } else {
                        write!(f, " {}", sort.column)?;
                    }
                }
                newline(f)?;
                input.print(f, indent + 1)
            }
            Operator::LogicalUnion(left, right) => {
                write!(f, "LogicalUnion")?;
                newline(f)?;
                left.print(f, indent + 1)?;
                newline(f)?;
                right.print(f, indent + 1)
            }
            Operator::LogicalIntersect(left, right) => {
                write!(f, "LogicalIntersect")?;
                newline(f)?;
                left.print(f, indent + 1)?;
                newline(f)?;
                right.print(f, indent + 1)
            }
            Operator::LogicalExcept(left, right) => {
                write!(f, "LogicalExcept")?;
                newline(f)?;
                left.print(f, indent + 1)?;
                newline(f)?;
                right.print(f, indent + 1)
            }
            Operator::LogicalInsert(table, columns, input) => {
                write!(f, "LogicalInsert {}", table.name)?;
                for c in columns {
                    write!(f, " {}", c)?;
                }
                newline(f)?;
                input.print(f, indent + 1)
            }
            Operator::LogicalValues(columns, rows, input) => {
                write!(f, "LogicalValues")?;
                for column in columns {
                    write!(f, " {}", column)?;
                }
                for row in rows {
                    write!(f, " [{}]", join(row))?;
                }
                newline(f)?;
                input.print(f, indent + 1)
            }
            Operator::LogicalUpdate(updates, input) => {
                write!(f, "LogicalUpdate")?;
                for (target, value) in updates {
                    match value {
                        Some(value) => write!(f, " {}:{}", target, value)?,
                        None => write!(f, " {}:_", target)?,
                    }
                }
                newline(f)?;
                input.print(f, indent + 1)
            }
            Operator::LogicalDelete(table, input) => {
                write!(f, "LogicalDelete {}", table.name)?;
                newline(f)?;
                input.print(f, indent + 1)
            }
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
            Operator::PhysicalFilter(predicates, input) => {
                write!(f, "Filter {}", join(predicates))?;
                newline(f)?;
                input.print(f, indent + 1)
            }
            Operator::PhysicalProject(projects, input) => {
                let mut strings = vec![];
                for (x, c) in projects {
                    strings.push(format!("{}:{}", c, x));
                }
                write!(f, "Project {}", strings.join(" "))?;
                newline(f)?;
                input.print(f, indent + 1)
            }
            Operator::PhysicalNestedLoop(join, left, right) => {
                write!(f, "NestedLoop {}", join)?;
                newline(f)?;
                left.print(f, indent + 1)?;
                newline(f)?;
                right.print(f, indent + 1)
            }
            Operator::PhysicalHashJoin(join, equals, left, right) => {
                write!(f, "HashJoin {}", join)?;
                for (left, right) in equals {
                    write!(f, " {}={}", left, right)?;
                }
                newline(f)?;
                left.print(f, indent + 1)?;
                newline(f)?;
                right.print(f, indent + 1)
            }
            Operator::PhysicalCreateTempTable(name, input) => {
                write!(f, "CreateTempTable {}", name)?;
                newline(f)?;
                input.print(f, indent + 1)
            }
            Operator::PhysicalGetTempTable(name) => write!(f, "GetTempTable {}", name),
            Operator::PhysicalAggregate {
                group_by,
                aggregate,
                input,
            } => {
                write!(f, "Aggregate")?;
                for column in group_by {
                    write!(f, " {}", column)?;
                }
                for (aggregate, column) in aggregate {
                    write!(f, " {}:{}", column, aggregate)?;
                }
                newline(f)?;
                input.print(f, indent + 1)
            }
            Operator::PhysicalLimit {
                limit,
                offset,
                input,
            } => {
                write!(f, "Limit {} {}", limit, offset)?;
                newline(f)?;
                input.print(f, indent + 1)
            }
            Operator::PhysicalSort(order_by, input) => {
                write!(f, "Sort")?;
                for sort in order_by {
                    if sort.desc {
                        write!(f, " (Desc {})", sort.column)?;
                    } else {
                        write!(f, " {}", sort.column)?;
                    }
                }
                newline(f)?;
                input.print(f, indent + 1)
            }
            Operator::PhysicalUnion(left, right) => {
                write!(f, "Union")?;
                newline(f)?;
                left.print(f, indent + 1)?;
                newline(f)?;
                right.print(f, indent + 1)
            }
            Operator::PhysicalIntersect(left, right) => {
                write!(f, "Intersect")?;
                newline(f)?;
                left.print(f, indent + 1)?;
                newline(f)?;
                right.print(f, indent + 1)
            }
            Operator::PhysicalExcept(left, right) => {
                write!(f, "Except")?;
                newline(f)?;
                left.print(f, indent + 1)?;
                newline(f)?;
                right.print(f, indent + 1)
            }
            Operator::PhysicalInsert(table, columns, input) => {
                write!(f, "Insert {}", table.name)?;
                for c in columns {
                    write!(f, " {}", c)?;
                }
                newline(f)?;
                input.print(f, indent + 1)
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
            Operator::PhysicalUpdate(updates, input) => {
                write!(f, "Update")?;
                for (target, value) in updates {
                    match value {
                        Some(value) => write!(f, " {}:{}", target, value)?,
                        None => write!(f, " {}:_", target)?,
                    }
                }
                newline(f)?;
                input.print(f, indent + 1)
            }
            Operator::PhysicalDelete(table, input) => {
                write!(f, "Delete {}", table)?;
                newline(f)?;
                input.print(f, indent + 1)
            }
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

pub struct ExprIterator<'it> {
    stack: Vec<&'it Expr>,
}

impl<'it> Iterator for ExprIterator<'it> {
    type Item = &'it Expr;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next) = self.stack.pop() {
            match next {
                Expr(Operator::LogicalFilter(_, input)) => {
                    self.stack.push(input);
                }
                Expr(Operator::LogicalProject(_, input)) => {
                    self.stack.push(input);
                }
                Expr(Operator::LogicalJoin(_, left, right)) => {
                    self.stack.push(left);
                    self.stack.push(right);
                }
                Expr(Operator::LogicalWith(_, left, right)) => {
                    self.stack.push(left);
                    self.stack.push(right);
                }
                Expr(Operator::LogicalAggregate { input, .. }) => {
                    self.stack.push(input);
                }
                Expr(Operator::LogicalLimit { input, .. }) => {
                    self.stack.push(input);
                }
                Expr(Operator::LogicalSort(_, input)) => {
                    self.stack.push(input);
                }
                Expr(Operator::LogicalUnion(left, right)) => {
                    self.stack.push(left);
                    self.stack.push(right);
                }
                Expr(Operator::LogicalIntersect(left, right)) => {
                    self.stack.push(left);
                    self.stack.push(right);
                }
                Expr(Operator::LogicalExcept(left, right)) => {
                    self.stack.push(left);
                    self.stack.push(right);
                }
                Expr(Operator::LogicalInsert(_, _, input)) => {
                    self.stack.push(input);
                }
                Expr(Operator::LogicalValues(_, _, input)) => {
                    self.stack.push(input);
                }
                Expr(Operator::LogicalUpdate(_, input)) => {
                    self.stack.push(input);
                }
                Expr(Operator::LogicalDelete(_, input)) => {
                    self.stack.push(input);
                }
                _ => {}
            }
            Some(next)
        } else {
            None
        }
    }
}

impl<T> Operator<T> {
    pub fn introduces(&self, column: &Column) -> bool {
        match self {
            Operator::LogicalGet(table) => table.columns.contains(column),
            Operator::LogicalProject(projects, _) => projects.iter().any(|(_, c)| c == column),
            Operator::LogicalJoin(Join::Mark(mark), _, _) => mark == column,
            Operator::LogicalGetWith { .. } => todo!(),
            Operator::LogicalAggregate { aggregate, .. } => {
                aggregate.iter().any(|(_, c)| c == column)
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Join {
    // Inner implements the default SQL join.
    Inner,
    // Right is used for both left and right joins.
    // When parsing a left join, we convert it to a right join by reversing the order of left and right.
    // The left side is the build side of the join.
    Right(Vec<Scalar>),
    // Outer includes non-matching rows from both sides.
    Outer(Vec<Scalar>),
    // Semi indicates a semi-join like `select 1 from customer where store_id in (select store_id from store)`.
    // Note that the left side is the build side of the join, which is the reverse of the usual convention.
    Semi(Column),
    // Anti indicates an anti-join like `select 1 from customer where store_id not in (select store_id from store)`.
    // Note that like Semi, the left side is the build side of the join.
    Anti(Column),
    // Single implements "Single Join" from http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
    // The correlated subquery is always on the left.
    Single,
    // Mark implements "Mark Join" from http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
    // The correlated subquery is always on the left.
    Mark(Column),
}

impl fmt::Display for Join {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Join::Inner => write!(f, "Inner"),
            Join::Right(predicates) => write!(f, "Right {}", join(predicates)),
            Join::Outer(predicates) => write!(f, "Outer {}", join(predicates)),
            Join::Semi(column) => write!(f, "Semi {}", column),
            Join::Anti(column) => write!(f, "Anti {}", column),
            Join::Single => write!(f, "Single"),
            Join::Mark(column) => write!(f, "Mark {}", column),
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
