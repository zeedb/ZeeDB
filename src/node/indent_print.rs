use crate::operator::*;
use std::fmt;

pub trait IndentPrint {
    fn indent_print(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result;
}

impl<T: IndentPrint> IndentPrint for Operator<T> {
    fn indent_print(&self, f: &mut fmt::Formatter<'_>, mut indent: usize) -> fmt::Result {
        let newline = |f: &mut fmt::Formatter<'_>, indent: usize| -> fmt::Result {
            write!(f, "\n")?;
            for _ in 0..indent + 1 {
                write!(f, "\t")?;
            }
            Ok(())
        };
        match self {
            Operator::LogicalSingleGet => write!(f, "{}", self.name()),
            Operator::LogicalGet {
                projects,
                predicates,
                table,
            }
            | Operator::SeqScan {
                projects,
                predicates,
                table,
            } => {
                if !predicates.is_empty() {
                    write!(f, "Filter* {}", join_scalars(predicates))?;
                    newline(f, indent)?;
                    indent += 1;
                }
                write!(f, "Map* {}", join_columns(projects))?;
                newline(f, indent)?;
                write!(f, "{} {}", self.name(), table.name)?;
                Ok(())
            }
            Operator::LogicalFilter(predicates, input) => {
                write!(f, "{} {}", self.name(), join_scalars(predicates))?;
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::LogicalMap(projects, input) => {
                write!(f, "{} {}", self.name(), join_projects(projects))?;
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::LogicalJoin {
                parameters,
                join,
                left,
                right,
            } => {
                write!(f, "{} {}", self.name(), join)?;
                if !parameters.is_empty() {
                    write!(f, " [{}]", join_columns(parameters))?;
                }
                newline(f, indent)?;
                left.indent_print(f, indent + 1)?;
                newline(f, indent)?;
                right.indent_print(f, indent + 1)
            }
            Operator::NestedLoop(join, left, right) => {
                write!(f, "{} {}", self.name(), join)?;
                newline(f, indent)?;
                left.indent_print(f, indent + 1)?;
                newline(f, indent)?;
                right.indent_print(f, indent + 1)
            }
            Operator::LogicalProject(projects, input) | Operator::Project(projects, input) => {
                write!(f, "{} {}", self.name(), join_columns(projects))?;
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::LogicalWith(name, _, left, right)
            | Operator::CreateTempTable(name, _, left, right) => {
                write!(f, "{} {}", self.name(), name)?;
                newline(f, indent)?;
                left.indent_print(f, indent + 1)?;
                newline(f, indent)?;
                right.indent_print(f, indent + 1)
            }
            Operator::LogicalGetWith(name, _) | Operator::GetTempTable(name, _) => {
                write!(f, "{} {}", self.name(), name)
            }
            Operator::LogicalAggregate {
                group_by,
                aggregate,
                input,
            } => {
                write!(f, "{}", self.name())?;
                for column in group_by {
                    write!(f, " {}", column)?;
                }
                for (aggregate, column) in aggregate {
                    write!(f, " {}:{}", column, aggregate)?;
                }
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::LogicalLimit {
                limit,
                offset,
                input,
            } => {
                write!(f, "{} {} {}", self.name(), limit, offset)?;
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::LogicalSort(order_by, input) => {
                write!(f, "{}", self.name())?;
                for sort in order_by {
                    if sort.desc {
                        write!(f, " (Desc {})", sort.column)?;
                    } else {
                        write!(f, " {}", sort.column)?;
                    }
                }
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::LogicalUnion(left, right)
            | Operator::LogicalIntersect(left, right)
            | Operator::LogicalExcept(left, right)
            | Operator::Union(left, right)
            | Operator::Intersect(left, right)
            | Operator::Except(left, right) => {
                write!(f, "{}", self.name())?;
                newline(f, indent)?;
                left.indent_print(f, indent + 1)?;
                newline(f, indent)?;
                right.indent_print(f, indent + 1)?;
                Ok(())
            }
            Operator::LogicalInsert(table, columns, input) => {
                write!(f, "{} {}", self.name(), table.name)?;
                for c in columns {
                    write!(f, " {}", c)?;
                }
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::LogicalValues(columns, rows, input) => {
                write!(f, "{}", self.name())?;
                for column in columns {
                    write!(f, " {}", column)?;
                }
                for row in rows {
                    write!(f, " [{}]", join_scalars(row))?;
                }
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::LogicalUpdate(updates, input) => {
                write!(f, "{}", self.name())?;
                for (target, value) in updates {
                    match value {
                        Some(value) => write!(f, " {}:{}", target, value)?,
                        None => write!(f, " {}:_", target)?,
                    }
                }
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::LogicalDelete(table, input) => {
                write!(f, "{} {}", self.name(), table.name)?;
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::LogicalCreateDatabase(name) => {
                write!(f, "{} {}", self.name(), name.path.join("."))
            }
            Operator::LogicalCreateTable {
                name,
                columns,
                partition_by,
                cluster_by,
                primary_key,
                input,
            } => {
                write!(f, "{} {}", self.name(), name)?;
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
                if let Some(input) = input {
                    newline(f, indent)?;
                    input.indent_print(f, indent + 1)?;
                }
                Ok(())
            }
            Operator::LogicalCreateIndex {
                name,
                table,
                columns,
            } => write!(
                f,
                "{} {} {} {}",
                self.name(),
                name,
                table,
                columns.join(" ")
            ),
            Operator::LogicalAlterTable { name, actions } => {
                write!(f, "{} {}", self.name(), name)?;
                for a in actions {
                    write!(f, " {}", a)?;
                }
                Ok(())
            }
            Operator::LogicalDrop { object, name } => {
                write!(f, "{} {:?} {}", self.name(), object, name)
            }
            Operator::LogicalRename { object, from, to } => {
                write!(f, "{} {:?} {} {}", self.name(), object, from, to)
            }
            Operator::TableFreeScan => write!(f, "{}", self.name()),
            Operator::IndexScan {
                projects,
                predicates,
                table,
                equals,
            } => {
                if !predicates.is_empty() {
                    write!(f, "Filter* {}", join_scalars(predicates))?;
                    newline(f, indent)?;
                    indent += 1;
                }
                write!(f, "Map* {}", join_columns(projects))?;
                newline(f, indent)?;
                write!(
                    f,
                    "{} {}({})",
                    self.name(),
                    table.name,
                    join_index_lookups(equals)
                )?;
                Ok(())
            }
            Operator::Filter(predicates, input) => {
                write!(f, "{} {}", self.name(), join_scalars(predicates))?;
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::Map(projects, input) => {
                write!(f, "{} {}", self.name(), join_projects(projects))?;
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::HashJoin(join, equals, left, right) => {
                write!(f, "{} {}", self.name(), join)?;
                for (left, right) in equals {
                    write!(f, " {}={}", left, right)?;
                }
                newline(f, indent)?;
                left.indent_print(f, indent + 1)?;
                newline(f, indent)?;
                right.indent_print(f, indent + 1)
            }
            Operator::Aggregate {
                group_by,
                aggregate,
                input,
            } => {
                write!(f, "{}", self.name())?;
                for column in group_by {
                    write!(f, " {}", column)?;
                }
                for (aggregate, column) in aggregate {
                    write!(f, " {}:{}", column, aggregate)?;
                }
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::Limit {
                limit,
                offset,
                input,
            } => {
                write!(f, "{} {} {}", self.name(), limit, offset)?;
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::Sort(order_by, input) => {
                write!(f, "{}", self.name())?;
                for sort in order_by {
                    if sort.desc {
                        write!(f, " (Desc {})", sort.column)?;
                    } else {
                        write!(f, " {}", sort.column)?;
                    }
                }
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::Insert(table, columns, input) => {
                write!(
                    f,
                    "{} {} {}",
                    self.name(),
                    table.name,
                    join_columns(columns)
                )?;
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::Values(columns, rows, input) => {
                write!(f, "{} {}", self.name(), join_columns(columns))?;
                for row in rows {
                    write!(f, " [{}]", join_scalars(row))?;
                }
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::Update(updates, input) => {
                write!(f, "{}", self.name())?;
                for (target, value) in updates {
                    match value {
                        Some(value) => write!(f, " {}:{}", target, value)?,
                        None => write!(f, " {}:_", target)?,
                    }
                }
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::Delete(table, input) => {
                write!(f, "{} {}", self.name(), table)?;
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Operator::CreateDatabase(name) => write!(f, "{} {}", self.name(), name),
            Operator::CreateTable {
                name,
                columns,
                partition_by,
                cluster_by,
                primary_key,
                input,
            } => {
                write!(f, "{} {}", self.name(), name)?;
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
                if let Some(input) = input {
                    newline(f, indent)?;
                    input.indent_print(f, indent + 1)?;
                }
                Ok(())
            }
            Operator::CreateIndex {
                name,
                table,
                columns,
            } => write!(
                f,
                "{} {} {} {}",
                self.name(),
                name,
                table,
                columns.join(" ")
            ),
            Operator::AlterTable { name, actions } => {
                write!(f, "{} {}", self.name(), name)?;
                for a in actions {
                    write!(f, " {}", a)?;
                }
                Ok(())
            }
            Operator::Drop { object, name } => write!(f, "Drop {:?} {}", object, name),
            Operator::Rename { object, from, to } => {
                write!(f, "{} {:?} {} {}", self.name(), object, from, to)
            }
        }
    }
}

impl<T> Operator<T> {
    pub fn name(&self) -> String {
        match self {
            Operator::LogicalSingleGet { .. } => "LogicalSingleGet".to_string(),
            Operator::LogicalGet { .. } => "LogicalGet".to_string(),
            Operator::LogicalFilter { .. } => "LogicalFilter".to_string(),
            Operator::LogicalMap { .. } => "LogicalMap".to_string(),
            Operator::LogicalJoin { .. } => "LogicalJoin".to_string(),
            Operator::LogicalProject { .. } => "LogicalProject".to_string(),
            Operator::LogicalWith { .. } => "LogicalWith".to_string(),
            Operator::LogicalGetWith { .. } => "LogicalGetWith".to_string(),
            Operator::LogicalAggregate { .. } => "LogicalAggregate".to_string(),
            Operator::LogicalLimit { .. } => "LogicalLimit".to_string(),
            Operator::LogicalSort { .. } => "LogicalSort".to_string(),
            Operator::LogicalUnion { .. } => "LogicalUnion".to_string(),
            Operator::LogicalIntersect { .. } => "LogicalIntersect".to_string(),
            Operator::LogicalExcept { .. } => "LogicalExcept".to_string(),
            Operator::LogicalInsert { .. } => "LogicalInsert".to_string(),
            Operator::LogicalValues { .. } => "LogicalValues".to_string(),
            Operator::LogicalUpdate { .. } => "LogicalUpdate".to_string(),
            Operator::LogicalDelete { .. } => "LogicalDelete".to_string(),
            Operator::LogicalCreateDatabase { .. } => "LogicalCreateDatabase".to_string(),
            Operator::LogicalCreateTable { .. } => "LogicalCreateTable".to_string(),
            Operator::LogicalCreateIndex { .. } => "LogicalCreateIndex".to_string(),
            Operator::LogicalAlterTable { .. } => "LogicalAlterTable".to_string(),
            Operator::LogicalDrop { .. } => "LogicalDrop".to_string(),
            Operator::LogicalRename { .. } => "LogicalRename".to_string(),
            Operator::TableFreeScan { .. } => "TableFreeScan".to_string(),
            Operator::SeqScan { .. } => "SeqScan".to_string(),
            Operator::IndexScan { .. } => "IndexScan".to_string(),
            Operator::Filter { .. } => "Filter".to_string(),
            Operator::Map { .. } => "Map".to_string(),
            Operator::NestedLoop { .. } => "NestedLoop".to_string(),
            Operator::HashJoin { .. } => "HashJoin".to_string(),
            Operator::CreateTempTable { .. } => "CreateTempTable".to_string(),
            Operator::GetTempTable { .. } => "GetTempTable".to_string(),
            Operator::Aggregate { .. } => "Aggregate".to_string(),
            Operator::Project { .. } => "Project".to_string(),
            Operator::Limit { .. } => "Limit".to_string(),
            Operator::Sort { .. } => "Sort".to_string(),
            Operator::Union { .. } => "Union".to_string(),
            Operator::Intersect { .. } => "Intersect".to_string(),
            Operator::Except { .. } => "Except".to_string(),
            Operator::Insert { .. } => "Insert".to_string(),
            Operator::Values { .. } => "Values".to_string(),
            Operator::Update { .. } => "Update".to_string(),
            Operator::Delete { .. } => "Delete".to_string(),
            Operator::CreateDatabase { .. } => "CreateDatabase".to_string(),
            Operator::CreateTable { .. } => "CreateTable".to_string(),
            Operator::CreateIndex { .. } => "CreateIndex".to_string(),
            Operator::AlterTable { .. } => "AlterTable".to_string(),
            Operator::Drop { .. } => "Drop".to_string(),
            Operator::Rename { .. } => "Rename".to_string(),
        }
    }
}

impl fmt::Display for Join {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn suffix(xs: &Vec<Scalar>) -> String {
            if xs.is_empty() {
                "".to_string()
            } else {
                format!(" {}", join_scalars(xs))
            }
        }
        match self {
            Join::Inner(predicates) => write!(f, "Inner{}", suffix(predicates)),
            Join::Right(predicates) => write!(f, "Right{}", suffix(predicates)),
            Join::Outer(predicates) => write!(f, "Outer{}", suffix(predicates)),
            Join::Semi(predicates) => write!(f, "Semi{}", suffix(predicates)),
            Join::Anti(predicates) => write!(f, "Anti{}", suffix(predicates)),
            Join::Single(predicates) => write!(f, "Single{}", suffix(predicates)),
            Join::Mark(column, predicates) => write!(f, "Mark {}{}", column, suffix(predicates)),
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
                    write!(f, "({:?} {})", function, join_scalars(arguments))
                }
            }
            Scalar::Cast(value, typ) => write!(f, "(Cast {} {})", value, typ),
        }
    }
}

fn join_projects(projects: &Vec<(Scalar, Column)>) -> String {
    let mut strings = vec![];
    for (x, c) in projects {
        if x == &Scalar::Column(c.clone()) {
            strings.push(format!("{}", c));
        } else {
            strings.push(format!("{}:{}", c, x));
        }
    }
    strings.join(" ")
}

fn join_columns(cs: &Vec<Column>) -> String {
    let mut strings = vec![];
    for c in cs {
        strings.push(format!("{}", c));
    }
    strings.join(" ")
}

fn join_scalars(xs: &Vec<Scalar>) -> String {
    let mut strings = vec![];
    for x in xs {
        strings.push(format!("{}", x));
    }
    strings.join(" ")
}

fn join_index_lookups(equals: &Vec<(Column, Scalar)>) -> String {
    let mut strings = vec![];
    for (x, c) in equals {
        strings.push(format!("{}:{}", c, x));
    }
    strings.join(" ")
}
