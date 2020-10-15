use crate::operator::*;
use std::fmt;

pub trait IndentPrint {
    fn indent_print(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result;
}

impl<T: IndentPrint> IndentPrint for Operator<T> {
    fn indent_print(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        let newline = |f: &mut fmt::Formatter<'_>| -> fmt::Result {
            write!(f, "\n")?;
            for _ in 0..indent + 1 {
                write!(f, "\t")?;
            }
            Ok(())
        };
        match self {
            Operator::LogicalSingleGet => write!(f, "LogicalSingleGet"),
            Operator::LogicalGet(table) => write!(f, "LogicalGet {}", table.name),
            Operator::LogicalFilter(predicates, input) => {
                write!(f, "LogicalFilter {}", join(predicates))?;
                newline(f)?;
                input.indent_print(f, indent + 1)
            }
            Operator::LogicalProject(projects, input) => {
                let mut strings = vec![];
                for (x, c) in projects {
                    strings.push(format!("{}:{}", c, x));
                }
                write!(f, "LogicalProject {}", strings.join(" "))?;
                newline(f)?;
                input.indent_print(f, indent + 1)
            }
            Operator::LogicalJoin(join, left, right) => {
                write!(f, "LogicalJoin {}", join)?;
                newline(f)?;
                left.indent_print(f, indent + 1)?;
                newline(f)?;
                right.indent_print(f, indent + 1)
            }
            Operator::LogicalWith(name, left, right) => {
                write!(f, "LogicalWith {}", name)?;
                newline(f)?;
                left.indent_print(f, indent + 1)?;
                newline(f)?;
                right.indent_print(f, indent + 1)
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
                input.indent_print(f, indent + 1)
            }
            Operator::LogicalLimit {
                limit,
                offset,
                input,
            } => {
                write!(f, "LogicalLimit {} {}", limit, offset)?;
                newline(f)?;
                input.indent_print(f, indent + 1)
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
                input.indent_print(f, indent + 1)
            }
            Operator::LogicalUnion(left, right) => {
                write!(f, "LogicalUnion")?;
                newline(f)?;
                left.indent_print(f, indent + 1)?;
                newline(f)?;
                right.indent_print(f, indent + 1)
            }
            Operator::LogicalIntersect(left, right) => {
                write!(f, "LogicalIntersect")?;
                newline(f)?;
                left.indent_print(f, indent + 1)?;
                newline(f)?;
                right.indent_print(f, indent + 1)
            }
            Operator::LogicalExcept(left, right) => {
                write!(f, "LogicalExcept")?;
                newline(f)?;
                left.indent_print(f, indent + 1)?;
                newline(f)?;
                right.indent_print(f, indent + 1)
            }
            Operator::LogicalInsert(table, columns, input) => {
                write!(f, "LogicalInsert {}", table.name)?;
                for c in columns {
                    write!(f, " {}", c)?;
                }
                newline(f)?;
                input.indent_print(f, indent + 1)
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
                input.indent_print(f, indent + 1)
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
                input.indent_print(f, indent + 1)
            }
            Operator::LogicalDelete(table, input) => {
                write!(f, "LogicalDelete {}", table.name)?;
                newline(f)?;
                input.indent_print(f, indent + 1)
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
                input.indent_print(f, indent + 1)
            }
            Operator::PhysicalProject(projects, input) => {
                let mut strings = vec![];
                for (x, c) in projects {
                    strings.push(format!("{}:{}", c, x));
                }
                write!(f, "Project {}", strings.join(" "))?;
                newline(f)?;
                input.indent_print(f, indent + 1)
            }
            Operator::PhysicalNestedLoop(join, left, right) => {
                write!(f, "NestedLoop {}", join)?;
                newline(f)?;
                left.indent_print(f, indent + 1)?;
                newline(f)?;
                right.indent_print(f, indent + 1)
            }
            Operator::PhysicalHashJoin(join, equals, left, right) => {
                write!(f, "HashJoin {}", join)?;
                for (left, right) in equals {
                    write!(f, " {}={}", left, right)?;
                }
                newline(f)?;
                left.indent_print(f, indent + 1)?;
                newline(f)?;
                right.indent_print(f, indent + 1)
            }
            Operator::PhysicalCreateTempTable(name, input) => {
                write!(f, "CreateTempTable {}", name)?;
                newline(f)?;
                input.indent_print(f, indent + 1)
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
                input.indent_print(f, indent + 1)
            }
            Operator::PhysicalLimit {
                limit,
                offset,
                input,
            } => {
                write!(f, "Limit {} {}", limit, offset)?;
                newline(f)?;
                input.indent_print(f, indent + 1)
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
                input.indent_print(f, indent + 1)
            }
            Operator::PhysicalUnion(left, right) => {
                write!(f, "Union")?;
                newline(f)?;
                left.indent_print(f, indent + 1)?;
                newline(f)?;
                right.indent_print(f, indent + 1)
            }
            Operator::PhysicalIntersect(left, right) => {
                write!(f, "Intersect")?;
                newline(f)?;
                left.indent_print(f, indent + 1)?;
                newline(f)?;
                right.indent_print(f, indent + 1)
            }
            Operator::PhysicalExcept(left, right) => {
                write!(f, "Except")?;
                newline(f)?;
                left.indent_print(f, indent + 1)?;
                newline(f)?;
                right.indent_print(f, indent + 1)
            }
            Operator::PhysicalInsert(table, columns, input) => {
                write!(f, "Insert {}", table.name)?;
                for c in columns {
                    write!(f, " {}", c)?;
                }
                newline(f)?;
                input.indent_print(f, indent + 1)
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
                input.indent_print(f, indent + 1)
            }
            Operator::PhysicalDelete(table, input) => {
                write!(f, "Delete {}", table)?;
                newline(f)?;
                input.indent_print(f, indent + 1)
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

impl<T> Operator<T> {
    pub fn name(&self) -> String {
        match self {
            Operator::LogicalSingleGet { .. } => "LogicalSingleGet".to_string(),
            Operator::LogicalGet { .. } => "LogicalGet".to_string(),
            Operator::LogicalFilter { .. } => "LogicalFilter".to_string(),
            Operator::LogicalProject { .. } => "LogicalProject".to_string(),
            Operator::LogicalJoin { .. } => "LogicalJoin".to_string(),
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
            Operator::PhysicalTableFreeScan { .. } => "PhysicalTableFreeScan".to_string(),
            Operator::PhysicalSeqScan { .. } => "PhysicalSeqScan".to_string(),
            Operator::PhysicalIndexScan { .. } => "PhysicalIndexScan".to_string(),
            Operator::PhysicalFilter { .. } => "PhysicalFilter".to_string(),
            Operator::PhysicalProject { .. } => "PhysicalProject".to_string(),
            Operator::PhysicalNestedLoop { .. } => "PhysicalNestedLoop".to_string(),
            Operator::PhysicalHashJoin { .. } => "PhysicalHashJoin".to_string(),
            Operator::PhysicalCreateTempTable { .. } => "PhysicalCreateTempTable".to_string(),
            Operator::PhysicalGetTempTable { .. } => "PhysicalGetTempTable".to_string(),
            Operator::PhysicalAggregate { .. } => "PhysicalAggregate".to_string(),
            Operator::PhysicalLimit { .. } => "PhysicalLimit".to_string(),
            Operator::PhysicalSort { .. } => "PhysicalSort".to_string(),
            Operator::PhysicalUnion { .. } => "PhysicalUnion".to_string(),
            Operator::PhysicalIntersect { .. } => "PhysicalIntersect".to_string(),
            Operator::PhysicalExcept { .. } => "PhysicalExcept".to_string(),
            Operator::PhysicalInsert { .. } => "PhysicalInsert".to_string(),
            Operator::PhysicalValues { .. } => "PhysicalValues".to_string(),
            Operator::PhysicalUpdate { .. } => "PhysicalUpdate".to_string(),
            Operator::PhysicalDelete { .. } => "PhysicalDelete".to_string(),
            Operator::PhysicalCreateDatabase { .. } => "PhysicalCreateDatabase".to_string(),
            Operator::PhysicalCreateTable { .. } => "PhysicalCreateTable".to_string(),
            Operator::PhysicalCreateIndex { .. } => "PhysicalCreateIndex".to_string(),
            Operator::PhysicalAlterTable { .. } => "PhysicalAlterTable".to_string(),
            Operator::PhysicalDrop { .. } => "PhysicalDrop".to_string(),
            Operator::PhysicalRename { .. } => "PhysicalRename".to_string(),
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
