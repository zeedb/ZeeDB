use std::fmt::{Debug, Display, Formatter};

use crate::{column::Column, AggregateExpr, AggregateFunction, Expr, Index, Join, Scalar, F};

pub trait IndentPrint {
    fn indent_print(&self, f: &mut Formatter<'_>, indent: usize) -> std::fmt::Result;
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.indent_print(f, 0)
    }
}

impl Debug for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.indent_print(f, 0)
    }
}

impl IndentPrint for Expr {
    fn indent_print(&self, f: &mut Formatter<'_>, mut indent: usize) -> std::fmt::Result {
        let newline = |f: &mut Formatter<'_>, indent: usize| -> std::fmt::Result {
            write!(f, "\n")?;
            for _ in 0..indent + 1 {
                write!(f, "\t")?;
            }
            Ok(())
        };
        match self {
            Expr::Leaf { gid } => write!(f, "@{}", gid),
            Expr::LogicalSingleGet | Expr::TableFreeScan { .. } => write!(f, "{}", self.name()),
            Expr::LogicalGet {
                predicates, table, ..
            }
            | Expr::SeqScan {
                predicates, table, ..
            } => {
                let predicates = visible_predicates(predicates);
                if !predicates.is_empty() {
                    write!(f, "Filter* {}", join_scalars(&predicates))?;
                    newline(f, indent)?;
                }
                write!(f, "{} {}", self.name(), table.name)?;
                Ok(())
            }
            Expr::LogicalFilter { predicates, input } | Expr::Filter { predicates, input } => {
                write!(f, "{} {}", self.name(), join_scalars(predicates))?;
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Expr::LogicalOut { input, .. } | Expr::Out { input, .. } => {
                // Don't print output node.
                input.indent_print(f, indent)
            }
            Expr::LogicalMap {
                include_existing,
                projects,
                input,
            }
            | Expr::Map {
                include_existing,
                projects,
                input,
            } => {
                write!(f, "{} {}", self.name(), join_projects(projects))?;
                if *include_existing {
                    write!(f, ", ..")?;
                }
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Expr::LogicalJoin { join, left, right } => {
                write!(f, "{} {}", self.name(), join)?;
                newline(f, indent)?;
                left.indent_print(f, indent + 1)?;
                newline(f, indent)?;
                right.indent_print(f, indent + 1)
            }
            Expr::LogicalDependentJoin {
                parameters,
                predicates,
                subquery,
                domain,
            } => {
                if !predicates.is_empty() {
                    write!(f, "Filter* {}", join_scalars(predicates))?;
                    newline(f, indent)?;
                    indent += 1;
                }
                write!(f, "{}", self.name())?;
                if !parameters.is_empty() {
                    write!(f, " [{}]", join_columns(parameters))?;
                }
                newline(f, indent)?;
                subquery.indent_print(f, indent + 1)?;
                newline(f, indent)?;
                domain.indent_print(f, indent + 1)
            }
            Expr::NestedLoop { join, left, right } => {
                write!(f, "{} {}", self.name(), join)?;
                newline(f, indent)?;
                left.indent_print(f, indent + 1)?;
                newline(f, indent)?;
                right.indent_print(f, indent + 1)
            }
            Expr::LogicalWith {
                name, left, right, ..
            } => {
                write!(f, "{} {}", self.name(), name)?;
                newline(f, indent)?;
                left.indent_print(f, indent + 1)?;
                newline(f, indent)?;
                right.indent_print(f, indent + 1)
            }
            Expr::LogicalCreateTempTable {
                name,
                columns,
                input,
            }
            | Expr::CreateTempTable {
                name,
                columns,
                input,
            } => {
                write!(f, "{} {}", self.name(), name)?;
                for c in columns {
                    write!(f, " {} {}", c.name, c.data_type)?;
                }
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Expr::LogicalGetWith { name, .. } | Expr::GetTempTable { name, .. } => {
                write!(f, "{} {}", self.name(), name)
            }
            Expr::LogicalAggregate {
                group_by,
                aggregate,
                input,
            }
            | Expr::GroupByAggregate {
                group_by,
                aggregate,
                input,
                ..
            } => {
                write!(f, "{}", self.name())?;
                for column in group_by {
                    write!(f, " {}", column)?;
                }
                for AggregateExpr {
                    function,
                    distinct,
                    input,
                    output,
                } in aggregate
                {
                    if *distinct {
                        write!(f, " {}:({} Distinct {})", output, function, input)?;
                    } else {
                        write!(f, " {}:({} {})", output, function, input)?;
                    }
                }
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Expr::SimpleAggregate {
                aggregate, input, ..
            } => {
                write!(f, "{}", self.name())?;
                for AggregateExpr {
                    function,
                    distinct,
                    input,
                    output,
                } in aggregate
                {
                    if *distinct {
                        write!(f, " {}:({} Distinct {})", output, function, input)?;
                    } else {
                        write!(f, " {}:({} {})", output, function, input)?;
                    }
                }
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Expr::LogicalLimit {
                limit,
                offset,
                input,
            }
            | Expr::Limit {
                limit,
                offset,
                input,
            } => {
                write!(f, "{} {} {}", self.name(), limit, offset)?;
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Expr::LogicalSort { order_by, input } | Expr::Sort { order_by, input } => {
                write!(f, "{}", self.name())?;
                for sort in order_by {
                    if sort.descending {
                        write!(f, " (Desc {})", sort.column)?;
                    } else {
                        write!(f, " {}", sort.column)?;
                    }
                }
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Expr::LogicalUnion { left, right, .. } | Expr::Union { left, right } => {
                write!(f, "{}", self.name())?;
                newline(f, indent)?;
                left.indent_print(f, indent + 1)?;
                newline(f, indent)?;
                right.indent_print(f, indent + 1)?;
                Ok(())
            }
            Expr::Broadcast { input, .. } | Expr::Gather { input, .. } => {
                write!(f, "{}", self.name())?;
                newline(f, indent)?;
                input.indent_print(f, indent + 1)?;
                Ok(())
            }
            Expr::Exchange {
                hash_column, input, ..
            } => {
                write!(
                    f,
                    "{} {}",
                    self.name(),
                    match hash_column {
                        Some(name) => &name.name,
                        None => "?",
                    }
                )?;
                newline(f, indent)?;
                input.indent_print(f, indent + 1)?;
                Ok(())
            }
            Expr::LogicalInsert { table, input, .. } | Expr::Insert { table, input, .. } => {
                write!(f, "{} {}", self.name(), table.name)?;
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Expr::LogicalValues {
                columns,
                values,
                input,
            }
            | Expr::Values {
                columns,
                values,
                input,
            } => {
                write!(f, "{}", self.name())?;
                for column in columns {
                    write!(f, " {}", column)?;
                }
                let num_rows = values.first().map(|column| column.len()).unwrap_or(0);
                let rows: Vec<Vec<Scalar>> = (0..num_rows.min(3))
                    .map(|i| values.iter().map(|column| column[i].clone()).collect())
                    .collect();
                for row in rows {
                    write!(f, " [{}]", join_scalars(&row))?;
                }
                if num_rows > 3 {
                    write!(f, " ...{} additional rows", num_rows - 3)?;
                }
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Expr::LogicalUpdate { table, input, .. }
            | Expr::LogicalDelete { table, input, .. }
            | Expr::Delete { table, input, .. } => {
                write!(f, "{} {}", self.name(), table.name)?;
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Expr::LogicalCreateDatabase { name, .. } => {
                write!(f, "{} {}", self.name(), name.path.join("."))
            }
            Expr::LogicalCreateTable { name, columns, .. } => {
                write!(f, "{} {}", self.name(), name)?;
                for (name, data_type) in columns {
                    write!(f, " {}:{}", name, data_type)?;
                }
                Ok(())
            }
            Expr::LogicalCreateIndex {
                name,
                table,
                columns,
                ..
            } => write!(
                f,
                "{} {} {} {}",
                self.name(),
                name,
                table,
                columns.join(" ")
            ),
            Expr::LogicalDrop { object, name } => {
                write!(f, "{} {:?} {}", self.name(), object, name)
            }
            Expr::LogicalRewrite { sql } => write!(f, "{} {:?}", self.name(), sql),
            Expr::IndexScan {
                predicates,
                lookup,
                index,
                table,
                input,
                ..
            } => {
                let predicates = visible_predicates(predicates);
                if !predicates.is_empty() {
                    write!(f, "Filter* {}", join_scalars(&predicates))?;
                    newline(f, indent)?;
                    indent += 1;
                }
                write!(
                    f,
                    "{} {}({})",
                    self.name(),
                    table.name,
                    join_index_lookups(index, lookup)
                )?;
                newline(f, indent + 1)?;
                input.indent_print(f, indent + 2)
            }
            Expr::HashJoin {
                join, left, right, ..
            } => {
                write!(f, "{} {}", self.name(), join)?;
                newline(f, indent)?;
                left.indent_print(f, indent + 1)?;
                newline(f, indent)?;
                right.indent_print(f, indent + 1)
            }
            Expr::LogicalScript { stmts } | Expr::Script { stmts } => {
                write!(f, "{}", self.name())?;
                for expr in stmts {
                    newline(f, indent)?;
                    expr.indent_print(f, indent + 1)?;
                }
                Ok(())
            }
            Expr::LogicalCall {
                procedure, input, ..
            }
            | Expr::Call {
                procedure, input, ..
            } => {
                write!(f, "{} {}", self.name(), procedure)?;
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
            Expr::LogicalExplain { input, .. } | Expr::Explain { input, .. } => {
                write!(f, "{}", self.name())?;
                newline(f, indent)?;
                input.indent_print(f, indent + 1)
            }
        }
    }
}

impl Expr {
    pub fn name(&self) -> &str {
        match self {
            Expr::Leaf { .. } => "Leaf",
            Expr::LogicalSingleGet { .. } => "LogicalSingleGet",
            Expr::LogicalGet { .. } => "LogicalGet",
            Expr::LogicalFilter { .. } => "LogicalFilter",
            Expr::LogicalOut { .. } => "LogicalOut",
            Expr::LogicalMap { .. } => "LogicalMap",
            Expr::LogicalJoin { .. } => "LogicalJoin",
            Expr::LogicalDependentJoin { .. } => "LogicalDependentJoin",
            Expr::LogicalWith { .. } => "LogicalWith",
            Expr::LogicalCreateTempTable { .. } => "LogicalCreateTempTable",
            Expr::LogicalGetWith { .. } => "LogicalGetWith",
            Expr::LogicalAggregate { .. } => "LogicalAggregate",
            Expr::LogicalLimit { .. } => "LogicalLimit",
            Expr::LogicalSort { .. } => "LogicalSort",
            Expr::LogicalUnion { .. } => "LogicalUnion",
            Expr::LogicalInsert { .. } => "LogicalInsert",
            Expr::LogicalValues { .. } => "LogicalValues",
            Expr::LogicalUpdate { .. } => "LogicalUpdate",
            Expr::LogicalDelete { .. } => "LogicalDelete",
            Expr::LogicalCreateDatabase { .. } => "LogicalCreateDatabase",
            Expr::LogicalCreateTable { .. } => "LogicalCreateTable",
            Expr::LogicalCreateIndex { .. } => "LogicalCreateIndex",
            Expr::LogicalDrop { .. } => "LogicalDrop",
            Expr::LogicalScript { .. } => "LogicalScript",
            Expr::LogicalCall { .. } => "LogicalCall",
            Expr::LogicalExplain { .. } => "LogicalExplain",
            Expr::LogicalRewrite { .. } => "LogicalRewrite",
            Expr::TableFreeScan { .. } => "TableFreeScan",
            Expr::SeqScan { .. } => "SeqScan",
            Expr::IndexScan { .. } => "IndexScan",
            Expr::Filter { .. } => "Filter",
            Expr::Out { .. } => "Out",
            Expr::Map { .. } => "Map",
            Expr::NestedLoop { .. } => "NestedLoop",
            Expr::HashJoin { .. } => "HashJoin",
            Expr::CreateTempTable { .. } => "CreateTempTable",
            Expr::GetTempTable { .. } => "GetTempTable",
            Expr::SimpleAggregate { .. } => "SimpleAggregate",
            Expr::GroupByAggregate { .. } => "GroupByAggregate",
            Expr::Limit { .. } => "Limit",
            Expr::Sort { .. } => "Sort",
            Expr::Union { .. } => "Union",
            Expr::Broadcast { .. } => "Broadcast",
            Expr::Exchange { .. } => "Exchange",
            Expr::Gather { .. } => "Gather",
            Expr::Insert { .. } => "Insert",
            Expr::Values { .. } => "Values",
            Expr::Delete { .. } => "Delete",
            Expr::Script { .. } => "Script",
            Expr::Call { .. } => "Call",
            Expr::Explain { .. } => "Explain",
        }
    }
}

impl Display for Join {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

impl Display for Scalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Scalar::Literal(value) => write!(f, "{}", value),
            Scalar::Parameter(name, _) => write!(f, "@{}", name),
            Scalar::Column(column) => write!(f, "{}", column),
            Scalar::Call(function) => {
                if function.arguments().is_empty() {
                    write!(f, "({})", function.name())
                } else {
                    let arguments: Vec<String> = function
                        .arguments()
                        .iter()
                        .map(|argument| argument.to_string())
                        .collect();
                    write!(f, "({} {})", function.name(), arguments.join(" "))
                }
            }
            Scalar::Cast(value, data_type) => write!(f, "(Cast {} {})", value, data_type),
        }
    }
}

impl Display for AggregateFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateFunction::AnyValue => write!(f, "AnyValue"),
            AggregateFunction::Count => write!(f, "Count"),
            AggregateFunction::LogicalAnd => write!(f, "LogicalAnd"),
            AggregateFunction::LogicalOr => write!(f, "LogicalOr"),
            AggregateFunction::Max => write!(f, "Max"),
            AggregateFunction::Min => write!(f, "Min"),
            AggregateFunction::Sum => write!(f, "Sum"),
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

fn join_index_lookups(index: &Index, lookup: &Vec<Scalar>) -> String {
    let mut strings = vec![];
    for i in 0..index.columns.len() {
        strings.push(format!("{}:{}", index.columns[i], lookup[i]));
    }
    strings.join(" ")
}

/// Hide (LessOrEqual $xmin (Xid)) (Less (Xid) $xmax)
fn visible_predicates(predicates: &Vec<Scalar>) -> Vec<Scalar> {
    predicates
        .iter()
        .filter(|p| {
            match p {
                Scalar::Call(f) => match f.as_ref() {
                    F::LessOrEqual(Scalar::Column(c), Scalar::Call(f))
                        if c.name == "$xmin" && f.as_ref() == &F::Xid =>
                    {
                        return false;
                    }
                    F::Less(Scalar::Call(f), Scalar::Column(c))
                        if f.as_ref() == &F::Xid && c.name == "$xmax" =>
                    {
                        return false;
                    }
                    _ => {}
                },
                _ => {}
            }
            true
        })
        .cloned()
        .collect()
}
