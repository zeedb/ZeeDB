use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
};

use ast::*;
use remote_execution::RemoteExecution;
use statistics::{ColumnStatistics, NotNan};

use crate::search_space::*;

#[derive(Clone)]
pub struct LogicalProps {
    // cardinality contains the estimated number of rows in the query.
    pub cardinality: f64,
    // columns contains an approximation of the contents of each column.
    pub columns: HashMap<Column, Option<ColumnStatistics>>,
}

pub(crate) fn compute_logical_props(
    mid: MultiExprID,
    statistics: &dyn RemoteExecution,
    ss: &SearchSpace,
) -> LogicalProps {
    let mexpr = &ss[mid];
    match &mexpr.expr {
        LogicalSingleGet => LogicalProps {
            cardinality: 1.0,
            columns: HashMap::new(),
        },
        LogicalGet {
            predicates,
            projects,
            table,
        } => filter(predicates, &scan(projects, table, statistics)),
        LogicalFilter { predicates, input } => filter(predicates, &ss[leaf(input)].props),
        LogicalOut { projects, input } => project(projects, &ss[leaf(input)].props),
        LogicalMap {
            projects,
            include_existing,
            input,
        } => map(projects, *include_existing, &ss[leaf(input)].props),
        LogicalJoin { join, left, right } => filter(
            join.predicates(),
            &cross_product(join, &ss[leaf(left)].props, &ss[leaf(right)].props),
        ),
        LogicalDependentJoin { .. } | LogicalWith { .. } => panic!(
            "{} should have been eliminated during rewrite phase",
            mexpr.expr.name()
        ),
        LogicalGetWith { columns, .. } => {
            // TODO get the props of the query so we can use a real estimate.
            LogicalProps {
                cardinality: 1.0,
                columns: columns.iter().map(|c| (c.clone(), None)).collect(),
            }
        }
        LogicalAggregate {
            group_by,
            aggregate,
            input,
        } => group_by_and_aggregate(aggregate, group_by, &ss[leaf(input)].props),
        LogicalLimit {
            limit,
            offset,
            input,
        } => limit_offset(*limit, *offset, &ss[leaf(input)].props),
        LogicalSort { input, .. } => ss[leaf(input)].props.clone(),
        LogicalUnion { left, right } => union(&ss[leaf(left)].props, &ss[leaf(right)].props),
        LogicalScript { statements } => {
            let last = statements.last().unwrap();
            ss[leaf(last)].props.clone()
        }
        LogicalExplain { input, .. } => ss[leaf(input)].props.clone(),
        LogicalInsert { .. }
        | LogicalValues { .. }
        | LogicalUpdate { .. }
        | LogicalDelete { .. }
        | LogicalCreateDatabase { .. }
        | LogicalCreateTable { .. }
        | LogicalCreateTempTable { .. }
        | LogicalCreateIndex { .. }
        | LogicalDrop { .. }
        | LogicalAssign { .. }
        | LogicalCall { .. }
        | LogicalRewrite { .. } => LogicalProps {
            cardinality: 0.0,
            columns: HashMap::with_capacity(0),
        },
        Leaf { .. }
        | TableFreeScan { .. }
        | SeqScan { .. }
        | IndexScan { .. }
        | Filter { .. }
        | Out { .. }
        | Map { .. }
        | NestedLoop { .. }
        | HashJoin { .. }
        | CreateTempTable { .. }
        | GetTempTable { .. }
        | Aggregate { .. }
        | Limit { .. }
        | Sort { .. }
        | Union { .. }
        | Broadcast { .. }
        | Exchange { .. }
        | Insert { .. }
        | Values { .. }
        | Delete { .. }
        | Script { .. }
        | Assign { .. }
        | Call { .. }
        | Explain { .. } => panic!("{} is a physical operator", mexpr.expr.name()),
    }
}

fn scan(projects: &Vec<Column>, table: &Table, statistics: &dyn RemoteExecution) -> LogicalProps {
    let stats = |c: &Column| statistics.column_statistics(c.table_id?, &c.name);
    LogicalProps {
        cardinality: statistics.approx_cardinality(table.id),
        columns: projects.iter().map(|c| (c.clone(), stats(c))).collect(),
    }
}

fn filter(predicates: &Vec<Scalar>, input: &LogicalProps) -> LogicalProps {
    // Compute total selectivity.
    // If we can't figure out the selectivity of a predicate, assume it is 1.
    // Taking the product of all selectivities is aggressive because it assumes they are uncorrelated.
    // This estimate will be inaccurate when columns are correlated, for example
    //   SELECT * FROM cars WHERE make = 'Ford' AND model = 'Mustang'
    let selectivity: f64 = predicates
        .iter()
        .map(|scalar| selectivity(scalar, &input).unwrap_or(1.0))
        .product();
    // Uniformly reduce the cardinality of all columns.
    // We are ignoring the fact that predicates aren't actually uniform.
    // For example, in
    //   SELECT a, b FROM input WHERE a > 0
    // the negative part of the distribution of a is cut off,
    // and if b is correlated with a, the distribution of b may be reduced in non-uniform ways.
    // This matters when you apply multiple predicates to the same columns, for example
    //   SELECT left.a, right.b FROM left, right WHERE left.a > 0 AND left.a = right.b
    // In the future we may want to be more amibitious.
    let mut output = input.clone();
    output.cardinality *= selectivity;
    output
}

fn project(projects: &Vec<Column>, input: &LogicalProps) -> LogicalProps {
    LogicalProps {
        cardinality: input.cardinality,
        columns: input
            .columns
            .iter()
            .filter(|(c, _)| projects.contains(c))
            .map(|(c, s)| (c.clone(), s.clone()))
            .collect(),
    }
}

fn map(
    projects: &Vec<(Scalar, Column)>,
    include_existing: bool,
    input: &LogicalProps,
) -> LogicalProps {
    let mut columns = if include_existing {
        input.columns.clone()
    } else {
        HashMap::new()
    };
    for (_x, c) in projects {
        // TODO some simple scalars could be supported by adjusting the histogram.
        columns.insert(c.clone(), None);
    }
    LogicalProps {
        cardinality: input.cardinality,
        columns,
    }
}

fn cross_product(join: &Join, left: &LogicalProps, right: &LogicalProps) -> LogicalProps {
    let mut columns = HashMap::new();
    for (c, s) in &left.columns {
        columns.insert(c.clone(), s.clone());
    }
    for (c, s) in &right.columns {
        columns.insert(c.clone(), s.clone());
    }
    if let Join::Mark(mark, _) = join {
        columns.insert(mark.clone(), None);
    }
    LogicalProps {
        cardinality: (left.cardinality * right.cardinality)
            .max(left.cardinality)
            .max(right.cardinality),
        columns,
    }
}

fn group_by_and_aggregate(
    aggregate: &Vec<AggregateExpr>,
    group_by: &Vec<Column>,
    input: &LogicalProps,
) -> LogicalProps {
    let mut columns = HashMap::new();
    for c in group_by {
        columns.insert(c.clone(), input.columns[c].clone());
    }
    for AggregateExpr { output, .. } in aggregate {
        columns.insert(output.clone(), None);
    }
    LogicalProps {
        cardinality: group_by_cardinality(group_by, input).unwrap_or(input.cardinality),
        columns,
    }
}

fn group_by_cardinality(group_by: &Vec<Column>, input: &LogicalProps) -> Option<f64> {
    let mut cardinality = 1.0;
    for column in group_by {
        cardinality *= input.columns[column].as_ref()?.count_distinct();
    }
    Some(cardinality)
}

fn limit_offset(limit: usize, _offset: usize, input: &LogicalProps) -> LogicalProps {
    LogicalProps {
        cardinality: input.cardinality.min(limit as f64),
        columns: input.columns.clone(),
    }
}

fn union(left: &LogicalProps, right: &LogicalProps) -> LogicalProps {
    LogicalProps {
        cardinality: left.cardinality + right.cardinality,
        columns: left
            .columns
            .keys()
            .map(|c| (c.clone(), merge(&left.columns[c], &right.columns[c])))
            .collect(),
    }
}

fn merge(
    left: &Option<ColumnStatistics>,
    right: &Option<ColumnStatistics>,
) -> Option<ColumnStatistics> {
    Some(ColumnStatistics::union(left.as_ref()?, right.as_ref()?))
}

fn selectivity(scalar: &Scalar, input: &LogicalProps) -> Option<f64> {
    match scalar {
        Scalar::Literal(value) => match value {
            Value::Bool(None)
            | Value::Bool(Some(false))
            | Value::I64(None)
            | Value::I64(Some(0))
            | Value::String(None) => Some(0.0),
            Value::String(Some(string)) if string == "false" => Some(0.0),
            Value::Bool(Some(true)) | Value::I64(Some(_)) => Some(1.0),
            Value::String(Some(string)) if string == "true" => Some(1.0),
            Value::String(Some(_))
            | Value::F64(_)
            | Value::Date(_)
            | Value::Timestamp(_)
            | Value::EnumValue(_) => panic!("Bad bool value {}", value),
        },
        Scalar::Column(column) => match input.columns[column].as_ref()? {
            ColumnStatistics::Bool(statistics) => Some(statistics.probability_density(true)),
            ColumnStatistics::I64(_) => {
                // Any non-zero value is truthy.
                // Given an approximate histogram, even if 0 is in the histogram with a large count,
                // it's always possible that n-1 values are non-zero.
                None
            }
            ColumnStatistics::String(statistics) => {
                Some(statistics.probability_density("true".to_string()))
            }
            ColumnStatistics::F64(_)
            | ColumnStatistics::Date(_)
            | ColumnStatistics::Timestamp(_) => panic!("Bad bool value {}", column),
        },
        Scalar::Parameter(_, _) => {
            // We don't know anything about parameters.
            None
        }
        Scalar::Call(function) => match function.as_ref() {
            // WHERE column1 = column2
            F::Equal(Scalar::Column(column1), Scalar::Column(column2))
            | F::Is(Scalar::Column(column1), Scalar::Column(column2)) => {
                let statistics1 = input.columns[column1].as_ref()?;
                let statistics2 = input.columns[column2].as_ref()?;
                let count1 = statistics1.count_distinct();
                let count2 = statistics2.count_distinct();
                // This is a very naive estimate of correlation.
                // This works when we're filtering the results of a cross-join, for example
                //   SELECT * FROM left, right WHERE left.id = right.id
                // This will be very wrong when we compare two correlated columns, for example
                //   SELECT * FROM people WHERE name = name_at_birth
                let correlation = 1.0 / count1.min(count2).max(1.0);
                Some(correlation)
            }
            // WHERE column = 'literal'
            F::Equal(Scalar::Column(column), Scalar::Literal(literal))
            | F::Equal(Scalar::Literal(literal), Scalar::Column(column))
            | F::Is(Scalar::Column(column), Scalar::Literal(literal))
            | F::Is(Scalar::Literal(literal), Scalar::Column(column)) => {
                let statistics = input.columns[column].as_ref()?;
                Some(selectivity_equals(statistics, literal))
            }
            // WHERE column in ('literal1', 'literal2')
            F::In(Scalar::Column(column), right) => {
                let statistics = input.columns[column].as_ref()?;
                let mut not_selectivity = 1.0;
                for value in right {
                    if let Scalar::Literal(literal) = value {
                        not_selectivity *= 1.0 - selectivity_equals(statistics, literal);
                    } else {
                        return None;
                    }
                }
                Some(1.0 - not_selectivity)
            }
            // WHERE column < 'literal'
            F::Less(Scalar::Column(column), Scalar::Literal(literal))
            | F::LessOrEqual(Scalar::Column(column), Scalar::Literal(literal))
            | F::Greater(Scalar::Literal(literal), Scalar::Column(column))
            | F::GreaterOrEqual(Scalar::Literal(literal), Scalar::Column(column)) => {
                let statistics = input.columns[column].as_ref()?;
                Some(selectivity_less(statistics, literal))
            }
            // WHERE column > 'literal'
            F::Greater(Scalar::Column(column), Scalar::Literal(literal))
            | F::GreaterOrEqual(Scalar::Column(column), Scalar::Literal(literal))
            | F::Less(Scalar::Literal(literal), Scalar::Column(column))
            | F::LessOrEqual(Scalar::Literal(literal), Scalar::Column(column)) => {
                let statistics = input.columns[column].as_ref()?;
                Some(1.0 - selectivity_less(statistics, literal))
            }
            // WHERE _ AND _
            F::And(left, right) => {
                let left_selectivity = selectivity(left, input)?;
                let right_selectivity = selectivity(right, input)?;
                Some(left_selectivity * right_selectivity)
            }
            // WHERE _ OR _
            F::Or(left, right) => {
                let left_selectivity = selectivity(left, input)?;
                let right_selectivity = selectivity(right, input)?;
                Some(1.0 - (1.0 - left_selectivity) * (1.0 - right_selectivity))
            }
            _ => {
                // Give up.
                None
            }
        },
        Scalar::Cast(scalar, _) => selectivity(scalar, input),
    }
}

fn selectivity_equals(column: &ColumnStatistics, literal: &Value) -> f64 {
    match (column, literal) {
        (ColumnStatistics::Bool(typed), Value::Bool(Some(value))) => {
            typed.probability_density(*value)
        }
        (ColumnStatistics::I64(typed), Value::I64(Some(value))) => {
            typed.probability_density(*value)
        }
        (ColumnStatistics::F64(typed), Value::F64(Some(value))) => {
            if value.is_nan() {
                0.0
            } else {
                typed.probability_density(NotNan(*value))
            }
        }
        (ColumnStatistics::Date(typed), Value::Date(Some(value))) => {
            typed.probability_density(*value)
        }
        (ColumnStatistics::Timestamp(typed), Value::Timestamp(Some(value))) => {
            typed.probability_density(*value)
        }
        (ColumnStatistics::String(typed), Value::String(Some(value))) => {
            typed.probability_density(value.clone())
        }
        (ColumnStatistics::Bool(_), Value::Bool(None)) => 0.0,
        (ColumnStatistics::I64(_), Value::I64(None)) => 0.0,
        (ColumnStatistics::F64(_), Value::F64(None)) => 0.0,
        (ColumnStatistics::Date(_), Value::Date(None)) => 0.0,
        (ColumnStatistics::Timestamp(_), Value::Timestamp(None)) => 0.0,
        (ColumnStatistics::String(_), Value::String(None)) => 0.0,
        (_, _) => panic!(
            "{} does not match {}",
            column.data_type(),
            literal.data_type()
        ),
    }
}

fn selectivity_less(column: &ColumnStatistics, literal: &Value) -> f64 {
    match (column, literal) {
        (ColumnStatistics::Bool(typed), Value::Bool(Some(value))) => {
            typed.cumulative_probability(*value)
        }
        (ColumnStatistics::I64(typed), Value::I64(Some(value))) => {
            typed.cumulative_probability(*value)
        }
        (ColumnStatistics::F64(typed), Value::F64(Some(value))) => {
            if value.is_nan() {
                0.0
            } else {
                typed.cumulative_probability(NotNan(*value))
            }
        }
        (ColumnStatistics::Date(typed), Value::Date(Some(value))) => {
            typed.cumulative_probability(*value)
        }
        (ColumnStatistics::Timestamp(typed), Value::Timestamp(Some(value))) => {
            typed.cumulative_probability(*value)
        }
        (ColumnStatistics::String(typed), Value::String(Some(value))) => {
            typed.cumulative_probability(value.clone())
        }
        (ColumnStatistics::Bool(_), Value::Bool(None)) => 0.0,
        (ColumnStatistics::I64(_), Value::I64(None)) => 0.0,
        (ColumnStatistics::F64(_), Value::F64(None)) => 0.0,
        (ColumnStatistics::Date(_), Value::Date(None)) => 0.0,
        (ColumnStatistics::Timestamp(_), Value::Timestamp(None)) => 0.0,
        (ColumnStatistics::String(_), Value::String(None)) => 0.0,
        (_, _) => panic!(
            "{} does not match {}",
            column.data_type(),
            literal.data_type()
        ),
    }
}

fn apply_false(input: &mut LogicalProps) {
    input.cardinality = 0.0;
    for (_, statistics) in &mut input.columns {
        *statistics = None;
    }
}

impl Debug for LogicalProps {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let columns: Vec<String> = self.columns.keys().map(|c| c.name.clone()).collect();
        write!(
            f,
            "LogicalProps #{} [{}]",
            self.cardinality,
            columns.join(", ")
        )
    }
}
