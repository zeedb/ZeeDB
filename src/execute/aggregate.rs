use arrow::array::*;
use arrow::datatypes::*;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use ast::{AggregateFn, Column, Distinct, Value};
use std::any::{Any, TypeId};
use std::sync::Arc;

pub struct SimpleAggregate {
    aggregate: AggregateFn,
    state: State,
}

#[derive(Debug)]
enum State {
    AnyValue { value: Option<Value> },
    Avg { value: Option<f64>, count: usize },
    Count { count: i64 },
    CountStar { count: i64 },
    LogicalAnd { value: Option<bool> },
    LogicalOr { value: Option<bool> },
    Max { value: Option<Value> },
    Min { value: Option<Value> },
    Sum { value: Option<Value> },
}

impl State {
    fn empty(aggregate: &AggregateFn) -> Self {
        match aggregate {
            AggregateFn::AnyValue(_) => State::AnyValue { value: None },
            AggregateFn::Avg(_, _) => State::Avg {
                value: None,
                count: 0,
            },
            AggregateFn::Count(_, _) => State::Count { count: 0 },
            AggregateFn::CountStar => State::CountStar { count: 0 },
            AggregateFn::LogicalAnd(_) => State::LogicalAnd { value: None },
            AggregateFn::LogicalOr(_) => State::LogicalOr { value: None },
            AggregateFn::Max(_) => State::Max { value: None },
            AggregateFn::Min(_) => State::Min { value: None },
            AggregateFn::Sum(_, _) => State::Sum { value: None },
        }
    }

    fn aggregate(aggregate: &AggregateFn, input: &RecordBatch) -> Self {
        match aggregate {
            AggregateFn::AnyValue(column) => State::AnyValue {
                value: Value::from(&kernel::agg::any_value(
                    &kernel::find(input, column).slice(0, 1),
                )),
            },
            AggregateFn::Avg(Distinct(false), column) => {
                let any = &kernel::find(input, column);
                let array = arrow::compute::cast(any, &DataType::Float64).unwrap();
                State::Avg {
                    value: arrow::compute::sum(as_primitive_array::<Float64Type>(&array)),
                    count: kernel::agg::count(any),
                }
            }
            AggregateFn::Avg(Distinct(true), column) => todo!(),
            AggregateFn::Count(Distinct(false), column) => State::Count {
                count: kernel::agg::count(&kernel::find(input, column)) as i64,
            },
            AggregateFn::Count(Distinct(true), column) => todo!(),
            AggregateFn::CountStar => State::CountStar {
                count: input.num_rows() as i64,
            },
            AggregateFn::LogicalAnd(column) => State::LogicalAnd {
                value: kernel::agg::logical_and(&kernel::find(input, column)),
            },
            AggregateFn::LogicalOr(column) => State::LogicalOr {
                value: kernel::agg::logical_or(&kernel::find(input, column)),
            },
            AggregateFn::Max(column) => State::Max {
                value: Value::from(&kernel::agg::max(&kernel::find(input, column))),
            },
            AggregateFn::Min(column) => State::Min {
                value: Value::from(&kernel::agg::min(&kernel::find(input, column))),
            },
            AggregateFn::Sum(Distinct(false), column) => State::Sum {
                value: Value::from(&kernel::agg::sum(&kernel::find(input, column))),
            },
            AggregateFn::Sum(Distinct(true), column) => todo!(),
        }
    }

    fn finish(&self) -> Option<Value> {
        match self {
            State::AnyValue { value }
            | State::Max { value }
            | State::Min { value }
            | State::Sum { value } => value.clone(),
            State::LogicalAnd { value } | State::LogicalOr { value } => match value {
                Some(value) => Some(Value::Boolean(*value)),
                None => None,
            },
            State::Avg { value, count } => match value {
                Some(value) => Some(Value::Float64(*value / *count as f64)),
                None => None,
            },
            State::Count { count } | State::CountStar { count } => Some(Value::Int64(*count)),
        }
    }

    fn combine(&mut self, other: &State) {
        match (self, other) {
            (State::AnyValue { value: left }, State::AnyValue { value: right }) => {
                *left = right.clone()
            }
            (
                State::Avg {
                    value: left,
                    count: left_count,
                },
                State::Avg {
                    value: right,
                    count: right_count,
                },
            ) => {
                *left = add_float64(left, right);
                *left_count = *left_count + *right_count;
            }
            (State::Count { count: left_count }, State::Count { count: right_count })
            | (State::CountStar { count: left_count }, State::CountStar { count: right_count }) => {
                *left_count = *left_count + *right_count;
            }
            (State::LogicalAnd { value: left }, State::LogicalAnd { value: right }) => {
                *left = and(left, right);
            }
            (State::LogicalOr { value: left }, State::LogicalOr { value: right }) => {
                *left = or(left, right);
            }
            (State::Max { value: left }, State::Max { value: right }) => {
                *left = max(left, right);
            }
            (State::Min { value: left }, State::Min { value: right }) => {
                *left = min(left, right);
            }
            (State::Sum { value: left }, State::Sum { value: right }) => {
                *left = sum(left, right);
            }
            (left, right) => panic!("{:?} does not match {:?}", left, right),
        }
    }
}

impl SimpleAggregate {
    pub fn begin(aggregate: &AggregateFn) -> Self {
        Self {
            aggregate: aggregate.clone(),
            state: State::empty(aggregate),
        }
    }

    pub fn update(&mut self, input: &RecordBatch) -> Result<(), ArrowError> {
        let next = State::aggregate(&self.aggregate, input);
        self.state.combine(&next);
        Ok(())
    }

    pub fn combine(&mut self, other: SimpleAggregate) -> Result<(), ArrowError> {
        self.state.combine(&other.state);
        Ok(())
    }

    pub fn finish(self) -> Arc<dyn Array> {
        match self.state.finish() {
            Some(value) => value.array(),
            None => kernel::nulls(1, self.aggregate.data_type()),
        }
    }
}

fn and(left: &Option<bool>, right: &Option<bool>) -> Option<bool> {
    match (left, right) {
        (left, None) => *left,
        (None, right) => *right,
        (Some(left), Some(right)) => Some(*left && *right),
    }
}

fn or(left: &Option<bool>, right: &Option<bool>) -> Option<bool> {
    match (left, right) {
        (left, None) => *left,
        (None, right) => *right,
        (Some(left), Some(right)) => Some(*left || *right),
    }
}

fn max(left: &Option<Value>, right: &Option<Value>) -> Option<Value> {
    match (left, right) {
        (left, None) => left.clone(),
        (None, right) => right.clone(),
        (Some(left), Some(right)) => {
            let value = match (left, right) {
                (Value::Boolean(left), Value::Boolean(right)) => Value::Boolean(*left.max(right)),
                (Value::Int64(left), Value::Int64(right)) => Value::Int64(*left.max(right)),
                (Value::Float64(left), Value::Float64(right)) => Value::Float64(left.max(*right)),
                (Value::Numeric(left), Value::Numeric(right)) => Value::Numeric(*left.max(right)),
                (Value::Timestamp(left), Value::Timestamp(right)) => {
                    Value::Timestamp(*left.max(right))
                }
                (Value::Date(left), Value::Date(right)) => Value::Date(*left.max(right)),
                (Value::Utf8(left), Value::Utf8(right)) => Value::Utf8(left.max(right).clone()),
                (left, right) => panic!("max({:?}, {:?}) is not supported", left, right),
            };
            Some(value)
        }
    }
}

fn min(left: &Option<Value>, right: &Option<Value>) -> Option<Value> {
    match (left, right) {
        (left, None) => left.clone(),
        (None, right) => right.clone(),
        (Some(left), Some(right)) => {
            let value = match (left, right) {
                (Value::Boolean(left), Value::Boolean(right)) => Value::Boolean(*left.min(right)),
                (Value::Int64(left), Value::Int64(right)) => Value::Int64(*left.min(right)),
                (Value::Float64(left), Value::Float64(right)) => Value::Float64(left.min(*right)),
                (Value::Numeric(left), Value::Numeric(right)) => Value::Numeric(*left.min(right)),
                (Value::Timestamp(left), Value::Timestamp(right)) => {
                    Value::Timestamp(*left.min(right))
                }
                (Value::Date(left), Value::Date(right)) => Value::Date(*left.min(right)),
                (Value::Utf8(left), Value::Utf8(right)) => Value::Utf8(left.min(right).clone()),
                (left, right) => panic!("min({:?}, {:?}) is not supported", left, right),
            };
            Some(value)
        }
    }
}

fn sum(left: &Option<Value>, right: &Option<Value>) -> Option<Value> {
    match (left, right) {
        (left, None) => left.clone(),
        (None, right) => right.clone(),
        (Some(left), Some(right)) => {
            let value = match (left, right) {
                (Value::Int64(left), Value::Int64(right)) => Value::Int64(left + right),
                (Value::Float64(left), Value::Float64(right)) => Value::Float64(left + right),
                (Value::Numeric(left), Value::Numeric(right)) => Value::Numeric(left + right),
                (left, right) => panic!("sum({:?}, {:?}) is not supported", left, right),
            };
            Some(value)
        }
    }
}

fn add_float64(left: &Option<f64>, right: &Option<f64>) -> Option<f64> {
    match (left, right) {
        (None, None) => None,
        (Some(left), None) => Some(*left),
        (None, Some(right)) => Some(*right),
        (Some(left), Some(right)) => Some(*left + *right),
    }
}

pub struct HashAggregate {}

impl HashAggregate {
    pub fn begin(group_by: &Vec<Column>, aggregate: &Vec<AggregateFn>) -> Self {
        todo!()
    }

    pub fn update(&mut self, input: &RecordBatch) {
        todo!()
    }

    pub fn combine(&mut self, other: &HashAggregate) {
        todo!()
    }

    pub fn finish(&self) -> RecordBatch {
        todo!()
    }
}
