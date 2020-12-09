use arrow::array::*;
use arrow::datatypes::*;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use ast::{AggregateFn, Column, Distinct};
use std::sync::Arc;

pub struct SimpleAggregate {
    aggregate: AggregateFn,
    state: Arc<dyn Array>,
}

impl SimpleAggregate {
    pub fn begin(aggregate: &AggregateFn) -> Self {
        let data_type = match aggregate {
            AggregateFn::Count(_, _) | AggregateFn::CountStar => &DataType::Int64,
            AggregateFn::LogicalAnd(_) | AggregateFn::LogicalOr(_) => &DataType::Boolean,
            AggregateFn::StringAgg(_, _) => &DataType::Utf8,
            AggregateFn::AnyValue(column)
            | AggregateFn::Avg(_, column)
            | AggregateFn::BitAnd(_, column)
            | AggregateFn::BitOr(_, column)
            | AggregateFn::BitXor(_, column)
            | AggregateFn::Max(column)
            | AggregateFn::Min(column)
            | AggregateFn::Sum(_, column) => &column.data,
        };
        Self {
            aggregate: aggregate.clone(),
            state: kernel::nulls(1, data_type),
        }
    }

    pub fn update(&mut self, input: &RecordBatch) -> Result<(), ArrowError> {
        let update = match &self.aggregate {
            AggregateFn::AnyValue(column) => kernel::find(input, column).slice(0, 1),
            AggregateFn::Avg(Distinct(false), column) => {
                kernel::agg::avg(&kernel::find(input, column))
            }
            AggregateFn::Avg(Distinct(true), column) => todo!(),
            AggregateFn::BitAnd(Distinct(false), column) => {
                kernel::agg::bit_and(&kernel::find(input, column))
            }
            AggregateFn::BitAnd(Distinct(true), column) => todo!(),
            AggregateFn::BitOr(Distinct(false), column) => {
                kernel::agg::bit_or(&kernel::find(input, column))
            }
            AggregateFn::BitOr(Distinct(true), column) => todo!(),
            AggregateFn::BitXor(Distinct(false), column) => {
                kernel::agg::bit_xor(&kernel::find(input, column))
            }
            AggregateFn::BitXor(Distinct(true), column) => todo!(),
            AggregateFn::Count(Distinct(false), column) => {
                kernel::agg::count(&kernel::find(input, column))
            }
            AggregateFn::Count(Distinct(true), column) => todo!(),
            AggregateFn::CountStar => {
                let int64 = Int64Array::from(vec![input.num_rows() as i64]);
                Arc::new(int64)
            }
            AggregateFn::LogicalAnd(column) => {
                kernel::agg::logical_and(&kernel::find(input, column))
            }
            AggregateFn::LogicalOr(column) => kernel::agg::logical_or(&kernel::find(input, column)),
            AggregateFn::Max(column) => kernel::agg::max(&kernel::find(input, column)),
            AggregateFn::Min(column) => kernel::agg::min(&kernel::find(input, column)),
            AggregateFn::StringAgg(Distinct(false), column) => {
                kernel::agg::string_agg(&kernel::find(input, column))
            }
            AggregateFn::StringAgg(Distinct(true), column) => todo!(),
            AggregateFn::Sum(Distinct(false), column) => {
                kernel::agg::sum(&kernel::find(input, column))
            }
            AggregateFn::Sum(Distinct(true), column) => todo!(),
        };
        self.combine(&SimpleAggregate {
            aggregate: self.aggregate.clone(),
            state: update,
        })
    }

    pub fn combine(&mut self, other: &SimpleAggregate) -> Result<(), ArrowError> {
        self.state = match self.aggregate {
            AggregateFn::AnyValue(_) => todo!(),
            AggregateFn::Avg(Distinct(false), _) => todo!(),
            AggregateFn::Avg(Distinct(true), _) => todo!(),
            AggregateFn::BitAnd(Distinct(false), _) => todo!(),
            AggregateFn::BitAnd(Distinct(true), _) => todo!(),
            AggregateFn::BitOr(Distinct(false), _) => todo!(),
            AggregateFn::BitOr(Distinct(true), _) => todo!(),
            AggregateFn::BitXor(Distinct(false), _) => todo!(),
            AggregateFn::BitXor(Distinct(true), _) => todo!(),
            AggregateFn::Count(Distinct(false), _) => todo!(),
            AggregateFn::Count(Distinct(true), _) => todo!(),
            AggregateFn::CountStar => todo!(),
            AggregateFn::LogicalAnd(_) => todo!(),
            AggregateFn::LogicalOr(_) => todo!(),
            AggregateFn::Max(_) => todo!(),
            AggregateFn::Min(_) => todo!(),
            AggregateFn::StringAgg(Distinct(false), _) => todo!(),
            AggregateFn::StringAgg(Distinct(true), _) => todo!(),
            AggregateFn::Sum(Distinct(false), _) => match self.state.data_type() {
                DataType::Int64 => {
                    let left = as_primitive_array::<Int64Type>(&self.state);
                    let right = as_primitive_array::<Int64Type>(&other.state);
                    if left.is_null(0) {
                        other.state.clone()
                    } else if right.is_null(0) {
                        self.state.clone()
                    } else {
                        Arc::new(Int64Array::from(vec![left.value(0) + right.value(0)]))
                    }
                }
                DataType::Float64 => {
                    let left = as_primitive_array::<Float64Type>(&self.state);
                    let right = as_primitive_array::<Float64Type>(&other.state);
                    if left.is_null(0) {
                        other.state.clone()
                    } else if right.is_null(0) {
                        self.state.clone()
                    } else {
                        Arc::new(Float64Array::from(vec![left.value(0) + right.value(0)]))
                    }
                }
                other => panic!("+({:?}) is not supported", other),
            },
            AggregateFn::Sum(Distinct(true), _) => todo!(),
        };
        Ok(())
    }

    pub fn finish(self) -> Arc<dyn Array> {
        self.state
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
