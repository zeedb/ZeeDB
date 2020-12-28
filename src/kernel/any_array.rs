use crate::bool_array::*;
use crate::data_type::*;
use crate::primitive_array::*;
use crate::string_array::*;
use std::cmp::Ordering;

#[derive(Clone)]
pub enum Array {
    Bool(BoolArray),
    I64(I64Array),
    F64(F64Array),
    Date(DateArray),
    Timestamp(TimestampArray),
    String(StringArray),
}

impl Array {
    pub fn new(data_type: DataType) -> Self {
        match data_type {
            DataType::Bool => Array::Bool(BoolArray::new()),
            DataType::I64 => Array::I64(I64Array::new()),
            DataType::F64 => Array::F64(F64Array::new()),
            DataType::Date => Array::Date(DateArray::new()),
            DataType::Timestamp => Array::Timestamp(TimestampArray::new()),
            DataType::String => Array::String(StringArray::new()),
        }
    }

    pub fn with_capacity(data_type: DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Bool => Array::Bool(BoolArray::with_capacity(capacity)),
            DataType::I64 => Array::I64(I64Array::with_capacity(capacity)),
            DataType::F64 => Array::F64(F64Array::with_capacity(capacity)),
            DataType::Date => Array::Date(DateArray::with_capacity(capacity)),
            DataType::Timestamp => Array::Timestamp(TimestampArray::with_capacity(capacity)),
            DataType::String => Array::String(StringArray::with_capacity(capacity)),
        }
    }

    pub fn nulls(data_type: DataType, len: usize) -> Self {
        match data_type {
            DataType::Bool => Array::Bool(BoolArray::nulls(len)),
            DataType::I64 => Array::I64(I64Array::nulls(len)),
            DataType::F64 => Array::F64(F64Array::nulls(len)),
            DataType::Date => Array::Date(DateArray::nulls(len)),
            DataType::Timestamp => Array::Timestamp(TimestampArray::nulls(len)),
            DataType::String => Array::String(StringArray::nulls(len)),
        }
    }

    pub fn repeat(&self, n: usize) -> Self {
        todo!()
    }

    pub fn extend(&mut self, other: &Self) -> Self {
        todo!()
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Array::Bool(_) => DataType::Bool,
            Array::I64(_) => DataType::I64,
            Array::F64(_) => DataType::F64,
            Array::Date(_) => DataType::Date,
            Array::Timestamp(_) => DataType::Timestamp,
            Array::String(_) => DataType::String,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Array::Bool(array) => array.len(),
            Array::I64(array) => array.len(),
            Array::F64(array) => array.len(),
            Array::Date(array) => array.len(),
            Array::Timestamp(array) => array.len(),
            Array::String(array) => array.len(),
        }
    }

    pub fn gather(&self, indexes: &I32Array) -> Self {
        match self {
            Array::Bool(array) => Array::Bool(array.gather(indexes)),
            Array::I64(array) => Array::I64(array.gather(indexes)),
            Array::F64(array) => Array::F64(array.gather(indexes)),
            Array::Date(array) => Array::Date(array.gather(indexes)),
            Array::Timestamp(array) => Array::Timestamp(array.gather(indexes)),
            Array::String(array) => Array::String(array.gather(indexes)),
        }
    }

    pub fn compress(&self, mask: &BoolArray) -> Self {
        match self {
            Array::Bool(array) => Array::Bool(array.compress(mask)),
            Array::I64(array) => Array::I64(array.compress(mask)),
            Array::F64(array) => Array::F64(array.compress(mask)),
            Array::Date(array) => Array::Date(array.compress(mask)),
            Array::Timestamp(array) => Array::Timestamp(array.compress(mask)),
            Array::String(array) => Array::String(array.compress(mask)),
        }
    }

    pub fn scatter(&self, indexes: &I32Array, into: &mut Self) {
        match (self, into) {
            (Array::Bool(from), Array::Bool(into)) => from.scatter(indexes, into),
            (Array::I64(from), Array::I64(into)) => from.scatter(indexes, into),
            (Array::F64(from), Array::F64(into)) => from.scatter(indexes, into),
            (Array::Date(from), Array::Date(into)) => from.scatter(indexes, into),
            (Array::Timestamp(from), Array::Timestamp(into)) => from.scatter(indexes, into),
            (Array::String(from), Array::String(into)) => from.scatter(indexes, into),
            (left, right) => panic!("{} does not match {}", left.data_type(), right.data_type()),
        }
    }

    pub fn transpose(&self, stride: usize) -> Self {
        todo!()
    }

    pub fn cmp(&self, i: usize, j: usize) -> Ordering {
        match self {
            Array::Bool(array) => array.cmp(i, j),
            Array::I64(array) => array.cmp(i, j),
            Array::F64(array) => array.cmp(i, j),
            Array::Date(array) => array.cmp(i, j),
            Array::Timestamp(array) => array.cmp(i, j),
            Array::String(array) => array.cmp(i, j),
        }
    }

    pub fn sort(&self) -> I32Array {
        match self {
            Array::Bool(array) => array.sort(),
            Array::I64(array) => array.sort(),
            Array::F64(array) => array.sort(),
            Array::Date(array) => array.sort(),
            Array::Timestamp(array) => array.sort(),
            Array::String(array) => array.sort(),
        }
    }

    pub fn is(&self, other: &Self) -> BoolArray {
        todo!()
    }

    pub fn equal(&self, other: &Self) -> BoolArray {
        todo!()
    }

    pub fn not_equal(&self, other: &Self) -> BoolArray {
        todo!()
    }

    pub fn less(&self, other: &Self) -> BoolArray {
        todo!()
    }

    pub fn less_equal(&self, other: &Self) -> BoolArray {
        todo!()
    }

    pub fn greater(&self, other: &Self) -> BoolArray {
        todo!()
    }

    pub fn greater_equal(&self, other: &Self) -> BoolArray {
        todo!()
    }

    pub fn is_null(&self) -> BoolArray {
        todo!()
    }

    pub fn hash(&self, state: &mut U64Array) {
        match self {
            Array::Bool(array) => array.hash(state),
            Array::I64(array) => array.hash(state),
            Array::F64(array) => array.hash(state),
            Array::Date(array) => array.hash(state),
            Array::Timestamp(array) => array.hash(state),
            Array::String(array) => array.hash(state),
        }
    }

    pub fn as_bool(&self) -> Option<&BoolArray> {
        match self {
            Array::Bool(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<&I64Array> {
        match self {
            Array::I64(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<&F64Array> {
        match self {
            Array::F64(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_date(&self) -> Option<&DateArray> {
        match self {
            Array::Date(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_timestamp(&self) -> Option<&TimestampArray> {
        match self {
            Array::Timestamp(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&StringArray> {
        match self {
            Array::String(array) => Some(array),
            _ => None,
        }
    }

    pub fn cast(&self, data_type: DataType) -> Self {
        todo!()
    }
}
