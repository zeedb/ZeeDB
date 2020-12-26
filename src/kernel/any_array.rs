use crate::data_type::*;
use crate::typed_array::*;

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

    pub fn select(&self, mask: &BoolArray, default: &Self) -> Self {
        match (self, default) {
            (Array::Bool(from), Array::Bool(default)) => Array::Bool(from.select(mask, default)),
            (Array::I64(from), Array::I64(default)) => Array::I64(from.select(mask, default)),
            (Array::F64(from), Array::F64(default)) => Array::F64(from.select(mask, default)),
            (Array::Date(from), Array::Date(default)) => Array::Date(from.select(mask, default)),
            (Array::Timestamp(from), Array::Timestamp(default)) => {
                Array::Timestamp(from.select(mask, default))
            }
            (Array::String(from), Array::String(default)) => {
                Array::String(from.select(mask, default))
            }
            (left, right) => panic!("{} does not match {}", left.data_type(), right.data_type()),
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
}
