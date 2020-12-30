use crate::bool_array::*;
use crate::data_type::*;
use crate::primitive_array::*;
use crate::string_array::*;
use std::{cmp::Ordering, ops::Range};

#[derive(Debug, Clone)]
pub enum Array {
    Bool(BoolArray),
    I64(I64Array),
    F64(F64Array),
    Date(DateArray),
    Timestamp(TimestampArray),
    String(StringArray),
}

// trait ArrayVisitor {
//     fn visit_bool(value: BoolArray) -> BoolArray;
//     fn visit_i64(value: I64Array) -> I64Array;
//     fn visit_f64(value: F64Array) -> F64Array;
//     fn visit_date(value: DateArray) -> DateArray;
//     fn visit_timestamp(value: TimestampArray) -> TimestampArray;
//     fn visit_string(value: StringArray) -> StringArray;
// }

// trait DataTypeVisitor {
//     fn visit_bool() -> BoolArray;
//     fn visit_i64() -> I64Array;
//     fn visit_f64() -> F64Array;
//     fn visit_date() -> DateArray;
//     fn visit_timestamp() -> TimestampArray;
//     fn visit_string() -> StringArray;
// }

macro_rules! unary_method {
    ($self:ident, $matched:ident, $result:expr) => {
        match $self {
            Array::Bool($matched) => $result,
            Array::I64($matched) => $result,
            Array::F64($matched) => $result,
            Array::Date($matched) => $result,
            Array::Timestamp($matched) => $result,
            Array::String($matched) => $result,
        }
    };
}

macro_rules! binary_method {
    ($self:ident, $other:ident, $left:ident, $right:ident, $result:expr) => {
        match ($self, $other) {
            (Array::Bool($left), Array::Bool($right)) => $result,
            (Array::I64($left), Array::I64($right)) => $result,
            (Array::F64($left), Array::F64($right)) => $result,
            (Array::Date($left), Array::Date($right)) => $result,
            (Array::Timestamp($left), Array::Timestamp($right)) => $result,
            (Array::String($left), Array::String($right)) => $result,
            (left, right) => panic!("{} does not match {}", left.data_type(), right.data_type()),
        }
    };
}

macro_rules! unary_operator {
    ($self:ident, $matched:ident, $result:expr) => {
        match $self {
            Array::Bool($matched) => Array::Bool($result),
            Array::I64($matched) => Array::I64($result),
            Array::F64($matched) => Array::F64($result),
            Array::Date($matched) => Array::Date($result),
            Array::Timestamp($matched) => Array::Timestamp($result),
            Array::String($matched) => Array::String($result),
        }
    };
}

macro_rules! binary_operator {
    ($self:ident, $other:ident, $left:ident, $right:ident, $result:expr) => {
        match ($self, $other) {
            (Array::Bool($left), Array::Bool($right)) => Array::Bool($result),
            (Array::I64($left), Array::I64($right)) => Array::I64($result),
            (Array::F64($left), Array::F64($right)) => Array::F64($result),
            (Array::Date($left), Array::Date($right)) => Array::Date($result),
            (Array::Timestamp($left), Array::Timestamp($right)) => Array::Timestamp($result),
            (Array::String($left), Array::String($right)) => Array::String($result),
            (left, right) => panic!("{} does not match {}", left.data_type(), right.data_type()),
        }
    };
}

impl Array {
    // Constructors.
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

    pub fn cat(mut arrays: Vec<Self>) -> Self {
        match arrays[0].data_type() {
            DataType::Bool => Array::Bool(BoolArray::cat(
                arrays
                    .drain(..)
                    .map(|array| array.as_bool().unwrap())
                    .collect(),
            )),
            DataType::I64 => Array::I64(I64Array::cat(
                arrays
                    .drain(..)
                    .map(|array| array.as_i64().unwrap())
                    .collect(),
            )),
            DataType::F64 => Array::F64(F64Array::cat(
                arrays
                    .drain(..)
                    .map(|array| array.as_f64().unwrap())
                    .collect(),
            )),
            DataType::Date => Array::Date(DateArray::cat(
                arrays
                    .drain(..)
                    .map(|array| array.as_date().unwrap())
                    .collect(),
            )),
            DataType::Timestamp => Array::Timestamp(TimestampArray::cat(
                arrays
                    .drain(..)
                    .map(|array| array.as_timestamp().unwrap())
                    .collect(),
            )),
            DataType::String => Array::String(StringArray::cat(
                arrays
                    .drain(..)
                    .map(|array| array.as_string().unwrap())
                    .collect(),
            )),
        }
    }

    // Basic container operations.

    pub fn len(&self) -> usize {
        unary_method!(self, array, array.len())
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

    pub fn slice(&self, range: Range<usize>) -> Self {
        unary_operator!(self, array, array.slice(range))
    }

    pub fn extend(&mut self, other: &Self) {
        binary_method!(self, other, left, right, left.extend(right))
    }

    pub fn repeat(&self, n: usize) -> Self {
        unary_operator!(self, array, array.repeat(n))
    }

    // Complex vector operations.

    pub fn gather(&self, indexes: &I32Array) -> Self {
        unary_operator!(self, array, array.gather(indexes))
    }

    pub fn compress(&self, mask: &BoolArray) -> Self {
        unary_operator!(self, array, array.compress(mask))
    }

    pub fn scatter(&self, indexes: &I32Array, into: &mut Self) {
        binary_method!(self, into, left, right, left.scatter(indexes, right))
    }

    pub fn transpose(&self, stride: usize) -> Self {
        unary_operator!(self, array, array.transpose(stride))
    }

    pub fn sort(&self) -> I32Array {
        unary_method!(self, array, array.sort())
    }

    // Array comparison operators.

    pub fn is(&self, other: &Self) -> BoolArray {
        binary_method!(self, other, left, right, left.is(right))
    }

    pub fn equal(&self, other: &Self) -> BoolArray {
        binary_method!(self, other, left, right, left.equal(right))
    }

    pub fn not_equal(&self, other: &Self) -> BoolArray {
        binary_method!(self, other, left, right, left.not_equal(right))
    }

    pub fn less(&self, other: &Self) -> BoolArray {
        binary_method!(self, other, left, right, left.less(right))
    }

    pub fn less_equal(&self, other: &Self) -> BoolArray {
        binary_method!(self, other, left, right, left.less_equal(right))
    }

    pub fn greater(&self, other: &Self) -> BoolArray {
        binary_method!(self, other, left, right, left.greater(right))
    }

    pub fn greater_equal(&self, other: &Self) -> BoolArray {
        binary_method!(self, other, left, right, left.greater_equal(right))
    }

    pub fn is_null(&self) -> BoolArray {
        unary_method!(self, array, array.is_null())
    }

    pub fn coalesce(&self, other: &Self) -> Self {
        binary_operator!(self, other, left, right, left.coalesce(right))
    }

    // Support operations for data structures.

    pub fn cmp(&self, i: usize, j: usize) -> Ordering {
        unary_method!(self, array, array.cmp(i, j))
    }

    pub fn hash(&self, state: &mut U64Array) {
        unary_method!(self, array, array.hash(state))
    }

    // Type coercion.

    pub fn as_bool(self) -> Result<BoolArray, Array> {
        match self {
            Array::Bool(array) => Ok(array),
            other => Err(other),
        }
    }

    pub fn as_i64(self) -> Result<I64Array, Array> {
        match self {
            Array::I64(array) => Ok(array),
            other => Err(other),
        }
    }

    pub fn as_f64(self) -> Result<F64Array, Array> {
        match self {
            Array::F64(array) => Ok(array),
            other => Err(other),
        }
    }

    pub fn as_date(self) -> Result<DateArray, Array> {
        match self {
            Array::Date(array) => Ok(array),
            other => Err(other),
        }
    }

    pub fn as_timestamp(self) -> Result<TimestampArray, Array> {
        match self {
            Array::Timestamp(array) => Ok(array),
            other => Err(other),
        }
    }

    pub fn as_string(self) -> Result<StringArray, Array> {
        match self {
            Array::String(array) => Ok(array),
            other => Err(other),
        }
    }

    pub fn cast(&self, data_type: DataType) -> Self {
        match (self, data_type) {
            (Array::Bool(_), DataType::Bool) => self.clone(),
            (Array::Bool(array), DataType::I64) => Array::I64(array.cast_i64()),
            (Array::Bool(array), DataType::F64) => Array::F64(array.cast_f64()),
            (Array::Bool(array), DataType::String) => Array::String(array.cast_string()),
            (Array::I64(array), DataType::Bool) => Array::Bool(array.cast_bool()),
            (Array::I64(_), DataType::I64) => self.clone(),
            (Array::I64(array), DataType::F64) => Array::F64(array.cast_f64()),
            (Array::I64(array), DataType::String) => Array::String(array.cast_string()),
            (Array::F64(array), DataType::I64) => Array::I64(array.cast_i64()),
            (Array::F64(_), DataType::F64) => self.clone(),
            (Array::F64(array), DataType::String) => Array::String(array.cast_string()),
            (Array::Date(_), DataType::Date) => self.clone(),
            (Array::Date(array), DataType::Timestamp) => Array::Timestamp(array.cast_timestamp()),
            (Array::Date(array), DataType::String) => Array::String(array.cast_string()),
            (Array::Timestamp(array), DataType::Date) => Array::Date(array.cast_date()),
            (Array::Timestamp(_), DataType::Timestamp) => self.clone(),
            (Array::Timestamp(array), DataType::String) => Array::String(array.cast_string()),
            (Array::String(array), DataType::Bool) => Array::Bool(array.cast_bool()),
            (Array::String(array), DataType::I64) => Array::I64(array.cast_i64()),
            (Array::String(array), DataType::F64) => Array::F64(array.cast_f64()),
            (Array::String(array), DataType::Date) => Array::Date(array.cast_date()),
            (Array::String(array), DataType::Timestamp) => Array::Timestamp(array.cast_timestamp()),
            (Array::String(_), DataType::String) => self.clone(),
            (_, _) => panic!("cannot cast {} to {}", self.data_type(), data_type),
        }
    }
}
