use crate::{
    Array, BoolArray, DataType, DateArray, F64Array, I32Array, I64Array, StringArray,
    TimestampArray, U64Array,
};
use std::{cmp::Ordering, ops::Range};

#[derive(Debug, Clone)]
pub enum AnyArray {
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
            AnyArray::Bool($matched) => $result,
            AnyArray::I64($matched) => $result,
            AnyArray::F64($matched) => $result,
            AnyArray::Date($matched) => $result,
            AnyArray::Timestamp($matched) => $result,
            AnyArray::String($matched) => $result,
        }
    };
}

macro_rules! binary_method {
    ($self:ident, $other:ident, $left:ident, $right:ident, $result:expr) => {
        match ($self, $other) {
            (AnyArray::Bool($left), AnyArray::Bool($right)) => $result,
            (AnyArray::I64($left), AnyArray::I64($right)) => $result,
            (AnyArray::F64($left), AnyArray::F64($right)) => $result,
            (AnyArray::Date($left), AnyArray::Date($right)) => $result,
            (AnyArray::Timestamp($left), AnyArray::Timestamp($right)) => $result,
            (AnyArray::String($left), AnyArray::String($right)) => $result,
            (left, right) => panic!("{} does not match {}", left.data_type(), right.data_type()),
        }
    };
}

macro_rules! unary_operator {
    ($self:ident, $matched:ident, $result:expr) => {
        match $self {
            AnyArray::Bool($matched) => AnyArray::Bool($result),
            AnyArray::I64($matched) => AnyArray::I64($result),
            AnyArray::F64($matched) => AnyArray::F64($result),
            AnyArray::Date($matched) => AnyArray::Date($result),
            AnyArray::Timestamp($matched) => AnyArray::Timestamp($result),
            AnyArray::String($matched) => AnyArray::String($result),
        }
    };
}

macro_rules! binary_operator {
    ($self:ident, $other:ident, $left:ident, $right:ident, $result:expr) => {
        match ($self, $other) {
            (AnyArray::Bool($left), AnyArray::Bool($right)) => AnyArray::Bool($result),
            (AnyArray::I64($left), AnyArray::I64($right)) => AnyArray::I64($result),
            (AnyArray::F64($left), AnyArray::F64($right)) => AnyArray::F64($result),
            (AnyArray::Date($left), AnyArray::Date($right)) => AnyArray::Date($result),
            (AnyArray::Timestamp($left), AnyArray::Timestamp($right)) => {
                AnyArray::Timestamp($result)
            }
            (AnyArray::String($left), AnyArray::String($right)) => AnyArray::String($result),
            (left, right) => panic!("{} does not match {}", left.data_type(), right.data_type()),
        }
    };
}

impl AnyArray {
    // Constructors.
    pub fn new(data_type: DataType) -> Self {
        match data_type {
            DataType::Bool => AnyArray::Bool(BoolArray::new()),
            DataType::I64 => AnyArray::I64(I64Array::new()),
            DataType::F64 => AnyArray::F64(F64Array::new()),
            DataType::Date => AnyArray::Date(DateArray::new()),
            DataType::Timestamp => AnyArray::Timestamp(TimestampArray::new()),
            DataType::String => AnyArray::String(StringArray::new()),
        }
    }

    pub fn with_capacity(data_type: DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Bool => AnyArray::Bool(BoolArray::with_capacity(capacity)),
            DataType::I64 => AnyArray::I64(I64Array::with_capacity(capacity)),
            DataType::F64 => AnyArray::F64(F64Array::with_capacity(capacity)),
            DataType::Date => AnyArray::Date(DateArray::with_capacity(capacity)),
            DataType::Timestamp => AnyArray::Timestamp(TimestampArray::with_capacity(capacity)),
            DataType::String => AnyArray::String(StringArray::with_capacity(capacity)),
        }
    }

    pub fn nulls(data_type: DataType, len: usize) -> Self {
        match data_type {
            DataType::Bool => AnyArray::Bool(BoolArray::nulls(len)),
            DataType::I64 => AnyArray::I64(I64Array::nulls(len)),
            DataType::F64 => AnyArray::F64(F64Array::nulls(len)),
            DataType::Date => AnyArray::Date(DateArray::nulls(len)),
            DataType::Timestamp => AnyArray::Timestamp(TimestampArray::nulls(len)),
            DataType::String => AnyArray::String(StringArray::nulls(len)),
        }
    }

    pub fn cat(mut arrays: Vec<Self>) -> Self {
        match arrays[0].data_type() {
            DataType::Bool => AnyArray::Bool(BoolArray::cat(
                &arrays.drain(..).map(|array| array.as_bool()).collect(),
            )),
            DataType::I64 => AnyArray::I64(I64Array::cat(
                &arrays.drain(..).map(|array| array.as_i64()).collect(),
            )),
            DataType::F64 => AnyArray::F64(F64Array::cat(
                &arrays.drain(..).map(|array| array.as_f64()).collect(),
            )),
            DataType::Date => AnyArray::Date(DateArray::cat(
                &arrays.drain(..).map(|array| array.as_date()).collect(),
            )),
            DataType::Timestamp => AnyArray::Timestamp(TimestampArray::cat(
                &arrays.drain(..).map(|array| array.as_timestamp()).collect(),
            )),
            DataType::String => AnyArray::String(StringArray::cat(
                &arrays.drain(..).map(|array| array.as_string()).collect(),
            )),
        }
    }

    // Basic container operations.

    pub fn len(&self) -> usize {
        unary_method!(self, array, array.len())
    }

    pub fn data_type(&self) -> DataType {
        match self {
            AnyArray::Bool(_) => DataType::Bool,
            AnyArray::I64(_) => DataType::I64,
            AnyArray::F64(_) => DataType::F64,
            AnyArray::Date(_) => DataType::Date,
            AnyArray::Timestamp(_) => DataType::Timestamp,
            AnyArray::String(_) => DataType::String,
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

    // Vector operations.

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

    pub fn null_if(&self, other: &Self) -> Self {
        binary_operator!(self, other, left, right, left.null_if(right))
    }

    pub fn equal_any(&self, others: Vec<Self>) -> BoolArray {
        let mut acc = self.equal(&others[0]);
        for other in &others[1..] {
            acc = acc.or(&self.equal(other));
        }
        acc
    }

    pub fn greatest(mut arrays: Vec<Self>) -> Self {
        let mut acc = arrays.remove(0);
        for other in arrays {
            acc = acc.greater(&other).blend_or_null(&acc, &other);
        }
        acc
    }

    pub fn least(mut arrays: Vec<Self>) -> Self {
        let mut acc = arrays.remove(0);
        for other in arrays {
            acc = acc.less(&other).blend_or_null(&acc, &other);
        }
        acc
    }

    // Support operations for data structures.

    pub fn cmp(&self, i: usize, j: usize) -> Ordering {
        unary_method!(self, array, array.cmp(i, j))
    }

    pub fn hash(&self, state: &mut U64Array) {
        unary_method!(self, array, array.hash(state))
    }

    // Type coercion.

    pub fn as_bool(self) -> BoolArray {
        match self {
            AnyArray::Bool(array) => array,
            other => panic!("expected BOOL but found {}", other.data_type()),
        }
    }

    pub fn as_i64(self) -> I64Array {
        match self {
            AnyArray::I64(array) => array,
            other => panic!("expected I64 but found {}", other.data_type()),
        }
    }

    pub fn as_f64(self) -> F64Array {
        match self {
            AnyArray::F64(array) => array,
            other => panic!("expected F64 but found {}", other.data_type()),
        }
    }

    pub fn as_date(self) -> DateArray {
        match self {
            AnyArray::Date(array) => array,
            other => panic!("expected DATE but found {}", other.data_type()),
        }
    }

    pub fn as_timestamp(self) -> TimestampArray {
        match self {
            AnyArray::Timestamp(array) => array,
            other => panic!("expected TIMESTAMP but found {}", other.data_type()),
        }
    }

    pub fn as_string(self) -> StringArray {
        match self {
            AnyArray::String(array) => array,
            other => panic!("expected STRING but found {}", other.data_type()),
        }
    }

    pub fn cast(&self, data_type: DataType) -> Self {
        match (self, data_type) {
            (AnyArray::Bool(_), DataType::Bool) => self.clone(),
            (AnyArray::Bool(array), DataType::I64) => AnyArray::I64(array.cast_i64()),
            (AnyArray::Bool(array), DataType::F64) => AnyArray::F64(array.cast_f64()),
            (AnyArray::Bool(array), DataType::String) => AnyArray::String(array.cast_string()),
            (AnyArray::I64(array), DataType::Bool) => AnyArray::Bool(array.cast_bool()),
            (AnyArray::I64(_), DataType::I64) => self.clone(),
            (AnyArray::I64(array), DataType::F64) => AnyArray::F64(array.cast_f64()),
            (AnyArray::I64(array), DataType::String) => AnyArray::String(array.cast_string()),
            (AnyArray::F64(array), DataType::I64) => AnyArray::I64(array.cast_i64()),
            (AnyArray::F64(_), DataType::F64) => self.clone(),
            (AnyArray::F64(array), DataType::String) => AnyArray::String(array.cast_string()),
            (AnyArray::Date(_), DataType::Date) => self.clone(),
            (AnyArray::Date(array), DataType::Timestamp) => {
                AnyArray::Timestamp(array.cast_timestamp())
            }
            (AnyArray::Date(array), DataType::String) => AnyArray::String(array.cast_string()),
            (AnyArray::Timestamp(array), DataType::Date) => AnyArray::Date(array.cast_date()),
            (AnyArray::Timestamp(_), DataType::Timestamp) => self.clone(),
            (AnyArray::Timestamp(array), DataType::String) => AnyArray::String(array.cast_string()),
            (AnyArray::String(array), DataType::Bool) => AnyArray::Bool(array.cast_bool()),
            (AnyArray::String(array), DataType::I64) => AnyArray::I64(array.cast_i64()),
            (AnyArray::String(array), DataType::F64) => AnyArray::F64(array.cast_f64()),
            (AnyArray::String(array), DataType::Date) => AnyArray::Date(array.cast_date()),
            (AnyArray::String(array), DataType::Timestamp) => {
                AnyArray::Timestamp(array.cast_timestamp())
            }
            (AnyArray::String(_), DataType::String) => self.clone(),
            (_, _) => panic!("cannot cast {} to {}", self.data_type(), data_type),
        }
    }
}
