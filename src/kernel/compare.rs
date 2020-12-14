use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;

pub trait CompareProvider<Right> {
    fn equal(left: &Self, right: Right) -> BooleanArray;
    fn less(left: &Self, right: Right) -> BooleanArray;
    fn less_equal(left: &Self, right: Right) -> BooleanArray;
}

pub fn equal<Right, P: CompareProvider<Right>>(left: &P, right: Right) -> BooleanArray {
    P::equal(left, right)
}

pub fn less<Right, P: CompareProvider<Right>>(left: &P, right: Right) -> BooleanArray {
    P::less(left, right)
}

pub fn less_equal<Right, P: CompareProvider<Right>>(left: &P, right: Right) -> BooleanArray {
    P::less_equal(left, right)
}

macro_rules! primitive {
    ($T:ty, $t:ty) => {
        impl CompareProvider<&$T> for $T {
            fn equal(left: &Self, right: &$T) -> BooleanArray {
                arrow::compute::eq(left, right).unwrap()
            }

            fn less(left: &Self, right: &$T) -> BooleanArray {
                arrow::compute::lt(left, right).unwrap()
            }

            fn less_equal(left: &Self, right: &$T) -> BooleanArray {
                arrow::compute::lt_eq(left, right).unwrap()
            }
        }

        impl CompareProvider<$t> for $T {
            fn equal(left: &Self, right: $t) -> BooleanArray {
                arrow::compute::eq_scalar(left, right).unwrap()
            }

            fn less(left: &Self, right: $t) -> BooleanArray {
                arrow::compute::lt_scalar(left, right).unwrap()
            }

            fn less_equal(left: &Self, right: $t) -> BooleanArray {
                arrow::compute::lt_eq_scalar(left, right).unwrap()
            }
        }
    };
}

primitive!(Int64Array, i64);
primitive!(Float64Array, f64);
primitive!(Date32Array, i32);
primitive!(TimestampMicrosecondArray, i64);

impl CompareProvider<&StringArray> for StringArray {
    fn equal(left: &Self, right: &StringArray) -> BooleanArray {
        arrow::compute::eq_utf8(left, right).unwrap()
    }

    fn less(left: &Self, right: &StringArray) -> BooleanArray {
        arrow::compute::lt_utf8(left, right).unwrap()
    }

    fn less_equal(left: &Self, right: &StringArray) -> BooleanArray {
        arrow::compute::lt_eq_utf8(left, right).unwrap()
    }
}

impl CompareProvider<&str> for StringArray {
    fn equal(left: &Self, right: &str) -> BooleanArray {
        arrow::compute::eq_utf8_scalar(left, right).unwrap()
    }

    fn less(left: &Self, right: &str) -> BooleanArray {
        arrow::compute::lt_utf8_scalar(left, right).unwrap()
    }

    fn less_equal(left: &Self, right: &str) -> BooleanArray {
        arrow::compute::lt_eq_utf8_scalar(left, right).unwrap()
    }
}

macro_rules! binary {
    ($left:expr, $right:expr, $op:ident) => {
        match $left.data_type() {
            // TODO should boolean be supported?
            DataType::Int64 => $op(
                as_primitive_array::<Int64Type>($left),
                as_primitive_array::<Int64Type>($right),
            ),
            DataType::Float64 => $op(
                as_primitive_array::<Float64Type>($left),
                as_primitive_array::<Float64Type>($right),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, None) => $op(
                as_primitive_array::<TimestampMicrosecondType>($left),
                as_primitive_array::<TimestampMicrosecondType>($right),
            ),
            DataType::Date32(DateUnit::Day) => $op(
                as_primitive_array::<Date32Type>($left),
                as_primitive_array::<Date32Type>($right),
            ),
            DataType::Utf8 => $op(as_string_array($left), as_string_array($right)),
            other => panic!("=({:?}) is not supported", other),
        }
    };
}

impl CompareProvider<&Arc<dyn Array>> for Arc<dyn Array> {
    fn equal(left: &Self, right: &Arc<dyn Array>) -> BooleanArray {
        binary!(left, right, equal)
    }

    fn less(left: &Self, right: &Arc<dyn Array>) -> BooleanArray {
        binary!(left, right, less)
    }

    fn less_equal(left: &Self, right: &Arc<dyn Array>) -> BooleanArray {
        binary!(left, right, less_equal)
    }
}
