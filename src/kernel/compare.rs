use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;

pub trait CompareProvider<Right> {
    fn is(left: &Self, right: Right) -> BooleanArray;
    fn equal(left: &Self, right: Right) -> BooleanArray;
    fn less(left: &Self, right: Right) -> BooleanArray;
    fn less_equal(left: &Self, right: Right) -> BooleanArray;
    fn greater(left: &Self, right: Right) -> BooleanArray;
    fn greater_equal(left: &Self, right: Right) -> BooleanArray;
}

pub fn is<Right, P: CompareProvider<Right>>(left: &P, right: Right) -> BooleanArray {
    P::is(left, right)
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

pub fn greater<Right, P: CompareProvider<Right>>(left: &P, right: Right) -> BooleanArray {
    P::greater(left, right)
}

pub fn greater_equal<Right, P: CompareProvider<Right>>(left: &P, right: Right) -> BooleanArray {
    P::greater_equal(left, right)
}

macro_rules! primitive {
    ($T:ty, $t:ty) => {
        impl CompareProvider<&$T> for $T {
            fn is(left: &Self, right: &$T) -> BooleanArray {
                primitive_is_equal(left, right)
            }

            fn equal(left: &Self, right: &$T) -> BooleanArray {
                arrow::compute::eq(left, right).unwrap()
            }

            fn less(left: &Self, right: &$T) -> BooleanArray {
                arrow::compute::lt(left, right).unwrap()
            }

            fn less_equal(left: &Self, right: &$T) -> BooleanArray {
                arrow::compute::lt_eq(left, right).unwrap()
            }

            fn greater(left: &Self, right: &$T) -> BooleanArray {
                arrow::compute::gt(left, right).unwrap()
            }

            fn greater_equal(left: &Self, right: &$T) -> BooleanArray {
                arrow::compute::gt_eq(left, right).unwrap()
            }
        }

        impl CompareProvider<$t> for $T {
            fn is(left: &Self, right: $t) -> BooleanArray {
                primitive_is_equal_scalar(left, right)
            }

            fn equal(left: &Self, right: $t) -> BooleanArray {
                arrow::compute::eq_scalar(left, right).unwrap()
            }

            fn less(left: &Self, right: $t) -> BooleanArray {
                arrow::compute::lt_scalar(left, right).unwrap()
            }

            fn less_equal(left: &Self, right: $t) -> BooleanArray {
                arrow::compute::lt_eq_scalar(left, right).unwrap()
            }

            fn greater(left: &Self, right: $t) -> BooleanArray {
                arrow::compute::gt_scalar(left, right).unwrap()
            }

            fn greater_equal(left: &Self, right: $t) -> BooleanArray {
                arrow::compute::gt_eq_scalar(left, right).unwrap()
            }
        }
    };
}

fn primitive_is_equal<T: ArrowPrimitiveType>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> BooleanArray {
    assert_eq!(left.len(), right.len());

    let mut builder = BooleanArray::builder(left.len());
    for i in 0..left.len() {
        if left.is_null(i) && right.is_null(i) {
            builder.append_value(true).unwrap();
        } else if left.is_valid(i) && right.is_valid(i) && left.value(i) == right.value(i) {
            builder.append_value(true).unwrap();
        } else {
            builder.append_value(false).unwrap();
        }
    }
    return builder.finish();
}

fn primitive_is_equal_scalar<T: ArrowPrimitiveType>(
    left: &PrimitiveArray<T>,
    right: T::Native,
) -> BooleanArray {
    let mut builder = BooleanArray::builder(left.len());
    for i in 0..left.len() {
        if left.is_valid(i) && left.value(i) == right {
            builder.append_value(true).unwrap();
        } else {
            builder.append_value(false).unwrap();
        }
    }
    return builder.finish();
}

primitive!(Int64Array, i64);
primitive!(Float64Array, f64);
primitive!(Date32Array, i32);
primitive!(TimestampMicrosecondArray, i64);

impl CompareProvider<&StringArray> for StringArray {
    fn is(left: &Self, right: &StringArray) -> BooleanArray {
        assert_eq!(left.len(), right.len());

        let mut builder = BooleanArray::builder(left.len());
        for i in 0..left.len() {
            if left.is_null(i) && right.is_null(i) {
                builder.append_value(true).unwrap();
            } else if left.is_valid(i) && right.is_valid(i) && left.value(i) == right.value(i) {
                builder.append_value(true).unwrap();
            } else {
                builder.append_value(false).unwrap();
            }
        }
        return builder.finish();
    }

    fn equal(left: &Self, right: &StringArray) -> BooleanArray {
        arrow::compute::eq_utf8(left, right).unwrap()
    }

    fn less(left: &Self, right: &StringArray) -> BooleanArray {
        arrow::compute::lt_utf8(left, right).unwrap()
    }

    fn less_equal(left: &Self, right: &StringArray) -> BooleanArray {
        arrow::compute::lt_eq_utf8(left, right).unwrap()
    }

    fn greater(left: &Self, right: &StringArray) -> BooleanArray {
        arrow::compute::gt_utf8(left, right).unwrap()
    }

    fn greater_equal(left: &Self, right: &StringArray) -> BooleanArray {
        arrow::compute::gt_eq_utf8(left, right).unwrap()
    }
}

impl CompareProvider<&str> for StringArray {
    fn is(left: &Self, right: &str) -> BooleanArray {
        let mut builder = BooleanArray::builder(left.len());
        for i in 0..left.len() {
            if left.is_valid(i) && left.value(i) == right {
                builder.append_value(true).unwrap();
            } else {
                builder.append_value(false).unwrap();
            }
        }
        return builder.finish();
    }

    fn equal(left: &Self, right: &str) -> BooleanArray {
        arrow::compute::eq_utf8_scalar(left, right).unwrap()
    }

    fn less(left: &Self, right: &str) -> BooleanArray {
        arrow::compute::lt_utf8_scalar(left, right).unwrap()
    }

    fn less_equal(left: &Self, right: &str) -> BooleanArray {
        arrow::compute::lt_eq_utf8_scalar(left, right).unwrap()
    }

    fn greater(left: &Self, right: &str) -> BooleanArray {
        arrow::compute::gt_utf8_scalar(left, right).unwrap()
    }

    fn greater_equal(left: &Self, right: &str) -> BooleanArray {
        arrow::compute::gt_eq_utf8_scalar(left, right).unwrap()
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
    fn is(left: &Self, right: &Arc<dyn Array>) -> BooleanArray {
        binary!(left, right, is)
    }

    fn equal(left: &Self, right: &Arc<dyn Array>) -> BooleanArray {
        binary!(left, right, equal)
    }

    fn less(left: &Self, right: &Arc<dyn Array>) -> BooleanArray {
        binary!(left, right, less)
    }

    fn less_equal(left: &Self, right: &Arc<dyn Array>) -> BooleanArray {
        binary!(left, right, less_equal)
    }

    fn greater(left: &Self, right: &Arc<dyn Array>) -> BooleanArray {
        binary!(left, right, greater)
    }

    fn greater_equal(left: &Self, right: &Arc<dyn Array>) -> BooleanArray {
        binary!(left, right, greater_equal)
    }
}
