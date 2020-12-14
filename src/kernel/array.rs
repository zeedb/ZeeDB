use arrow::array::*;
use arrow::buffer::Buffer;
use arrow::datatypes::*;
use arrow::error::ArrowError;
use arrow::record_batch::*;
use ast::Column;
use std::sync::Arc;

impl crate::Kernel for Arc<dyn Array> {
    fn cat(any: &Vec<Self>) -> Self {
        arrow::compute::concat(&any[..]).unwrap()
    }

    fn gather(any: &Self, uint32: &Arc<dyn Array>) -> Self {
        arrow::compute::take(any, as_primitive_array(uint32), None).unwrap()
    }

    fn gather_logical(any: &Self, boolean: &Arc<dyn Array>) -> Self {
        arrow::compute::filter(any.as_ref(), as_primitive_array(boolean)).unwrap()
    }

    fn scatter(any: &Self, uint32: &Arc<dyn Array>) -> Self {
        let indexes = as_primitive_array::<UInt32Type>(uint32);
        crate::scatter::scatter(any, indexes)
    }

    fn repeat(any: &Self, len: usize) -> Self {
        let mut alias = vec![];
        alias.resize_with(len, || any.clone());
        arrow::compute::concat(&alias[..]).unwrap()
    }

    fn sort(any: &Self) -> UInt32Array {
        arrow::compute::sort_to_indices(any, None).unwrap()
    }

    fn hash(any: &Self) -> UInt32Array {
        match any.data_type() {
            DataType::Boolean => hash_typed::<BooleanType>(any),
            DataType::Int64 => hash_typed::<Int64Type>(any),
            DataType::Float64 => hash_typed::<Float64Type>(any),
            DataType::Date32(DateUnit::Day) => hash_typed::<Date32Type>(any),
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                hash_typed::<TimestampMicrosecondType>(any)
            }
            DataType::Utf8 => todo!(),
            other => panic!("{:?} not supported", other),
        }
    }
}

pub fn equal(
    left_any: &Arc<dyn Array>,
    right_any: &Arc<dyn Array>,
) -> Result<Arc<dyn Array>, ArrowError> {
    binary_operator::<Equal>(left_any, right_any)
}

pub fn less(
    left_any: &Arc<dyn Array>,
    right_any: &Arc<dyn Array>,
) -> Result<Arc<dyn Array>, ArrowError> {
    binary_operator::<Less>(left_any, right_any)
}

pub fn less_equal(
    left_any: &Arc<dyn Array>,
    right_any: &Arc<dyn Array>,
) -> Result<Arc<dyn Array>, ArrowError> {
    binary_operator::<LessEq>(left_any, right_any)
}

pub fn and(
    left_bool: &Arc<dyn Array>,
    right_bool: &Arc<dyn Array>,
) -> Result<Arc<dyn Array>, ArrowError> {
    let bool = arrow::compute::and(as_boolean_array(left_bool), as_boolean_array(right_bool))?;
    Ok(Arc::new(bool))
}

fn binary_operator<Op: BinaryOperator>(
    left: &Arc<dyn Array>,
    right: &Arc<dyn Array>,
) -> Result<Arc<dyn Array>, ArrowError> {
    let output = match left.data_type() {
        DataType::Boolean => Op::bool(as_boolean_array(left), as_boolean_array(right))?,
        DataType::Int64 => {
            Op::num::<Int64Type>(as_primitive_array(left), as_primitive_array(right))?
        }
        DataType::Float64 => {
            Op::num::<Float64Type>(as_primitive_array(left), as_primitive_array(right))?
        }
        DataType::Date32(DateUnit::Day) => {
            Op::num::<Date32Type>(as_primitive_array(left), as_primitive_array(right))?
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => Op::num::<TimestampMicrosecondType>(
            as_primitive_array(left),
            as_primitive_array(right),
        )?,
        DataType::Utf8 => Op::utf8(as_string_array(left), as_string_array(right))?,
        other => panic!("{:?} is not a supported type", other),
    };
    Ok(Arc::new(output))
}

trait BinaryOperator {
    type Output: Array + 'static;

    fn bool(left: &BooleanArray, right: &BooleanArray) -> Result<Self::Output, ArrowError>;

    fn num<T: ArrowNumericType>(
        left: &PrimitiveArray<T>,
        right: &PrimitiveArray<T>,
    ) -> Result<Self::Output, ArrowError>;

    fn utf8(left: &StringArray, right: &StringArray) -> Result<Self::Output, ArrowError>;
}

struct Equal;

impl BinaryOperator for Equal {
    type Output = BooleanArray;

    fn bool(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
        todo!()
    }

    fn num<T: ArrowNumericType>(
        left: &PrimitiveArray<T>,
        right: &PrimitiveArray<T>,
    ) -> Result<BooleanArray, ArrowError> {
        arrow::compute::eq(left, right)
    }

    fn utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray, ArrowError> {
        arrow::compute::eq_utf8(left, right)
    }
}

struct Less;

impl BinaryOperator for Less {
    type Output = BooleanArray;

    fn bool(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
        todo!()
    }

    fn num<T: ArrowNumericType>(
        left: &PrimitiveArray<T>,
        right: &PrimitiveArray<T>,
    ) -> Result<BooleanArray, ArrowError> {
        arrow::compute::lt(left, right)
    }

    fn utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray, ArrowError> {
        arrow::compute::lt_utf8(left, right)
    }
}

struct LessEq;

impl BinaryOperator for LessEq {
    type Output = BooleanArray;

    fn bool(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
        todo!()
    }

    fn num<T: ArrowNumericType>(
        left: &PrimitiveArray<T>,
        right: &PrimitiveArray<T>,
    ) -> Result<BooleanArray, ArrowError> {
        arrow::compute::lt_eq(left, right)
    }

    fn utf8(left: &StringArray, right: &StringArray) -> Result<BooleanArray, ArrowError> {
        arrow::compute::lt_eq_utf8(left, right)
    }
}

pub fn int64(int64: &Arc<dyn Array>) -> Option<i64> {
    let int64 = as_primitive_array::<Int64Type>(int64);
    assert!(int64.len() == 1);
    if int64.is_null(0) {
        None
    } else {
        Some(int64.value(0))
    }
}

pub fn any<T: Array + 'static>(any: &T) -> Arc<dyn Array> {
    make_array(any.data())
}

pub fn find(input: &RecordBatch, column: &Column) -> Arc<dyn Array> {
    for i in 0..input.num_columns() {
        if input.schema().field(i).name() == &column.canonical_name() {
            return input.column(i).clone();
        }
    }
    panic!("{} is not in {}", column.name, input.schema())
}
