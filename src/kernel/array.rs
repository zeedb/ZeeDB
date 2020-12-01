use arrow::array::*;
use arrow::datatypes::*;
use arrow::error::ArrowError;
use arrow::record_batch::*;
use ast::Column;
use std::ops::BitOr;
use std::sync::Arc;

impl crate::Kernel for Arc<dyn Array> {
    fn cat(any: &Vec<Self>) -> Self {
        arrow::compute::concat(&any[..]).unwrap()
    }

    fn gather(any: &Self, uint32: &Arc<dyn Array>) -> Self {
        arrow::compute::take(any, coerce(uint32), None).unwrap()
    }

    fn gather_logical(any: &Self, boolean: &Arc<dyn Array>) -> Self {
        arrow::compute::filter(any.as_ref(), coerce(boolean)).unwrap()
    }

    fn scatter(any: &Self, uint32: &Arc<dyn Array>) -> Self {
        let indexes = coerce::<UInt32Array>(uint32);
        crate::scatter::scatter(any, indexes)
    }

    fn repeat(any: &Self, len: usize) -> Self {
        let mut alias = vec![];
        alias.resize_with(len, || any.clone());
        arrow::compute::concat(&alias[..]).unwrap()
    }

    fn sort(any: &Self) -> Arc<dyn Array> {
        let u32 = arrow::compute::sort_to_indices(any, None).unwrap();
        Arc::new(u32)
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
    let bool = arrow::compute::and(coerce(left_bool), coerce(right_bool))?;
    Ok(Arc::new(bool))
}

fn binary_operator<Op: BinaryOperator>(
    left: &Arc<dyn Array>,
    right: &Arc<dyn Array>,
) -> Result<Arc<dyn Array>, ArrowError> {
    let output = match left.data_type() {
        DataType::Boolean => Op::bool(coerce(left), coerce(right))?,
        DataType::Int64 => Op::num::<Int64Type>(coerce(left), coerce(right))?,
        DataType::UInt64 => Op::num::<UInt64Type>(coerce(left), coerce(right))?,
        DataType::Float64 => Op::num::<Float64Type>(coerce(left), coerce(right))?,
        DataType::Date32(DateUnit::Day) => Op::num::<Date32Type>(coerce(left), coerce(right))?,
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Op::num::<TimestampMicrosecondType>(coerce(left), coerce(right))?
        }
        DataType::FixedSizeBinary(16) => todo!(),
        DataType::Utf8 => Op::utf8(coerce(left), coerce(right))?,
        DataType::Struct(fields) => todo!(),
        DataType::List(element) => todo!(),
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

pub fn nulls(len: usize, as_type: &DataType) -> Arc<dyn Array> {
    match as_type {
        DataType::Boolean => null_bool_array(len),
        DataType::Int64 => null_primitive_array::<Int64Type>(len),
        DataType::UInt64 => null_primitive_array::<UInt64Type>(len),
        DataType::Float64 => null_primitive_array::<Float64Type>(len),
        DataType::Date32(DateUnit::Day) => null_primitive_array::<Date32Type>(len),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            null_primitive_array::<TimestampMicrosecondType>(len)
        }
        DataType::FixedSizeBinary(16) => todo!(),
        DataType::Utf8 => null_string_array(len),
        DataType::Struct(fields) => todo!(),
        DataType::List(element) => todo!(),
        other => panic!("{:?} not supported", other),
    }
}

fn null_bool_array(num_rows: usize) -> Arc<dyn Array> {
    let mut array = BooleanArray::builder(num_rows);
    for _ in 0..num_rows {
        array.append_null().unwrap();
    }
    Arc::new(array.finish())
}

fn null_primitive_array<T: ArrowNumericType>(num_rows: usize) -> Arc<dyn Array> {
    let mut array = PrimitiveArray::<T>::builder(num_rows);
    for _ in 0..num_rows {
        array.append_null().unwrap();
    }
    Arc::new(array.finish())
}

fn null_string_array(num_rows: usize) -> Arc<dyn Array> {
    let mut array = StringBuilder::new(num_rows);
    for _ in 0..num_rows {
        array.append_null().unwrap();
    }
    Arc::new(array.finish())
}

pub fn int64(int64: &Arc<dyn Array>) -> i64 {
    coerce::<Int64Array>(int64).value(0)
}

pub fn coerce<T: 'static>(any: &Arc<dyn Array>) -> &T {
    any.as_any().downcast_ref::<T>().unwrap()
}

pub fn hash(any: &Arc<dyn Array>, buckets: usize) -> Arc<dyn Array> {
    match any.data_type() {
        DataType::Boolean => hash_typed::<BooleanType>(any, buckets),
        DataType::Int64 => hash_typed::<Int64Type>(any, buckets),
        DataType::Float64 => hash_typed::<Float64Type>(any, buckets),
        DataType::Date32(DateUnit::Day) => hash_typed::<Date32Type>(any, buckets),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            hash_typed::<TimestampMicrosecondType>(any, buckets)
        }
        DataType::FixedSizeBinary(16) => todo!(),
        DataType::Utf8 => todo!(),
        DataType::Struct(fields) => todo!(),
        DataType::List(element) => todo!(),
        other => panic!("{:?} not supported", other),
    }
}

fn hash_typed<T: ArrowPrimitiveType>(any: &Arc<dyn Array>, buckets: usize) -> Arc<dyn Array> {
    assert!(buckets > 0);
    let any: &PrimitiveArray<T> = any.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let mut output = UInt32Array::builder(any.len());
    for i in 0..any.len() {
        let hash = fnv(any.value(i).to_byte_slice());
        let bucket = hash % (buckets as u64);
        output.append_value(bucket as u32).unwrap();
    }
    let u32 = output.finish();
    Arc::new(u32)
}

fn fnv(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325;
    for byte in bytes.iter() {
        hash = hash ^ (*byte as u64);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

pub fn bit_or(left: &Arc<dyn Array>, right: &Arc<dyn Array>) -> Arc<dyn Array> {
    let mut builder = ArrayData::builder(left.data_type().clone());
    let left_data = left.data();
    let right_data = right.data();
    let left_buffer = left_data.buffers().first().unwrap();
    let right_buffer = right_data.buffers().first().unwrap();
    let buffer = (left_buffer | right_buffer).unwrap();
    builder = builder.add_buffer(buffer);
    let left_nulls = left_data.null_buffer();
    let right_nulls = right_data.null_buffer();
    match (left_nulls, right_nulls) {
        (Some(left), Some(right)) => builder = builder.null_bit_buffer((left | right).unwrap()),
        (Some(bits), _) | (_, Some(bits)) => builder = builder.null_bit_buffer(bits.clone()),
        (None, None) => {}
    };
    make_array(builder.build())
}

pub fn find(input: &RecordBatch, column: &Column) -> Arc<dyn Array> {
    for i in 0..input.num_columns() {
        if input.schema().field(i).name() == &column.canonical_name() {
            return input.column(i).clone();
        }
    }
    panic!("{} is not in {}", column.name, input.schema())
}
