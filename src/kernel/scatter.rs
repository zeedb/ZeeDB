use arrow::array::*;
use arrow::buffer::*;
use arrow::datatypes::*;
use arrow::util::bit_util::*;
use num::Zero;
use std::ops::AddAssign;
use std::sync::Arc;

pub fn scatter(values: &Arc<dyn Array>, indexes: &UInt32Array) -> Arc<dyn Array> {
    assert!(values.len() == indexes.len());

    match values.data_type() {
        DataType::Boolean => scatter_boolean(values, indexes),
        DataType::Int64 => scatter_primitive::<Int64Type>(values, indexes),
        DataType::UInt64 => scatter_primitive::<UInt64Type>(values, indexes),
        DataType::Float64 => scatter_primitive::<Float64Type>(values, indexes),
        DataType::Date32(_) => scatter_primitive::<Date32Type>(values, indexes),
        DataType::Timestamp(Microsecond, _) => {
            scatter_primitive::<TimestampMicrosecondType>(values, indexes)
        }
        DataType::FixedSizeBinary(16) => todo!(),
        DataType::Utf8 => scatter_string::<i32>(values, indexes),
        DataType::Struct(fields) => todo!(),
        DataType::List(element) => todo!(),
        other => panic!("{:?} not supported", other),
    }
}

fn scatter_primitive<T>(values: &Arc<dyn Array>, indexes: &UInt32Array) -> Arc<dyn Array>
where
    T: ArrowPrimitiveType,
{
    let data_len = arrow::compute::max(indexes).map(|n| n + 1).unwrap_or(0) as usize;

    let array = values.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

    let num_bytes = bit_ceil(data_len, 8);
    let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);

    let null_slice = null_buf.data_mut();

    let mut new_values: Vec<T::Native> = Vec::with_capacity(data_len);
    new_values.resize_with(data_len, T::Native::default);

    for src in 0..indexes.len() {
        let dst = indexes.value(src) as usize;
        if array.is_null(src) {
            unset_bit(null_slice, dst);
        }
        new_values[dst] = array.value(src);
    }

    let nulls = null_buf.freeze();

    let data = ArrayData::new(
        T::get_data_type(),
        indexes.len(),
        None,
        Some(nulls),
        0,
        vec![Buffer::from(new_values.to_byte_slice())],
        vec![],
    );
    Arc::new(PrimitiveArray::<T>::from(Arc::new(data)))
}

fn scatter_boolean(values: &Arc<dyn Array>, indexes: &UInt32Array) -> Arc<dyn Array> {
    let data_len = arrow::compute::max(indexes).map(|n| n + 1).unwrap_or(0) as usize;

    let array = values.as_any().downcast_ref::<BooleanArray>().unwrap();

    let num_bytes = bit_ceil(data_len, 8);
    let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
    let mut val_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, false);

    let null_slice = null_buf.data_mut();
    let val_slice = val_buf.data_mut();

    for src in 0..indexes.len() {
        let dst = indexes.value(src) as usize;
        if array.is_null(src) {
            unset_bit(null_slice, dst);
        } else if array.value(src) {
            set_bit(val_slice, dst);
        }
    }

    let nulls = null_buf.freeze();

    let data = ArrayData::new(
        DataType::Boolean,
        indexes.len(),
        None,
        Some(nulls),
        0,
        vec![val_buf.freeze()],
        vec![],
    );
    Arc::new(BooleanArray::from(Arc::new(data)))
}

fn scatter_string<OffsetSize>(values: &Arc<dyn Array>, indexes: &UInt32Array) -> Arc<dyn Array>
where
    OffsetSize: Zero + AddAssign + StringOffsetSizeTrait,
{
    let data_len = arrow::compute::max(indexes).map(|n| n + 1).unwrap_or(0) as usize;

    let array: &GenericStringArray<OffsetSize> = values
        .as_any()
        .downcast_ref::<GenericStringArray<OffsetSize>>()
        .unwrap();

    let num_bytes = bit_ceil(data_len, 8);
    let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
    let null_slice = null_buf.data_mut();

    let mut offsets = Vec::with_capacity(data_len + 1);
    let mut values = Vec::with_capacity(data_len);
    let mut length_so_far = OffsetSize::zero();

    offsets.push(length_so_far);
    for src in 0..indexes.len() {
        let dst = indexes.value(src) as usize;
        if array.is_valid(src) {
            let s = array.value(src);

            length_so_far += OffsetSize::from_usize(s.len()).unwrap();
            values.extend_from_slice(s.as_bytes());
        } else {
            unset_bit(null_slice, dst);
        }
        offsets.push(length_so_far);
    }

    let nulls = null_buf.freeze();

    let data = ArrayData::builder(<OffsetSize as StringOffsetSizeTrait>::DATA_TYPE)
        .len(data_len)
        .null_bit_buffer(nulls)
        .add_buffer(Buffer::from(offsets.to_byte_slice()))
        .add_buffer(Buffer::from(&values[..]))
        .build();
    Arc::new(GenericStringArray::<OffsetSize>::from(data))
}

fn bit_ceil(value: usize, divisor: usize) -> usize {
    let (quot, rem) = (value / divisor, value % divisor);
    if rem > 0 && divisor > 0 {
        quot + 1
    } else {
        quot
    }
}
