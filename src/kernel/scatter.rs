use arrow::array::*;
use arrow::buffer::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use arrow::util::bit_util::*;
use num::Zero;
use std::sync::Arc;

pub trait ScatterProvider {
    fn scatter(values: &Self, indexes: &UInt32Array) -> Self;
}

pub fn scatter<P: ScatterProvider>(values: &P, indexes: &UInt32Array) -> P {
    P::scatter(values, indexes)
}

impl ScatterProvider for BooleanArray {
    fn scatter(values: &Self, indexes: &UInt32Array) -> Self {
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
        BooleanArray::from(Arc::new(data))
    }
}

macro_rules! primitive {
    ($T:ty) => {
        impl ScatterProvider for $T {
            fn scatter(values: &Self, indexes: &UInt32Array) -> Self {
                scatter_primitive(values, indexes)
            }
        }
    };
}

fn scatter_primitive<T>(values: &PrimitiveArray<T>, indexes: &UInt32Array) -> PrimitiveArray<T>
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
    PrimitiveArray::<T>::from(Arc::new(data))
}

primitive!(Int64Array);
primitive!(Float64Array);
primitive!(Date32Array);
primitive!(TimestampMicrosecondArray);

impl ScatterProvider for StringArray {
    fn scatter(values: &Self, indexes: &UInt32Array) -> Self {
        let data_len = arrow::compute::max(indexes).map(|n| n + 1).unwrap_or(0) as usize;

        let array: &GenericStringArray<i32> = values
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .unwrap();

        let num_bytes = bit_ceil(data_len, 8);
        let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
        let null_slice = null_buf.data_mut();

        let mut offsets = Vec::with_capacity(data_len + 1);
        let mut values = Vec::with_capacity(data_len);
        let mut length_so_far = i32::zero();

        offsets.push(length_so_far);
        for src in 0..indexes.len() {
            let dst = indexes.value(src) as usize;
            if array.is_valid(src) {
                let s = array.value(src);

                length_so_far += i32::from_usize(s.len()).unwrap();
                values.extend_from_slice(s.as_bytes());
            } else {
                unset_bit(null_slice, dst);
            }
            offsets.push(length_so_far);
        }

        let nulls = null_buf.freeze();

        let data = ArrayData::builder(<i32 as StringOffsetSizeTrait>::DATA_TYPE)
            .len(data_len)
            .null_bit_buffer(nulls)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_buffer(Buffer::from(&values[..]))
            .build();
        GenericStringArray::<i32>::from(data)
    }
}

impl ScatterProvider for Arc<dyn Array> {
    fn scatter(values: &Self, indexes: &UInt32Array) -> Self {
        match values.data_type() {
            DataType::Boolean => Arc::new(scatter(as_boolean_array(values), indexes)),
            DataType::Int64 => Arc::new(scatter(as_primitive_array::<Int64Type>(values), indexes)),
            DataType::Float64 => {
                Arc::new(scatter(as_primitive_array::<Float64Type>(values), indexes))
            }
            DataType::Date32(DateUnit::Day) => {
                Arc::new(scatter(as_primitive_array::<Date32Type>(values), indexes))
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => Arc::new(scatter(
                as_primitive_array::<TimestampMicrosecondType>(values),
                indexes,
            )),
            DataType::Utf8 => Arc::new(scatter(as_string_array(values), indexes)),
            other => panic!("{:?} not supported", other),
        }
    }
}

impl ScatterProvider for RecordBatch {
    fn scatter(values: &Self, indexes: &UInt32Array) -> Self {
        let columns = values
            .columns()
            .iter()
            .map(|c| scatter(c, indexes))
            .collect();
        RecordBatch::try_new(values.schema(), columns).unwrap()
    }
}

fn bit_ceil(value: usize, divisor: usize) -> usize {
    let (quot, rem) = (value / divisor, value % divisor);
    if rem > 0 && divisor > 0 {
        quot + 1
    } else {
        quot
    }
}
