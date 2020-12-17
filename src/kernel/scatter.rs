use arrow::array::*;
use arrow::buffer::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::sync::Arc;

pub trait ScatterProvider {
    fn scatter(into: &Self, indexes: &UInt32Array, from: &Self) -> Self;
}

pub fn scatter<P: ScatterProvider>(into: &P, indexes: &UInt32Array, from: &P) -> P {
    P::scatter(into, indexes, from)
}

impl ScatterProvider for BooleanArray {
    fn scatter(into: &Self, indexes: &UInt32Array, from: &Self) -> Self {
        assert_eq!(indexes.len(), from.len());

        let (mut values_buffer, mut valid_buffer) = copy_buffers(into);
        let values_slice = values_buffer.data_mut();
        let valid_slice = valid_buffer.data_mut();
        for src in 0..indexes.len() {
            let dst = indexes.value(src) as usize;
            if from.is_null(src) {
                crate::bits::unset_bit(valid_slice, dst);
            } else if from.value(src) {
                crate::bits::set_bit(values_slice, dst);
                crate::bits::set_bit(valid_slice, dst);
            }
        }
        let data = ArrayData::new(
            DataType::Boolean,
            into.len(),
            None,
            Some(valid_buffer.freeze()),
            0,
            vec![values_buffer.freeze()],
            vec![],
        );
        BooleanArray::from(Arc::new(data))
    }
}

fn copy_buffers<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
) -> (MutableBuffer, MutableBuffer) {
    let values = array.data_ref().buffers().first().unwrap();
    let valid = array.data_ref().null_buffer();
    let mut values_buffer = MutableBuffer::new(values.len());
    let mut valid_buffer = MutableBuffer::new((array.len() + 7) / 8);
    values_buffer
        .write_bytes(values.data(), values.len())
        .unwrap();
    if let Some(valid) = valid {
        valid_buffer.write_bytes(valid.data(), valid.len()).unwrap();
    } else {
        let capacity = valid_buffer.capacity();
        valid_buffer = valid_buffer.with_bitset(capacity, true);
    }
    (values_buffer, valid_buffer)
}

macro_rules! primitive {
    ($T:ty) => {
        impl ScatterProvider for $T {
            fn scatter(into: &Self, indexes: &UInt32Array, from: &Self) -> Self {
                scatter_primitive(into, indexes, from)
            }
        }
    };
}

fn scatter_primitive<T>(
    into: &PrimitiveArray<T>,
    indexes: &UInt32Array,
    from: &PrimitiveArray<T>,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
{
    assert_eq!(indexes.len(), from.len());

    let (mut values_buffer, mut valid_buffer) = copy_buffers(into);
    let values_ptr = values_buffer.raw_data_mut() as *mut T::Native;
    let values_slice = unsafe { std::slice::from_raw_parts_mut(values_ptr, into.len()) };
    let valid_slice = valid_buffer.data_mut();

    for src in 0..indexes.len() {
        let dst = indexes.value(src) as usize;
        if from.is_null(src) {
            crate::bits::unset_bit(valid_slice, dst);
        } else {
            crate::bits::set_bit(valid_slice, dst);
            values_slice[dst] = from.value(src);
        }
    }

    let data = ArrayData::new(
        T::get_data_type(),
        into.len(),
        None,
        Some(valid_buffer.freeze()),
        0,
        vec![values_buffer.freeze()],
        vec![],
    );
    PrimitiveArray::<T>::from(Arc::new(data))
}

primitive!(Int64Array);
primitive!(Float64Array);
primitive!(Date32Array);
primitive!(TimestampMicrosecondArray);

impl ScatterProvider for Arc<dyn Array> {
    fn scatter(into: &Self, indexes: &UInt32Array, from: &Self) -> Self {
        match into.data_type() {
            DataType::Boolean => Arc::new(scatter(
                as_boolean_array(into),
                indexes,
                as_boolean_array(from),
            )),
            DataType::Int64 => Arc::new(scatter(
                as_primitive_array::<Int64Type>(into),
                indexes,
                as_primitive_array(from),
            )),
            DataType::Float64 => Arc::new(scatter(
                as_primitive_array::<Float64Type>(into),
                indexes,
                as_primitive_array(from),
            )),
            DataType::Date32(DateUnit::Day) => Arc::new(scatter(
                as_primitive_array::<Date32Type>(into),
                indexes,
                as_primitive_array(from),
            )),
            DataType::Timestamp(TimeUnit::Microsecond, None) => Arc::new(scatter(
                as_primitive_array::<TimestampMicrosecondType>(into),
                indexes,
                as_primitive_array(from),
            )),
            other => panic!("scatter({:?}) not supported", other),
        }
    }
}

impl ScatterProvider for RecordBatch {
    fn scatter(into: &Self, indexes: &UInt32Array, from: &Self) -> Self {
        let columns = (0..into.num_columns())
            .map(|i| scatter(into.column(i), indexes, from.column(i)))
            .collect();
        RecordBatch::try_new(into.schema(), columns).unwrap()
    }
}
