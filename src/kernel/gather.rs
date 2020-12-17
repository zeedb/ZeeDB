use arrow::array::*;
use arrow::buffer::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::sync::Arc;

pub trait GatherProvider {
    fn gather(values: &Self, indexes: &UInt32Array) -> Self;
    fn gather_logical(values: &Self, mask: &BooleanArray) -> Self;
}

pub fn gather<P: GatherProvider>(values: &P, indexes: &UInt32Array) -> P {
    P::gather(values, indexes)
}

pub fn gather_logical<P: GatherProvider>(values: &P, mask: &BooleanArray) -> P {
    P::gather_logical(values, mask)
}

impl GatherProvider for BooleanArray {
    fn gather(values: &Self, indexes: &UInt32Array) -> Self {
        let any = arrow::compute::take(&crate::util::any(values), indexes, None).unwrap();
        Self::from(any.data())
    }

    fn gather_logical(values: &Self, mask: &BooleanArray) -> Self {
        assert_eq!(values.len(), mask.len());

        let n = count_bits(mask);
        let values_buffer = mask_bits(values.data().buffers().first().unwrap(), mask, n);
        let mut builder = ArrayData::builder(values.data_type().clone())
            .len(n)
            .add_buffer(values_buffer);
        if let Some(nulls) = values.data().null_buffer() {
            builder = builder.null_bit_buffer(mask_bits(nulls, mask, n));
        }
        Self::from(builder.build())
    }
}

macro_rules! primitive {
    ($T:ty) => {
        impl GatherProvider for $T {
            fn gather(values: &Self, indexes: &UInt32Array) -> Self {
                let any = arrow::compute::take(&crate::util::any(values), indexes, None).unwrap();
                Self::from(any.data())
            }

            fn gather_logical(values: &Self, mask: &BooleanArray) -> Self {
                assert_eq!(values.len(), mask.len());

                mask_primitive(values, mask)
            }
        }
    };
}

primitive!(UInt32Array);
primitive!(Int64Array);
primitive!(Float64Array);
primitive!(Date32Array);
primitive!(TimestampMicrosecondArray);

impl GatherProvider for StringArray {
    fn gather(values: &Self, indexes: &UInt32Array) -> Self {
        let any = arrow::compute::take(&crate::util::any(values), indexes, None).unwrap();
        Self::from(any.data())
    }

    fn gather_logical(values: &Self, mask: &BooleanArray) -> Self {
        assert_eq!(values.len(), mask.len());

        let mut builder = StringBuilder::new(values.len());
        for i in 0..values.len() {
            if mask.value(i) {
                if values.is_valid(i) {
                    builder.append_value(values.value(i)).unwrap();
                } else {
                    builder.append_null().unwrap();
                }
            }
        }
        builder.finish()
    }
}

impl GatherProvider for Arc<dyn Array> {
    fn gather(values: &Self, indexes: &UInt32Array) -> Self {
        arrow::compute::take(values, indexes, None).unwrap()
    }

    fn gather_logical(values: &Self, mask: &BooleanArray) -> Self {
        match values.data_type() {
            DataType::Boolean => Arc::new(gather_logical(as_boolean_array(values), mask)),
            DataType::Int64 => Arc::new(gather_logical(
                as_primitive_array::<Int64Type>(values),
                mask,
            )),
            DataType::Float64 => Arc::new(gather_logical(
                as_primitive_array::<Float64Type>(values),
                mask,
            )),
            DataType::Date32(DateUnit::Day) => Arc::new(gather_logical(
                as_primitive_array::<Date32Type>(values),
                mask,
            )),
            DataType::Timestamp(TimeUnit::Microsecond, None) => Arc::new(gather_logical(
                as_primitive_array::<TimestampMicrosecondType>(values),
                mask,
            )),
            DataType::Utf8 => Arc::new(gather_logical(as_string_array(values), mask)),
            other => panic!("gather_logical({:?}) not supported", other),
        }
    }
}

impl GatherProvider for Vec<Arc<dyn Array>> {
    fn gather(values: &Self, indexes: &UInt32Array) -> Self {
        values.iter().map(|c| gather(c, indexes)).collect()
    }

    fn gather_logical(values: &Self, mask: &BooleanArray) -> Self {
        values.iter().map(|c| gather_logical(c, mask)).collect()
    }
}

impl GatherProvider for RecordBatch {
    fn gather(values: &Self, indexes: &UInt32Array) -> Self {
        RecordBatch::try_new(values.schema(), gather(&values.columns().to_vec(), indexes)).unwrap()
    }

    fn gather_logical(values: &Self, mask: &BooleanArray) -> Self {
        RecordBatch::try_new(
            values.schema(),
            gather_logical(&values.columns().to_vec(), mask),
        )
        .unwrap()
    }
}

fn mask_primitive<T: ArrowNumericType>(
    from: &PrimitiveArray<T>,
    mask: &BooleanArray,
) -> PrimitiveArray<T> {
    let n = count_bits(mask);
    let values_buffer = mask_bytes(
        from.data().buffers().first().unwrap(),
        std::mem::size_of::<T::Native>(),
        mask,
        n,
    );
    let mut builder = ArrayData::builder(from.data_type().clone())
        .len(n)
        .add_buffer(values_buffer);
    if let Some(nulls) = from.data().null_buffer() {
        builder = builder.null_bit_buffer(mask_bits(nulls, mask, n));
    }
    PrimitiveArray::from(builder.build())
}

fn mask_bytes(from: &Buffer, width: usize, mask: &BooleanArray, n: usize) -> Buffer {
    assert!(mask.len() * width <= from.len());

    let mask_bits = mask.data_ref().buffers().first().unwrap().data();
    let mut into = MutableBuffer::new(n * width);
    into.resize(n * width).unwrap();
    let mut src = from.raw_data();
    let mut dst = into.raw_data_mut();
    unsafe {
        for i in 0..mask.len() {
            if crate::bits::get_bit(mask_bits, i) {
                std::ptr::copy_nonoverlapping(src, dst, width);
                dst = dst.offset(width as isize);
            }
            src = src.offset(width as isize);
        }
    }
    into.freeze()
}

fn mask_bits(from: &Buffer, mask: &BooleanArray, n: usize) -> Buffer {
    assert!(mask.len() / 8 <= from.len());

    let mask_bits = mask.data_ref().buffers().first().unwrap().data();
    let n_bytes = (n + 7) / 8;
    let mut into = MutableBuffer::new(n_bytes);
    into.resize(n_bytes).unwrap();
    let from_bits = from.raw_data();
    let into_bits = into.raw_data_mut();
    let mut dst = 0;
    unsafe {
        for src in 0..mask.len() {
            if crate::bits::get_bit(mask_bits, src) {
                if crate::bits::get_bit_raw(from_bits, src) {
                    crate::bits::set_bit_raw(into_bits, dst);
                } else {
                    crate::bits::unset_bit_raw(into_bits, dst);
                }
                dst += 1;
                dbg!(src, dst, std::slice::from_raw_parts(into.raw_data(), 1));
            }
        }
    }
    into.freeze()
}

fn count_bits(mask: &BooleanArray) -> usize {
    arrow::util::bit_util::count_set_bits_offset(
        mask.data_ref().buffers().first().unwrap().data(),
        0,
        mask.len(),
    )
}
