use arrow::datatypes::*;
use arrow::{array::*, buffer::MutableBuffer};
use std::sync::Arc;
use twox_hash::xxh3;

pub trait HashProvider {
    fn hash_update(seeds: *mut u64, values: &Self);
    fn len(values: &Self) -> usize;
}

pub fn hash<P: HashProvider>(values: &P) -> UInt64Array {
    let len = P::len(values);
    let buffer = MutableBuffer::new(len * 8).with_bitset(len * 8, false);
    P::hash_update(buffer.raw_data() as *mut u64, values);
    UInt64Array::from(
        ArrayData::builder(DataType::UInt64)
            .add_buffer(buffer.freeze())
            .len(len)
            .build(),
    )
}

macro_rules! primitive {
    ($T:ty) => {
        impl HashProvider for $T {
            fn hash_update(seeds: *mut u64, values: &Self) {
                hash_primitive(seeds, values)
            }
            fn len(values: &Self) -> usize {
                values.len()
            }
        }
    };
}

fn hash_primitive<T: ArrowPrimitiveType>(seeds: *mut u64, values: &PrimitiveArray<T>) {
    for i in 0..values.len() {
        if values.is_valid(i) {
            unsafe {
                let value = values.value(i);
                let data = value.to_byte_slice();
                let seed = *seeds.offset(i as isize);
                *seeds.offset(i as isize) = xxh3::hash64_with_seed(data, seed);
            }
        }
    }
}

primitive!(BooleanArray);
primitive!(Int64Array);
primitive!(Float64Array);
primitive!(Date32Array);
primitive!(TimestampMicrosecondArray);

impl HashProvider for StringArray {
    fn hash_update(seeds: *mut u64, values: &Self) {
        for i in 0..values.len() {
            if values.is_valid(i) {
                unsafe {
                    let data = values.value(i).as_bytes();
                    let seed = *seeds.offset(i as isize);
                    *seeds.offset(i as isize) = xxh3::hash64_with_seed(data, seed);
                }
            }
        }
    }

    fn len(values: &Self) -> usize {
        values.len()
    }
}

impl HashProvider for Arc<dyn Array> {
    fn hash_update(seeds: *mut u64, values: &Self) {
        match values.data_type() {
            DataType::Boolean => {
                HashProvider::hash_update(seeds, as_primitive_array::<BooleanType>(values))
            }
            DataType::Int64 => {
                HashProvider::hash_update(seeds, as_primitive_array::<Int64Type>(values))
            }
            DataType::Float64 => {
                HashProvider::hash_update(seeds, as_primitive_array::<Float64Type>(values))
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => HashProvider::hash_update(
                seeds,
                as_primitive_array::<TimestampMicrosecondType>(values),
            ),
            DataType::Date32(DateUnit::Day) => {
                HashProvider::hash_update(seeds, as_primitive_array::<Date32Type>(values))
            }
            DataType::Utf8 => HashProvider::hash_update(seeds, as_string_array(values)),
            other => panic!("hash({:?}) is not supported", other),
        }
    }

    fn len(values: &Self) -> usize {
        values.len()
    }
}

impl HashProvider for Vec<Arc<dyn Array>> {
    fn hash_update(seeds: *mut u64, values: &Self) {
        for column in values {
            HashProvider::hash_update(seeds, column);
        }
    }

    fn len(values: &Self) -> usize {
        values.first().unwrap().len()
    }
}
