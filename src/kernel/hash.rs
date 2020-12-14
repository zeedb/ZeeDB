use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use twox_hash::xxh3;

pub trait HashProvider {
    fn hash(values: &Self) -> UInt32Array;
}

pub fn hash<P: HashProvider>(values: &P) -> UInt32Array {
    P::hash(values)
}

macro_rules! primitive {
    ($T:ty) => {
        impl HashProvider for $T {
            fn hash(values: &Self) -> UInt32Array {
                hash_primitive(values)
            }
        }
    };
}

fn hash_primitive<T: ArrowPrimitiveType>(values: &PrimitiveArray<T>) -> UInt32Array {
    let mut builder = UInt32Array::builder(values.len());
    for i in 0..values.len() {
        if values.is_null(i) {
            builder.append_null().unwrap();
        } else {
            let value = values.value(i);
            let bytes = value.to_byte_slice();
            let hash = fnv(bytes);
            builder.append_value(hash).unwrap();
        }
    }
    builder.finish()
}

primitive!(BooleanArray);
primitive!(Int64Array);
primitive!(Float64Array);
primitive!(Date32Array);
primitive!(TimestampMicrosecondArray);

impl HashProvider for StringArray {
    fn hash(values: &Self) -> UInt32Array {
        let mut builder = UInt32Array::builder(values.len());
        for i in 0..values.len() {
            if values.is_null(i) {
                builder.append_null().unwrap();
            } else {
                let hash64 = xxh3::hash64(values.value(i).as_bytes());
                builder.append_value(hash64 as u32).unwrap();
            }
        }
        builder.finish()
    }
}

impl HashProvider for Arc<dyn Array> {
    fn hash(values: &Self) -> UInt32Array {
        match values.data_type() {
            DataType::Boolean => hash(as_primitive_array::<BooleanType>(values)),
            DataType::Int64 => hash(as_primitive_array::<Int64Type>(values)),
            DataType::Float64 => hash(as_primitive_array::<Float64Type>(values)),
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                hash(as_primitive_array::<TimestampMicrosecondType>(values))
            }
            DataType::Date32(DateUnit::Day) => hash(as_primitive_array::<Date32Type>(values)),
            DataType::Utf8 => hash(as_string_array(values)),
            other => panic!("+({:?}) is not supported", other),
        }
    }
}

impl HashProvider for Vec<Arc<dyn Array>> {
    fn hash(values: &Self) -> UInt32Array {
        let mut output: Option<UInt32Array> = None;
        for column in values {
            let next = hash(column);
            output = match output {
                None => Some(next),
                Some(prev) => Some(crate::bit_or(&prev, &next)),
            }
        }
        output.unwrap()
    }
}

fn fnv(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c9dc5;
    for byte in bytes.iter() {
        hash = hash ^ (*byte as u32);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

// TODO impl HashProvider for strings with a different algorithm that is efficient for long keys.
