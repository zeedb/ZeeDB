use arrow::array::*;
use arrow::datatypes::*;
use hyperloglogplus::*;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Arc;

#[derive(Clone)]
pub struct Counter {
    count: f64,
    inner: HyperLogLogPlus<u64, BuildHasherDefault<StubHasher>>,
}

impl Counter {
    pub fn new() -> Self {
        Self {
            count: 0f64,
            inner: HyperLogLogPlus::new(4, BuildHasherDefault::default()).unwrap(),
        }
    }

    pub fn count(&self) -> f64 {
        self.count
    }

    pub fn insert(&mut self, column: &Arc<dyn Array>) {
        match column.data_type() {
            DataType::Boolean => self.insert_generic::<BooleanType>(column),
            DataType::Int64 => self.insert_generic::<Int64Type>(column),
            DataType::Float64 => self.insert_generic::<Float64Type>(column),
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                self.insert_generic::<TimestampMicrosecondType>(column)
            }
            DataType::Date32(DateUnit::Day) => self.insert_generic::<Date32Type>(column),
            DataType::FixedSizeBinary(16) => todo!(),
            DataType::Utf8 => self.insert_utf8(column),
            other => panic!("type {:?} is not supported", other),
        }
        self.count = self.inner.count()
    }

    fn insert_generic<T: ArrowPrimitiveType>(&mut self, column: &Arc<dyn Array>) {
        let column: &PrimitiveArray<T> =
            column.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        for i in 0..column.len() {
            if column.is_valid(i) {
                self.inner.add(&fnv(column.value(i).to_byte_slice()))
            }
        }
    }

    fn insert_utf8(&mut self, column: &Arc<dyn Array>) {
        let column: &StringArray = column.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..column.len() {
            if column.is_valid(i) {
                self.inner.add(&fnv(column.value(i).as_bytes()))
            }
        }
    }
}

#[derive(Default)]
struct StubHasher {
    hash: u64,
}

impl Hasher for StubHasher {
    fn finish(&self) -> u64 {
        self.hash
    }

    fn write(&mut self, _: &[u8]) {
        panic!()
    }

    fn write_u64(&mut self, i: u64) {
        self.hash = i;
    }
}

fn fnv(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325;
    for byte in bytes.iter() {
        hash = hash ^ (*byte as u64);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
