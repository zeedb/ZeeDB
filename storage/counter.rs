use hyperloglogplus::*;
use kernel::*;
use std::hash::{BuildHasherDefault, Hasher};
use twox_hash::xxh3::hash64;

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

    pub fn insert(&mut self, column: &AnyArray) {
        match column {
            AnyArray::Bool(array) => {
                for i in 0..array.len() {
                    if let Some(value) = array.get(i) {
                        if value {
                            self.inner.add(&hash64(&[1]))
                        } else {
                            self.inner.add(&hash64(&[0]))
                        }
                    }
                }
            }
            AnyArray::I64(array) => {
                for i in 0..array.len() {
                    if let Some(value) = array.get(i) {
                        self.inner.add(&hash64(&value.to_ne_bytes()))
                    }
                }
            }
            AnyArray::F64(array) => {
                for i in 0..array.len() {
                    if let Some(value) = array.get(i) {
                        self.inner.add(&hash64(&value.to_ne_bytes()))
                    }
                }
            }
            AnyArray::Date(array) => {
                for i in 0..array.len() {
                    if let Some(value) = array.get(i) {
                        self.inner.add(&hash64(&value.to_ne_bytes()))
                    }
                }
            }
            AnyArray::Timestamp(array) => {
                for i in 0..array.len() {
                    if let Some(value) = array.get(i) {
                        self.inner.add(&hash64(&value.to_ne_bytes()))
                    }
                }
            }
            AnyArray::String(array) => {
                for i in 0..array.len() {
                    if let Some(value) = array.get(i) {
                        self.inner.add(&hash64(value.as_bytes()))
                    }
                }
            }
        }
        self.count = self.inner.count()
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
