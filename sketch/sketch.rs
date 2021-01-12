use crate::histogram::*;
use hyperloglogplus::*;
use kernel::*;
use std::{
    cmp::Ordering,
    hash::{BuildHasherDefault, Hasher},
};
use twox_hash::xxh3::hash64;

#[derive(Clone)]
pub struct Sketch {
    count_distinct: HyperLogLogPlus<u64, BuildHasherDefault<StubHasher>>,
    histogram: AnyHistogram,
    last_count: f64,
}

#[derive(Clone)]
enum AnyHistogram {
    Bool(Histogram<bool>),
    I64(Histogram<i64>),
    F64(Histogram<NotNan>),
    Date(Histogram<i32>),
    Timestamp(Histogram<i64>),
    String(Histogram<String>),
}

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Default)]
struct NotNan(f64);

impl Eq for NotNan {}
impl Ord for NotNan {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Sketch {
    pub fn new(data_type: DataType) -> Self {
        Self {
            count_distinct: HyperLogLogPlus::new(4, BuildHasherDefault::default()).unwrap(),
            histogram: AnyHistogram::new(data_type),
            last_count: 0.0,
        }
    }

    pub fn count_distinct(&self) -> f64 {
        self.last_count
    }

    pub fn insert(&mut self, column: &AnyArray) {
        match column {
            AnyArray::Bool(array) => {
                let histogram = match &mut self.histogram {
                    AnyHistogram::Bool(h) => h,
                    _ => panic!(),
                };
                for i in 0..array.len() {
                    if let Some(value) = array.get(i) {
                        if value {
                            self.count_distinct.add(&hash64(&[1]))
                        } else {
                            self.count_distinct.add(&hash64(&[0]))
                        }
                        histogram.insert(value);
                    }
                }
            }
            AnyArray::I64(array) => {
                let histogram = match &mut self.histogram {
                    AnyHistogram::I64(h) => h,
                    _ => panic!(),
                };
                for i in 0..array.len() {
                    if let Some(value) = array.get(i) {
                        self.count_distinct.add(&hash64(&value.to_ne_bytes()));
                        histogram.insert(value);
                    }
                }
            }
            AnyArray::F64(array) => {
                let histogram = match &mut self.histogram {
                    AnyHistogram::F64(h) => h,
                    _ => panic!(),
                };
                for i in 0..array.len() {
                    if let Some(value) = array.get(i) {
                        if !value.is_nan() {
                            self.count_distinct.add(&hash64(&value.to_ne_bytes()));
                            histogram.insert(NotNan(value));
                        }
                    }
                }
            }
            AnyArray::Date(array) => {
                let histogram = match &mut self.histogram {
                    AnyHistogram::Date(h) => h,
                    _ => panic!(),
                };
                for i in 0..array.len() {
                    if let Some(value) = array.get(i) {
                        self.count_distinct.add(&hash64(&value.to_ne_bytes()));
                        histogram.insert(value);
                    }
                }
            }
            AnyArray::Timestamp(array) => {
                let histogram = match &mut self.histogram {
                    AnyHistogram::Timestamp(h) => h,
                    _ => panic!(),
                };
                for i in 0..array.len() {
                    if let Some(value) = array.get(i) {
                        self.count_distinct.add(&hash64(&value.to_ne_bytes()));
                        histogram.insert(value);
                    }
                }
            }
            AnyArray::String(array) => {
                let histogram = match &mut self.histogram {
                    AnyHistogram::String(h) => h,
                    _ => panic!(),
                };
                for i in 0..array.len() {
                    if let Some(value) = array.get(i) {
                        self.count_distinct.add(&hash64(value.as_bytes()));
                        histogram.insert(value.to_string());
                    }
                }
            }
        }
        self.last_count = self.count_distinct.count()
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

impl AnyHistogram {
    fn new(data_type: DataType) -> Self {
        match data_type {
            DataType::Bool => AnyHistogram::Bool(Histogram::default()),
            DataType::I64 => AnyHistogram::I64(Histogram::default()),
            DataType::F64 => AnyHistogram::F64(Histogram::default()),
            DataType::Date => AnyHistogram::Date(Histogram::default()),
            DataType::Timestamp => AnyHistogram::Timestamp(Histogram::default()),
            DataType::String => AnyHistogram::String(Histogram::default()),
        }
    }
}
