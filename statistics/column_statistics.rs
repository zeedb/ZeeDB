use crate::histogram::*;
use hyperloglogplus::*;
use kernel::*;
use std::{
    cmp::Ordering,
    hash::{BuildHasherDefault, Hash, Hasher},
};

#[derive(Clone)]
pub enum ColumnStatistics {
    Bool(TypedColumnStatistics<bool>),
    I64(TypedColumnStatistics<i64>),
    F64(TypedColumnStatistics<NotNan>),
    Date(TypedColumnStatistics<i32>),
    Timestamp(TypedColumnStatistics<i64>),
    String(TypedColumnStatistics<String>),
}

#[derive(Clone, Default)]
pub struct TypedColumnStatistics<T: Hash + Ord + Default + Clone> {
    count_distinct: CountDistinct<T>,
    histogram: Histogram<T>,
}

#[derive(Clone)]
struct CountDistinct<T: Hash> {
    hll: HyperLogLogPlus<T, BuildHasherDefault<twox_hash::Xxh3Hash64>>,
    cache: f64,
}

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Default)]
pub struct NotNan(pub f64);

impl Eq for NotNan {}
impl Ord for NotNan {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
impl Hash for NotNan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let not_nan: u64 = unsafe { std::mem::transmute(*self) };
        not_nan.hash(state);
    }
}

impl ColumnStatistics {
    pub fn new(data_type: DataType) -> Self {
        match data_type {
            DataType::Bool => ColumnStatistics::Bool(TypedColumnStatistics::default()),
            DataType::I64 => ColumnStatistics::I64(TypedColumnStatistics::default()),
            DataType::F64 => ColumnStatistics::F64(TypedColumnStatistics::default()),
            DataType::Date => ColumnStatistics::Date(TypedColumnStatistics::default()),
            DataType::Timestamp => ColumnStatistics::Timestamp(TypedColumnStatistics::default()),
            DataType::String => ColumnStatistics::String(TypedColumnStatistics::default()),
        }
    }

    pub fn union(left: &Self, right: &Self) -> Self {
        match (left, right) {
            (ColumnStatistics::Bool(left), ColumnStatistics::Bool(right)) => {
                ColumnStatistics::Bool(TypedColumnStatistics::union(left, right))
            }
            (ColumnStatistics::I64(left), ColumnStatistics::I64(right)) => {
                ColumnStatistics::I64(TypedColumnStatistics::union(left, right))
            }
            (ColumnStatistics::F64(left), ColumnStatistics::F64(right)) => {
                ColumnStatistics::F64(TypedColumnStatistics::union(left, right))
            }
            (ColumnStatistics::Date(left), ColumnStatistics::Date(right)) => {
                ColumnStatistics::Date(TypedColumnStatistics::union(left, right))
            }
            (ColumnStatistics::Timestamp(left), ColumnStatistics::Timestamp(right)) => {
                ColumnStatistics::Timestamp(TypedColumnStatistics::union(left, right))
            }
            (ColumnStatistics::String(left), ColumnStatistics::String(right)) => {
                ColumnStatistics::String(TypedColumnStatistics::union(left, right))
            }
            (_, _) => panic!("{} does not match {}", left.data_type(), right.data_type()),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            ColumnStatistics::Bool { .. } => DataType::Bool,
            ColumnStatistics::I64 { .. } => DataType::I64,
            ColumnStatistics::F64 { .. } => DataType::F64,
            ColumnStatistics::Date { .. } => DataType::Date,
            ColumnStatistics::Timestamp { .. } => DataType::Timestamp,
            ColumnStatistics::String { .. } => DataType::String,
        }
    }

    pub fn count_distinct(&self) -> f64 {
        match self {
            ColumnStatistics::Bool(typed) => typed.count_distinct(),
            ColumnStatistics::I64(typed) => typed.count_distinct(),
            ColumnStatistics::F64(typed) => typed.count_distinct(),
            ColumnStatistics::Date(typed) => typed.count_distinct(),
            ColumnStatistics::Timestamp(typed) => typed.count_distinct(),
            ColumnStatistics::String(typed) => typed.count_distinct(),
        }
    }

    pub fn insert(&mut self, column: &AnyArray) {
        match (self, column) {
            (ColumnStatistics::Bool(typed), AnyArray::Bool(array)) => {
                typed.insert(array, |x| Some(x));
            }
            (ColumnStatistics::I64(typed), AnyArray::I64(array)) => {
                typed.insert(array, |x| Some(x));
            }
            (ColumnStatistics::F64(typed), AnyArray::F64(array)) => {
                typed.insert(array, |x| if x.is_nan() { None } else { Some(NotNan(x)) });
            }
            (ColumnStatistics::Date(typed), AnyArray::Date(array)) => {
                typed.insert(array, |x| Some(x));
            }
            (ColumnStatistics::Timestamp(typed), AnyArray::Timestamp(array)) => {
                typed.insert(array, |x| Some(x));
            }
            (ColumnStatistics::String(typed), AnyArray::String(array)) => {
                typed.insert(array, |x| Some(x.to_string()));
            }
            (left, right) => panic!("{} does not match {}", left.data_type(), right.data_type()),
        }
    }
}

impl<T: Hash + Ord + Default + Clone> TypedColumnStatistics<T> {
    pub fn probability_density(&self, value: T) -> f64 {
        let (_, upper_bound) = self.histogram.probability(value);
        upper_bound
    }

    pub fn cumulative_probability(&self, value: T) -> f64 {
        self.histogram.rank(value).unwrap_or(0.0)
    }

    fn union(left: &Self, right: &Self) -> Self {
        let mut left = left.clone();
        left.count_distinct
            .hll
            .merge(&right.count_distinct.hll)
            .unwrap();
        left.count_distinct.cache = left.count_distinct.hll.count();
        left.histogram.merge(right.histogram.clone());
        left
    }

    fn count_distinct(&self) -> f64 {
        self.count_distinct.cache
    }

    fn insert<'a, A: Array<'a>>(&mut self, array: &'a A, lens: impl Fn(A::Element) -> Option<T>) {
        for i in 0..array.len() {
            if let Some(value) = array.get(i) {
                if let Some(mapped) = lens(value) {
                    self.count_distinct.hll.add(&mapped);
                    self.histogram.insert(mapped);
                }
            }
        }
        self.count_distinct.cache = self.count_distinct.hll.count();
    }
}

impl<T: Hash> Default for CountDistinct<T> {
    fn default() -> Self {
        Self {
            hll: HyperLogLogPlus::new(4, BuildHasherDefault::default()).unwrap(),
            cache: 0.0,
        }
    }
}
