use crate::any_array::*;
use crate::bitmask::*;
use crate::bool_array::*;
use std::cmp::Ordering;
use twox_hash::xxh3;

#[derive(Clone)]
pub struct I32Array {
    values: Vec<i32>,
    is_valid: Bitmask,
}

#[derive(Clone)]
pub struct I64Array {
    values: Vec<i64>,
    is_valid: Bitmask,
}

#[derive(Clone)]
pub struct U64Array {
    values: Vec<u64>,
    is_valid: Bitmask,
}

#[derive(Clone)]
pub struct F64Array {
    values: Vec<f64>,
    is_valid: Bitmask,
}

#[derive(Clone)]
pub struct DateArray {
    values: Vec<i32>,
    is_valid: Bitmask,
}

#[derive(Clone)]
pub struct TimestampArray {
    values: Vec<i64>,
    is_valid: Bitmask,
}

macro_rules! primitive_ops {
    ($T:ty, $t:ty) => {
        impl $T {
            pub fn new() -> Self {
                Self {
                    values: vec![],
                    is_valid: Bitmask::new(),
                }
            }

            pub fn with_capacity(capacity: usize) -> Self {
                Self {
                    values: Vec::with_capacity(capacity),
                    is_valid: Bitmask::with_capacity(capacity),
                }
            }

            pub fn nulls(len: usize) -> Self {
                Self {
                    values: vec![Default::default()].repeat(len),
                    is_valid: Bitmask::falses(len),
                }
            }

            pub fn zeros(len: usize) -> Self {
                Self {
                    values: vec![Default::default()].repeat(len),
                    is_valid: Bitmask::trues(len),
                }
            }

            pub fn from_slice(values: &[$t], is_valid: &[u8]) -> Self {
                assert_eq!((values.len() + 7) / 8, is_valid.len());

                Self {
                    values: values.to_vec(),
                    is_valid: Bitmask::from_slice(is_valid, values.len()),
                }
            }

            pub fn repeat(&self, n: usize) -> Self {
                todo!()
            }

            pub fn extend(&mut self, other: &Self) -> Self {
                todo!()
            }

            pub fn len(&self) -> usize {
                self.values.len()
            }

            pub fn get(&self, index: usize) -> Option<$t> {
                if self.is_valid.get(index) {
                    Some(self.values[index])
                } else {
                    None
                }
            }

            pub fn set(&mut self, index: usize, value: Option<$t>) {
                if let Some(value) = value {
                    self.is_valid.set(index, true);
                    self.values[index] = value;
                } else {
                    self.is_valid.set(index, false);
                    self.values[index] = Default::default();
                }
            }

            pub fn push(&mut self, value: Option<$t>) {
                if let Some(value) = value {
                    self.is_valid.push(true);
                    self.values.push(value);
                } else {
                    self.is_valid.push(false);
                    self.values.push(Default::default());
                }
            }

            pub fn gather(&self, indexes: &I32Array) -> Self {
                let mut into = Self::nulls(indexes.len());
                for i in 0..indexes.len() {
                    if let Some(j) = indexes.get(i) {
                        into.set(i, self.get(j as usize))
                    } else {
                        into.set(i, None)
                    }
                }
                into
            }

            pub fn compress(&self, mask: &BoolArray) -> Self {
                assert_eq!(self.len(), mask.len());

                let mut into = Self::new();
                for i in 0..self.len() {
                    if mask.get(i) == Some(true) {
                        into.push(self.get(i));
                    }
                }
                into
            }

            pub fn scatter(&self, indexes: &I32Array, into: &mut Self) {
                for i in 0..indexes.len() {
                    if let Some(j) = indexes.get(i) {
                        into.set(j as usize, self.get(i))
                    }
                }
            }

            pub fn transpose(&self, stride: usize) -> Self {
                todo!()
            }

            pub fn cmp(&self, i: usize, j: usize) -> Ordering {
                self.get(i).partial_cmp(&self.get(j)).unwrap()
            }

            pub fn sort(&self) -> I32Array {
                let mut indexes: Vec<_> = (0..self.len() as i32).collect();
                indexes.sort_by(|i, j| self.cmp(*i as usize, *j as usize));
                I32Array::from(indexes)
            }

            pub fn is(&self, other: Self) -> BoolArray {
                todo!()
            }

            pub fn equal(&self, other: Self) -> BoolArray {
                todo!()
            }

            pub fn not_equal(&self, other: &Self) -> BoolArray {
                todo!()
            }

            pub fn less(&self, other: Self) -> BoolArray {
                todo!()
            }

            pub fn less_equal(&self, other: Self) -> BoolArray {
                todo!()
            }

            pub fn greater(&self, other: Self) -> BoolArray {
                todo!()
            }

            pub fn greater_equal(&self, other: Self) -> BoolArray {
                todo!()
            }

            pub fn is_scalar(&self, other: Option<$t>) -> BoolArray {
                todo!()
            }

            pub fn equal_scalar(&self, other: Option<$t>) -> BoolArray {
                todo!()
            }

            pub fn less_scalar(&self, other: Option<$t>) -> BoolArray {
                todo!()
            }

            pub fn less_equal_scalar(&self, other: Option<$t>) -> BoolArray {
                todo!()
            }

            pub fn greater_scalar(&self, other: Option<$t>) -> BoolArray {
                todo!()
            }

            pub fn greater_equal_scalar(&self, other: Option<$t>) -> BoolArray {
                todo!()
            }

            pub fn is_null(&self) -> BoolArray {
                todo!()
            }

            pub fn hash(&self, state: &mut U64Array) {
                for i in 0..self.len() {
                    if let Some(value) = self.get(i) {
                        state.set(
                            i,
                            Some(xxh3::hash64_with_seed(
                                &value.to_ne_bytes(),
                                state.get(i).unwrap(),
                            )),
                        )
                    }
                }
            }
        }

        impl From<Vec<$t>> for $T {
            fn from(values: Vec<$t>) -> Self {
                let mut into = Self::new();
                for value in values {
                    into.push(Some(value));
                }
                into
            }
        }

        impl From<Vec<Option<$t>>> for $T {
            fn from(values: Vec<Option<$t>>) -> Self {
                let mut into = Self::new();
                for value in values {
                    into.push(value);
                }
                into
            }
        }
    };
}

primitive_ops!(I32Array, i32);
primitive_ops!(I64Array, i64);
primitive_ops!(U64Array, u64);
primitive_ops!(F64Array, f64);
primitive_ops!(DateArray, i32);
primitive_ops!(TimestampArray, i64);

macro_rules! math_ops {
    ($T:ty, $t:ty) => {
        impl $T {
            pub fn divide(&self, other: &Self) -> Self {
                todo!()
            }

            pub fn multiply(&self, other: &Self) -> Self {
                todo!()
            }

            pub fn add(&self, other: &Self) -> Self {
                todo!()
            }

            pub fn subtract(&self, other: &Self) -> Self {
                todo!()
            }

            pub fn divide_scalar(&self, other: Option<$t>) -> Self {
                todo!()
            }

            pub fn multiply_scalar(&self, other: Option<$t>) -> Self {
                todo!()
            }

            pub fn add_scalar(&self, other: Option<$t>) -> Self {
                todo!()
            }

            pub fn subtract_scalar(&self, other: Option<$t>) -> Self {
                todo!()
            }
        }
    };
}

math_ops!(I64Array, i32);
math_ops!(F64Array, i32);

impl U64Array {
    pub fn hash_all(columns: &Vec<Array>) -> Self {
        let mut seeds = U64Array::zeros(columns[0].len());
        for column in columns {
            column.hash(&mut seeds);
        }
        seeds
    }
}
