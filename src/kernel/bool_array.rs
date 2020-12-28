use crate::any_array::*;
use crate::bitmask::*;
use crate::primitive_array::*;
use std::cmp::Ordering;
use twox_hash::xxh3;

#[derive(Clone, Debug)]
pub struct BoolArray {
    values: Bitmask,
    is_valid: Bitmask,
}

impl BoolArray {
    pub fn new() -> Self {
        Self {
            values: Bitmask::new(),
            is_valid: Bitmask::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Bitmask::with_capacity(capacity),
            is_valid: Bitmask::with_capacity(capacity),
        }
    }

    pub fn trues(len: usize) -> Self {
        Self {
            values: Bitmask::trues(len),
            is_valid: Bitmask::trues(len),
        }
    }

    pub fn falses(len: usize) -> Self {
        Self {
            values: Bitmask::falses(len),
            is_valid: Bitmask::trues(len),
        }
    }

    pub fn nulls(len: usize) -> Self {
        Self {
            values: Bitmask::falses(len),
            is_valid: Bitmask::falses(len),
        }
    }

    pub fn from_slice(values: &[u8], is_valid: &[u8], len: usize) -> Self {
        Self {
            values: Bitmask::from_slice(values, len),
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

    pub fn get(&self, index: usize) -> Option<bool> {
        if self.is_valid.get(index) {
            Some(self.values.get(index))
        } else {
            None
        }
    }

    pub fn set(&mut self, index: usize, value: Option<bool>) {
        match value {
            Some(value) => {
                self.is_valid.set(index, true);
                self.values.set(index, value);
            }
            None => {
                self.is_valid.set(index, false);
                self.values.set(index, false);
            }
        }
    }

    pub fn push(&mut self, value: Option<bool>) {
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

    pub fn select(&self, if_true: &Array, if_false: &Array) -> Array {
        todo!()
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

    pub fn and(&self, other: &Self) -> Self {
        todo!()
    }

    pub fn or(&self, other: &Self) -> Self {
        todo!()
    }

    pub fn not(&self) -> Self {
        todo!()
    }

    pub fn and_not(&self, other: &Self) -> Self {
        todo!()
    }

    pub fn any(&self, stride: usize) -> Self {
        todo!()
    }

    pub fn all(&self, stride: usize) -> Self {
        todo!()
    }

    pub fn none(&self, stride: usize) -> Self {
        todo!()
    }

    pub fn is(&self, other: &Self) -> BoolArray {
        todo!()
    }

    pub fn equal(&self, other: &Self) -> BoolArray {
        todo!()
    }

    pub fn not_equal(&self, other: &Self) -> BoolArray {
        todo!()
    }

    pub fn less(&self, other: &Self) -> BoolArray {
        todo!()
    }

    pub fn less_equal(&self, other: &Self) -> BoolArray {
        todo!()
    }

    pub fn greater(&self, other: &Self) -> BoolArray {
        todo!()
    }

    pub fn greater_equal(&self, other: &Self) -> BoolArray {
        todo!()
    }

    pub fn is_scalar(&self, other: Option<bool>) -> BoolArray {
        todo!()
    }

    pub fn equal_scalar(&self, other: Option<bool>) -> BoolArray {
        todo!()
    }

    pub fn less_scalar(&self, other: Option<bool>) -> BoolArray {
        todo!()
    }

    pub fn less_equal_scalar(&self, other: Option<bool>) -> BoolArray {
        todo!()
    }

    pub fn greater_scalar(&self, other: Option<bool>) -> BoolArray {
        todo!()
    }

    pub fn greater_equal_scalar(&self, other: Option<bool>) -> BoolArray {
        todo!()
    }

    pub fn is_null(&self) -> BoolArray {
        todo!()
    }

    pub fn hash(&self, state: &mut U64Array) {
        for i in 0..self.len() {
            if let Some(value) = self.get(i) {
                if value {
                    state.set(
                        i,
                        Some(xxh3::hash64_with_seed(&[1u8], state.get(i).unwrap())),
                    )
                } else {
                    state.set(
                        i,
                        Some(xxh3::hash64_with_seed(&[0u8], state.get(i).unwrap())),
                    )
                }
            }
        }
    }
}

impl From<Vec<bool>> for BoolArray {
    fn from(values: Vec<bool>) -> Self {
        let mut into = Self::new();
        for value in values {
            into.push(Some(value));
        }
        into
    }
}

impl From<Vec<Option<bool>>> for BoolArray {
    fn from(values: Vec<Option<bool>>) -> Self {
        let mut into = Self::new();
        for value in values {
            into.push(value);
        }
        into
    }
}
