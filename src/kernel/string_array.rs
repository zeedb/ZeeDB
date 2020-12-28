use crate::bitmask::*;
use crate::bool_array::*;
use crate::primitive_array::*;
use std::cmp::Ordering;
use twox_hash::xxh3;

#[derive(Clone)]
pub struct StringArray {
    buffer: String,
    offsets: Vec<i32>,
    is_valid: Bitmask,
}

impl StringArray {
    pub fn new() -> Self {
        Self {
            buffer: String::new(),
            offsets: vec![0],
            is_valid: Bitmask::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        const STRING_LEN_ESTIMATE: usize = 10;
        Self {
            offsets: Vec::with_capacity(capacity + 1),
            buffer: String::with_capacity(capacity * STRING_LEN_ESTIMATE),
            is_valid: Bitmask::with_capacity(capacity),
        }
    }

    pub fn nulls(len: usize) -> Self {
        Self {
            offsets: vec![0].repeat(len + 1),
            buffer: "".to_string(),
            is_valid: Bitmask::falses(len),
        }
    }

    pub fn from_slice(values: &str, offsets: &[i32], is_valid: &[u8]) -> Self {
        assert_eq!((offsets.len() - 1 + 7) / 8, is_valid.len());

        Self {
            buffer: values.to_string(),
            offsets: offsets.to_vec(),
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
        self.offsets.len() - 1
    }

    pub fn byte_len(&self) -> usize {
        *self.offsets.last().unwrap_or(&0) as usize
    }

    pub fn gather(&self, indexes: &I32Array) -> Self {
        let mut into = Self::new();
        for i in 0..indexes.len() {
            if let Some(j) = indexes.get(i) {
                into.push(self.get(j as usize));
            } else {
                into.push(None);
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

    pub fn is_scalar(&self, other: Option<&str>) -> BoolArray {
        todo!()
    }

    pub fn equal_scalar(&self, other: Option<&str>) -> BoolArray {
        todo!()
    }

    pub fn less_scalar(&self, other: Option<&str>) -> BoolArray {
        todo!()
    }

    pub fn less_equal_scalar(&self, other: Option<&str>) -> BoolArray {
        todo!()
    }

    pub fn greater_scalar(&self, other: Option<&str>) -> BoolArray {
        todo!()
    }

    pub fn greater_equal_scalar(&self, other: Option<&str>) -> BoolArray {
        todo!()
    }

    pub fn is_null(&self) -> BoolArray {
        todo!()
    }

    pub fn get(&self, index: usize) -> Option<&str> {
        if self.is_valid.get(index) {
            let begin = self.offsets[index] as usize;
            let end = self.offsets[index + 1] as usize;
            Some(self.buffer.get(begin..end).unwrap())
        } else {
            None
        }
    }

    pub fn push(&mut self, value: Option<&str>) {
        if let Some(value) = value {
            self.buffer.push_str(value);
            self.is_valid.push(true);
            self.offsets.push(self.buffer.len() as i32);
        } else {
            self.is_valid.push(false);
            self.offsets.push(self.buffer.len() as i32);
        }
    }

    pub fn hash(&self, state: &mut U64Array) {
        for i in 0..self.len() {
            if let Some(value) = self.get(i) {
                state.set(
                    i,
                    Some(xxh3::hash64_with_seed(
                        value.as_bytes(),
                        state.get(i).unwrap(),
                    )),
                )
            }
        }
    }
}

impl From<Vec<&str>> for StringArray {
    fn from(values: Vec<&str>) -> Self {
        let mut into = Self::new();
        for value in values {
            into.push(Some(value));
        }
        into
    }
}

impl From<Vec<Option<&str>>> for StringArray {
    fn from(values: Vec<Option<&str>>) -> Self {
        let mut into = Self::new();
        for value in values {
            into.push(value);
        }
        into
    }
}
