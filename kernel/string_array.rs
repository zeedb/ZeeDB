use crate::{bitmask::*, bool_array::*, primitive_array::*, Array};
use regex::Regex;
use std::{cmp::Ordering, ops::Range};
use twox_hash::xxh3;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StringArray {
    buffer: String,
    offsets: Vec<i32>,
    is_valid: Bitmask,
}

impl StringArray {
    // Constructors.

    pub fn new() -> Self {
        Self {
            buffer: String::new(),
            offsets: vec![0],
            is_valid: Bitmask::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        const STRING_LEN_ESTIMATE: usize = 10;
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(0);
        Self {
            offsets,
            buffer: String::with_capacity(capacity * STRING_LEN_ESTIMATE),
            is_valid: Bitmask::with_capacity(capacity),
        }
    }

    pub fn from_slice(values: &str, offsets: &[i32], is_valid: BitSlice) -> Self {
        assert_eq!(offsets.len() - 1, is_valid.len());

        Self {
            buffer: values.to_string(),
            offsets: offsets.to_vec(),
            is_valid: Bitmask::from_slice(is_valid),
        }
    }

    pub fn nulls(len: usize) -> Self {
        Self {
            offsets: vec![0].repeat(len + 1),
            buffer: "".to_string(),
            is_valid: Bitmask::falses(len),
        }
    }

    pub fn cat(arrays: Vec<Self>) -> Self {
        let mut builder = Self::with_capacity(arrays.iter().map(|a| a.len()).sum());
        for array in arrays {
            builder.extend(&array);
        }
        builder
    }

    pub fn concat(arrays: Vec<Self>) -> Self {
        for rest in &arrays[1..] {
            assert_eq!(arrays[0].len(), rest.len());
        }
        let mut builder = Self::with_capacity(arrays.iter().map(|a| a.len()).sum());
        for i in 0..arrays[0].len() {
            let mut valid = true;
            for j in 0..arrays.len() {
                if let Some(next) = arrays[j].get(i) {
                    // TODO this can push junk data.
                    builder.buffer.push_str(next);
                } else {
                    valid = false;
                }
            }
            builder.is_valid.push(valid);
            builder.offsets.push(builder.buffer.len() as i32);
        }
        builder
    }

    // Basic container operations.

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn byte_len(&self) -> usize {
        *self.offsets.last().unwrap_or(&0) as usize
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

    pub fn slice(&self, range: Range<usize>) -> Self {
        let start = self.offsets[range.start];
        let end = self.offsets[range.end + 1];
        let buffer = self.buffer[start as usize..end as usize].to_string();
        let offsets = self.offsets[range.start..range.end + 1]
            .iter()
            .map(|offset| *offset - start)
            .collect();
        let is_valid = Bitmask::from_slice(self.is_valid.slice(range.start..range.end));
        Self {
            buffer,
            offsets,
            is_valid,
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

    pub fn extend(&mut self, other: &Self) {
        for i in 0..other.len() {
            self.push(other.get(i))
        }
    }

    pub fn repeat(&self, n: usize) -> Self {
        let mut builder = Self::with_capacity(self.len() * n);
        for _ in 0..n {
            builder.extend(self);
        }
        builder
    }

    // Complex vector operations.

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
        assert_eq!(self.len(), indexes.len());

        let mut invert = vec![None].repeat(into.len());
        for i in 0..self.len() {
            if let Some(j) = indexes.get(i) {
                invert[j as usize] = Some(i);
            }
        }

        let mut builder = Self::with_capacity(into.len());
        for i in 0..invert.len() {
            if let Some(j) = invert[i] {
                builder.push(self.get(j));
            } else {
                builder.push(None);
            }
        }

        *into = builder
    }

    pub fn transpose(&self, stride: usize) -> Self {
        // The transpose of the empty matrix is the empty matrix.
        if self.len() == 0 {
            return self.clone();
        }
        // Check that stride makes sense.
        assert_eq!(self.len() % stride, 0);
        // Reorganize the array.
        let mut builder = Self::with_capacity(self.len());
        for i in 0..stride {
            for j in 0..self.len() / stride {
                builder.push(self.get(j * stride + i));
            }
        }
        builder
    }

    pub fn sort(&self) -> I32Array {
        let mut indexes: Vec<_> = (0..self.len() as i32).collect();
        indexes.sort_by(|i, j| self.cmp(*i as usize, *j as usize));
        I32Array::from(indexes)
    }

    // Array comparison operators.

    pub fn is(&self, other: &Self) -> BoolArray {
        let mut builder = BoolArray::with_capacity(self.len());
        for i in 0..self.len() {
            builder.push(Some(self.get(i) == other.get(i)))
        }
        builder
    }

    pub fn equal(&self, other: &Self) -> BoolArray {
        array_comparison_operator!(self, other, left, right, left == right)
    }

    pub fn not_equal(&self, other: &Self) -> BoolArray {
        array_comparison_operator!(self, other, left, right, left != right)
    }

    pub fn less(&self, other: &Self) -> BoolArray {
        array_comparison_operator!(self, other, left, right, left < right)
    }

    pub fn less_equal(&self, other: &Self) -> BoolArray {
        array_comparison_operator!(self, other, left, right, left <= right)
    }

    pub fn greater(&self, other: &Self) -> BoolArray {
        array_comparison_operator!(self, other, left, right, left > right)
    }

    pub fn greater_equal(&self, other: &Self) -> BoolArray {
        array_comparison_operator!(self, other, left, right, left >= right)
    }

    // Scalar comparison operators.

    pub fn is_scalar(&self, other: Option<&str>) -> BoolArray {
        let mut builder = BoolArray::with_capacity(self.len());
        for i in 0..self.len() {
            builder.push(Some(self.get(i) == other))
        }
        builder
    }

    pub fn equal_scalar(&self, other: Option<&str>) -> BoolArray {
        scalar_comparison_operator!(self, other, left, right, left == right)
    }

    pub fn less_scalar(&self, other: Option<&str>) -> BoolArray {
        scalar_comparison_operator!(self, other, left, right, left < right)
    }

    pub fn less_equal_scalar(&self, other: Option<&str>) -> BoolArray {
        scalar_comparison_operator!(self, other, left, right, left <= right)
    }

    pub fn greater_scalar(&self, other: Option<&str>) -> BoolArray {
        scalar_comparison_operator!(self, other, left, right, left > right)
    }

    pub fn greater_equal_scalar(&self, other: Option<&str>) -> BoolArray {
        scalar_comparison_operator!(self, other, left, right, left >= right)
    }

    pub fn is_null(&self) -> BoolArray {
        let mut builder = BoolArray::with_capacity(self.len());
        for i in 0..self.len() {
            builder.push(Some(self.get(i).is_none()))
        }
        builder
    }

    pub fn coalesce(&self, other: &Self) -> Self {
        assert_eq!(self.len(), other.len());

        let mut builder = Self::with_capacity(self.len());
        for i in 0..self.len() {
            match (self.get(i), other.get(i)) {
                (Some(left), _) => builder.push(Some(left)),
                (_, Some(right)) => builder.push(Some(right)),
                (None, None) => builder.push(None),
            }
        }
        builder
    }

    pub fn null_if(&self, other: &Self) -> Self {
        assert_eq!(self.len(), other.len());

        let mut builder = Self::with_capacity(self.len());
        for i in 0..self.len() {
            match (self.get(i), other.get(i)) {
                (Some(left), Some(right)) if left == right => builder.push(None),
                (Some(left), _) => builder.push(Some(left)),
                (_, _) => builder.push(None),
            }
        }
        builder
    }

    // Support operations for data structures.

    pub fn cmp(&self, i: usize, j: usize) -> Ordering {
        self.get(i).partial_cmp(&self.get(j)).unwrap()
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

    // Casts.

    pub fn cast_bool(&self) -> BoolArray {
        cast_operator!(self, value, value.parse::<bool>().unwrap(), BoolArray)
    }

    pub fn cast_i64(&self) -> I64Array {
        cast_operator!(self, value, value.parse::<i64>().unwrap(), I64Array)
    }

    pub fn cast_f64(&self) -> F64Array {
        cast_operator!(self, value, value.parse::<f64>().unwrap(), F64Array)
    }

    pub fn cast_date(&self) -> DateArray {
        cast_operator!(self, value, crate::dates::parse_date(value), DateArray)
    }

    pub fn cast_timestamp(&self) -> TimestampArray {
        cast_operator!(
            self,
            value,
            crate::dates::parse_timestamp(value),
            TimestampArray
        )
    }

    pub fn as_array(self) -> Array {
        Array::String(self)
    }
}

// operator_support!(StringArray, String, &str);

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
