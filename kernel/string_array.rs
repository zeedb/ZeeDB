use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::{
    AnyArray, Array, BitSlice, Bitmask, BoolArray, DataType, DateArray, F64Array, I32Array,
    I64Array, TimestampArray,
};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct StringArray {
    buffer: String,
    offsets: Vec<i32>,
    is_valid: Bitmask,
}

impl StringArray {
    // Constructors.

    pub fn from_slice(values: &str, offsets: &[i32], is_valid: BitSlice) -> Self {
        assert_eq!(offsets.len() - 1, is_valid.len());

        Self {
            buffer: values.to_string(),
            offsets: offsets.to_vec(),
            is_valid: Bitmask::from_slice(is_valid),
        }
    }

    pub fn concat(arrays: Vec<Self>) -> Self {
        for rest in &arrays[1..] {
            assert_eq!(arrays[0].len(), rest.len());
        }
        let mut builder = Self::with_capacity(arrays.iter().map(|a| a.len()).sum());
        for i in 0..arrays[0].len() {
            let mut valid = true;
            for j in 0..arrays.len() {
                if let Some(next) = arrays[j].get_str(i) {
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

    pub fn from_str_values(values: Vec<&str>) -> Self {
        let mut into = Self::default();
        for value in values {
            into.push_str(Some(value));
        }
        into
    }

    pub fn from_str_options(values: Vec<Option<&str>>) -> Self {
        let mut into = Self::default();
        for value in values {
            into.push_str(value);
        }
        into
    }

    pub fn get_str(&self, index: usize) -> Option<&str> {
        if index < self.is_valid.len() && self.is_valid.get(index) {
            let begin = self.offsets[index] as usize;
            let end = self.offsets[index + 1] as usize;
            Some(&self.buffer[begin..end])
        } else {
            None
        }
    }

    pub fn push_str(&mut self, value: Option<&str>) {
        if let Some(value) = value {
            self.buffer.push_str(value);
            self.is_valid.push(true);
            self.offsets.push(self.buffer.len() as i32);
        } else {
            self.is_valid.push(false);
            self.offsets.push(self.buffer.len() as i32);
        }
    }

    pub fn byte_len(&self) -> usize {
        *self.offsets.last().unwrap_or(&0) as usize
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
        I32Array::from_values(indexes)
    }

    // Casts.

    pub fn cast_bool(&self) -> BoolArray {
        cast_from_string!(self, value, value.parse::<bool>().unwrap(), BoolArray)
    }

    pub fn cast_i64(&self) -> I64Array {
        cast_from_string!(self, value, value.parse::<i64>().unwrap(), I64Array)
    }

    pub fn cast_f64(&self) -> F64Array {
        cast_from_string!(self, value, value.parse::<f64>().unwrap(), F64Array)
    }

    pub fn cast_date(&self) -> DateArray {
        cast_from_string!(self, value, crate::dates::parse_date(value), DateArray)
    }

    pub fn cast_timestamp(&self) -> TimestampArray {
        cast_from_string!(
            self,
            value,
            crate::dates::parse_timestamp(value),
            TimestampArray
        )
    }
}

impl Default for StringArray {
    fn default() -> Self {
        Self {
            buffer: String::default(),
            offsets: vec![0],
            is_valid: Bitmask::default(),
        }
    }
}

impl Array for StringArray {
    type Element = String;

    fn with_capacity(capacity: usize) -> Self {
        const STRING_LEN_ESTIMATE: usize = 10;
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(0);
        Self {
            offsets,
            buffer: String::with_capacity(capacity * STRING_LEN_ESTIMATE),
            is_valid: Bitmask::with_capacity(capacity),
        }
    }

    fn nulls(len: usize) -> Self {
        Self {
            offsets: vec![0].repeat(len + 1),
            buffer: "".to_string(),
            is_valid: Bitmask::falses(len),
        }
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn get(&self, index: usize) -> Option<Self::Element> {
        if index < self.is_valid.len() && self.is_valid.get(index) {
            let begin = self.offsets[index] as usize;
            let end = self.offsets[index + 1] as usize;
            Some(self.buffer[begin..end].to_string())
        } else {
            None
        }
    }

    fn bytes(&self, index: usize) -> Option<&[u8]> {
        if index < self.is_valid.len() && self.is_valid.get(index) {
            let begin = self.offsets[index] as usize;
            let end = self.offsets[index + 1] as usize;
            Some(self.buffer[begin..end].as_bytes())
        } else {
            None
        }
    }

    fn slice(&self, range: Range<usize>) -> Self {
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

    fn push(&mut self, value: Option<Self::Element>) {
        if let Some(value) = value {
            self.buffer.push_str(value.as_str());
            self.is_valid.push(true);
            self.offsets.push(self.buffer.len() as i32);
        } else {
            self.is_valid.push(false);
            self.offsets.push(self.buffer.len() as i32);
        }
    }

    fn set(&mut self, _index: usize, _value: Option<Self::Element>) {
        panic!("set is not available for string type")
    }

    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn as_any(self) -> AnyArray {
        AnyArray::String(self)
    }
}
