use crate::{
    AnyArray, Array, BitSlice, Bitmask, BoolArray, DataType, DateArray, F64Array, I32Array,
    I64Array, TimestampArray,
};
use serde::{Deserialize, Serialize};
use std::ops::Range;

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
}

impl<'a> Array<'a> for StringArray {
    type Element = &'a str;

    fn new() -> Self {
        Self {
            buffer: String::new(),
            offsets: vec![0],
            is_valid: Bitmask::new(),
        }
    }

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

    fn get(&'a self, index: usize) -> Option<Self::Element> {
        if self.is_valid.get(index) {
            let begin = self.offsets[index] as usize;
            let end = self.offsets[index + 1] as usize;
            Some(&self.buffer[begin..end])
        } else {
            None
        }
    }

    fn bytes(&self, index: usize) -> Option<&[u8]> {
        if self.is_valid.get(index) {
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
            self.buffer.push_str(value);
            self.is_valid.push(true);
            self.offsets.push(self.buffer.len() as i32);
        } else {
            self.is_valid.push(false);
            self.offsets.push(self.buffer.len() as i32);
        }
    }

    fn set(&mut self, index: usize, value: Option<Self::Element>) {
        panic!("set is not available for string type")
    }

    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn as_any(self) -> AnyArray {
        AnyArray::String(self)
    }
}
