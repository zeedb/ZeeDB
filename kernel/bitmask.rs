use serde::{Deserialize, Serialize};
use std::{fmt::Debug, ops::Range};

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Bitmask {
    values: Vec<u8>,
    len: usize,
}

pub struct BitSlice<'a> {
    values: &'a [u8],
    offset: usize,
    len: usize,
}

impl Bitmask {
    pub fn new() -> Self {
        Self {
            values: vec![],
            len: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity((capacity + 7) / 8),
            len: 0,
        }
    }

    pub fn from_slice(slice: BitSlice) -> Self {
        Self {
            values: slice.to_vec(),
            len: slice.len(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn falses(len: usize) -> Self {
        Self {
            values: vec![0].repeat((len + 7) / 8),
            len,
        }
    }

    pub fn trues(len: usize) -> Self {
        Self {
            values: vec![u8::MAX].repeat((len + 7) / 8),
            len,
        }
    }

    pub fn get(&self, index: usize) -> bool {
        get_bit(&self.values, index)
    }

    pub fn slice(&self, range: Range<usize>) -> BitSlice<'_> {
        assert!(range.start < self.len);
        assert!(range.end < self.len);

        BitSlice::from_slice(&self.values, range)
    }

    pub fn set(&mut self, index: usize, value: bool) {
        if value {
            set_bit(&mut self.values, index)
        } else {
            unset_bit(&mut self.values, index)
        }
    }

    pub fn push(&mut self, value: bool) {
        let index = self.len;
        self.len += 1;
        if self.values.len() * 8 < self.len {
            self.values.push(0)
        }
        self.set(index, value)
    }

    pub fn extend(&mut self, other: &Self) {
        for i in 0..other.len() {
            self.push(other.get(i))
        }
    }
}

impl<'a> BitSlice<'a> {
    pub fn from_slice(slice: &'a [u8], range: Range<usize>) -> Self {
        Self {
            values: slice,
            offset: range.start,
            len: range.end - range.start,
        }
    }

    pub fn get(&self, index: usize) -> bool {
        get_bit(&self.values, self.offset + index)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    fn to_vec(&self) -> Vec<u8> {
        let byte_offset = self.offset / 8;
        let byte_len = (self.len + 7) / 8;
        let bit_offset = self.offset % 8;
        let mut bytes = self.values[byte_offset..byte_offset + byte_len].to_vec();
        bytes.rotate_left(bit_offset);
        bytes
    }
}

impl Debug for Bitmask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[")?;
        for i in 0..self.len() {
            if self.get(i) {
                f.write_str("1")?;
            } else {
                f.write_str("0")?;
            }
            if i < self.len() - 1 {
                f.write_str(" ")?;
            }
        }
        f.write_str("]")?;
        Ok(())
    }
}

pub fn get_bit(data: &[u8], i: usize) -> bool {
    (data[i >> 3] & BIT_MASK[i & 7]) != 0
}

unsafe fn get_bit_raw(data: *const u8, i: usize) -> bool {
    (*data.add(i >> 3) & BIT_MASK[i & 7]) != 0
}

pub fn set_bit(data: &mut [u8], i: usize) {
    data[i >> 3] |= BIT_MASK[i & 7];
}

unsafe fn set_bit_raw(data: *mut u8, i: usize) {
    *data.add(i >> 3) |= BIT_MASK[i & 7];
}

pub fn unset_bit(data: &mut [u8], i: usize) {
    data[i >> 3] &= !BIT_MASK[i & 7];
}

unsafe fn unset_bit_raw(data: *mut u8, i: usize) {
    *data.add(i >> 3) &= !BIT_MASK[i & 7];
}

static BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
