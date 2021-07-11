use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::{AnyArray, Array, BitSlice, Bitmask, BoolArray, DataType, I32Array, StringArray};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct I64Array {
    values: Vec<i64>,
    is_valid: Bitmask,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct F64Array {
    values: Vec<f64>,
    is_valid: Bitmask,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct DateArray {
    values: Vec<i32>,
    is_valid: Bitmask,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct TimestampArray {
    values: Vec<i64>,
    is_valid: Bitmask,
}

macro_rules! impl_array {
    ($T:ty, $V:ident, $t:ty) => {
        impl Array for $T {
            type Element = $t;

            fn with_capacity(capacity: usize) -> Self {
                Self {
                    values: Vec::with_capacity(capacity),
                    is_valid: Bitmask::with_capacity(capacity),
                }
            }

            fn nulls(len: usize) -> Self {
                Self {
                    values: vec![Default::default()].repeat(len),
                    is_valid: Bitmask::falses(len),
                }
            }

            fn len(&self) -> usize {
                self.values.len()
            }

            fn get(&self, index: usize) -> Option<Self::Element> {
                if self.is_valid.get(index) {
                    Some(self.values[index])
                } else {
                    None
                }
            }

            fn bytes(&self, index: usize) -> Option<&[u8]> {
                if self.is_valid.get(index) {
                    Some(self.values[index].as_ne_bytes())
                } else {
                    None
                }
            }

            fn slice(&self, range: Range<usize>) -> Self {
                Self::from_slice(
                    &self.values[range.start..range.end],
                    self.is_valid.slice(range.start..range.end),
                )
            }

            fn set(&mut self, index: usize, value: Option<Self::Element>) {
                match value {
                    Some(value) => {
                        self.is_valid.set(index, true);
                        self.values[index] = value;
                    }
                    None => {
                        self.is_valid.set(index, false);
                        self.values[index] = Default::default();
                    }
                }
            }

            fn push(&mut self, value: Option<Self::Element>) {
                if let Some(value) = value {
                    self.is_valid.push(true);
                    self.values.push(value);
                } else {
                    self.is_valid.push(false);
                    self.values.push(Default::default());
                }
            }

            fn extend(&mut self, other: &Self) {
                self.values.extend(&other.values);
                self.is_valid.extend(&other.is_valid);
            }

            fn repeat(&self, n: usize) -> Self {
                let mut builder = Self::with_capacity(self.len() * n);
                for _ in 0..n {
                    builder.extend(self);
                }
                builder
            }

            fn data_type(&self) -> DataType {
                DataType::$V
            }

            fn as_any(self) -> AnyArray {
                AnyArray::$V(self)
            }
        }

        impl $T {
            pub fn zeros(len: usize) -> Self {
                Self {
                    values: vec![Default::default()].repeat(len),
                    is_valid: Bitmask::trues(len),
                }
            }

            pub fn from_slice(values: &[$t], is_valid: BitSlice) -> Self {
                assert_eq!(values.len(), is_valid.len());

                Self {
                    values: values.to_vec(),
                    is_valid: Bitmask::from_slice(is_valid),
                }
            }
        }
    };
}

impl_array!(I64Array, I64, i64);
impl_array!(F64Array, F64, f64);
impl_array!(DateArray, Date, i32);
impl_array!(TimestampArray, Timestamp, i64);

// Casts.

impl I64Array {
    pub fn cast_bool(&self) -> BoolArray {
        cast_operator!(self, value, value != 0, BoolArray)
    }

    pub fn cast_f64(&self) -> F64Array {
        cast_operator!(self, value, value as f64, F64Array)
    }

    pub fn cast_string(&self) -> StringArray {
        cast_operator!(self, value, value.to_string(), StringArray)
    }

    pub fn as_any(self) -> AnyArray {
        AnyArray::I64(self)
    }
}

impl F64Array {
    pub fn cast_bool(&self) -> BoolArray {
        cast_operator!(self, value, value != 0.0, BoolArray)
    }

    pub fn cast_i64(&self) -> I64Array {
        cast_operator!(self, value, value as i64, I64Array)
    }

    pub fn cast_string(&self) -> StringArray {
        cast_operator!(self, value, value.to_string(), StringArray)
    }

    pub fn as_any(self) -> AnyArray {
        AnyArray::F64(self)
    }
}

impl DateArray {
    pub fn cast_timestamp(&self) -> TimestampArray {
        cast_operator!(
            self,
            value,
            crate::dates::cast_date_as_timestamp(value),
            TimestampArray
        )
    }

    pub fn cast_string(&self) -> StringArray {
        cast_operator!(
            self,
            value,
            crate::dates::date(value).format("%F").to_string(),
            StringArray
        )
    }

    pub fn as_any(self) -> AnyArray {
        AnyArray::Date(self)
    }
}

impl TimestampArray {
    pub fn cast_date(&self) -> DateArray {
        cast_operator!(
            self,
            value,
            crate::dates::cast_timestamp_as_date(value),
            DateArray
        )
    }

    pub fn cast_string(&self) -> StringArray {
        cast_operator!(
            self,
            value,
            crate::dates::timestamp(value).format("%+").to_string(),
            StringArray
        )
    }

    pub fn as_any(self) -> AnyArray {
        AnyArray::Timestamp(self)
    }
}

// Float equals.

impl Eq for F64Array {}
impl PartialEq for F64Array {
    fn eq(&self, other: &Self) -> bool {
        self.is_valid == other.is_valid && float_equals(&self.values, &other.values)
    }
}

fn float_equals(left: &Vec<f64>, right: &Vec<f64>) -> bool {
    if left.len() != right.len() {
        return false;
    }
    for i in 0..left.len() {
        if left[i] != right[i] {
            return false;
        }
    }
    true
}

impl I64Array {
    pub fn hash_all(columns: &Vec<AnyArray>) -> Self {
        let mut seeds = I64Array::zeros(columns[0].len());
        for column in columns {
            column.hash(&mut seeds);
        }
        seeds
    }
}
