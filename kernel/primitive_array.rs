use crate::{any_array::*, bitmask::*, bool_array::*, string_array::*};
use std::{cmp::Ordering, ops::Range};
use twox_hash::xxh3;

#[derive(Debug, Clone)]
pub struct I32Array {
    values: Vec<i32>,
    is_valid: Bitmask,
}

#[derive(Debug, Clone)]
pub struct I64Array {
    values: Vec<i64>,
    is_valid: Bitmask,
}

#[derive(Debug, Clone)]
pub struct U64Array {
    values: Vec<u64>,
    is_valid: Bitmask,
}

#[derive(Debug, Clone)]
pub struct F64Array {
    values: Vec<f64>,
    is_valid: Bitmask,
}

#[derive(Debug, Clone)]
pub struct DateArray {
    values: Vec<i32>,
    is_valid: Bitmask,
}

#[derive(Debug, Clone)]
pub struct TimestampArray {
    values: Vec<i64>,
    is_valid: Bitmask,
}

macro_rules! primitive_ops {
    ($T:ty, $t:ty) => {
        impl $T {
            // Constructors.

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

            pub fn from_slice(values: &[$t], is_valid: BitSlice) -> Self {
                assert_eq!(values.len(), is_valid.len());

                Self {
                    values: values.to_vec(),
                    is_valid: Bitmask::from_slice(is_valid),
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

            pub fn cat(arrays: Vec<Self>) -> Self {
                let mut builder = Self::with_capacity(arrays.iter().map(|a| a.len()).sum());
                for array in arrays {
                    builder.extend(&array);
                }
                builder
            }

            // Basic container operations.

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

            pub fn slice(&self, range: Range<usize>) -> Self {
                Self::from_slice(
                    &self.values[range.start..range.end],
                    self.is_valid.slice(range.start..range.end),
                )
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

            pub fn extend(&mut self, other: &Self) {
                self.values.extend_from_slice(&other.values);
                self.is_valid.extend(&other.is_valid);
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

            pub fn is_scalar(&self, other: Option<$t>) -> BoolArray {
                let mut builder = BoolArray::with_capacity(self.len());
                for i in 0..self.len() {
                    builder.push(Some(self.get(i) == other))
                }
                builder
            }

            pub fn equal_scalar(&self, other: Option<$t>) -> BoolArray {
                scalar_comparison_operator!(self, other, left, right, left == right)
            }

            pub fn less_scalar(&self, other: Option<$t>) -> BoolArray {
                scalar_comparison_operator!(self, other, left, right, left < right)
            }

            pub fn less_equal_scalar(&self, other: Option<$t>) -> BoolArray {
                scalar_comparison_operator!(self, other, left, right, left <= right)
            }

            pub fn greater_scalar(&self, other: Option<$t>) -> BoolArray {
                scalar_comparison_operator!(self, other, left, right, left > right)
            }

            pub fn greater_equal_scalar(&self, other: Option<$t>) -> BoolArray {
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
            pub fn minus(&self) -> Self {
                array_unary_operator!(self, value, -value)
            }

            pub fn divide(&self, other: &Self) -> Self {
                array_binary_operator!(self, other, left, right, left / right)
            }

            pub fn multiply(&self, other: &Self) -> Self {
                array_binary_operator!(self, other, left, right, left * right)
            }

            pub fn add(&self, other: &Self) -> Self {
                array_binary_operator!(self, other, left, right, left + right)
            }

            pub fn subtract(&self, other: &Self) -> Self {
                array_binary_operator!(self, other, left, right, left - right)
            }

            pub fn divide_scalar(&self, other: Option<$t>) -> Self {
                scalar_binary_operator!(self, other, left, right, left / right)
            }

            pub fn multiply_scalar(&self, other: Option<$t>) -> Self {
                scalar_binary_operator!(self, other, left, right, left * right)
            }

            pub fn add_scalar(&self, other: Option<$t>) -> Self {
                scalar_binary_operator!(self, other, left, right, left + right)
            }

            pub fn subtract_scalar(&self, other: Option<$t>) -> Self {
                scalar_binary_operator!(self, other, left, right, left - right)
            }
        }
    };
}

math_ops!(I64Array, i64);
math_ops!(F64Array, f64);

impl I32Array {
    pub fn conflict(&self, mask: &BoolArray, len: usize) -> bool {
        // Strategy for SIMD implementation:
        // Allocate an array of ids.
        // Scatter and gather the ids using self indexes.
        // If there is a conflict, some ids will collide during this process.
        let mut histogram = Bitmask::falses(len);
        for i in 0..self.len() {
            if mask.get(i) == Some(true) {
                if let Some(j) = self.get(i) {
                    if histogram.get(j as usize) {
                        return true;
                    } else {
                        histogram.set(j as usize, true);
                    }
                }
            }
        }
        false
    }
}

impl U64Array {
    pub fn hash_all(columns: &Vec<Array>) -> Self {
        let mut seeds = U64Array::zeros(columns[0].len());
        for column in columns {
            column.hash(&mut seeds);
        }
        seeds
    }
}

// Casts.

impl I64Array {
    pub fn cast_bool(&self) -> BoolArray {
        cast_operator!(self, value, value != 0, BoolArray)
    }

    pub fn cast_f64(&self) -> F64Array {
        cast_operator!(self, value, value as f64, F64Array)
    }

    pub fn cast_string(&self) -> StringArray {
        cast_operator!(self, value, value.to_string().as_str(), StringArray)
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
        cast_operator!(self, value, value.to_string().as_str(), StringArray)
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
            crate::dates::date(value).format("%F").to_string().as_str(),
            StringArray
        )
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
            crate::dates::timestamp(value)
                .format("%+")
                .to_string()
                .as_str(),
            StringArray
        )
    }
}
