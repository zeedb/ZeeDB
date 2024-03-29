use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::{
    AnyArray, Array, BitSlice, Bitmask, DataType, DateArray, F64Array, I64Array, StringArray,
    TimestampArray,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct BoolArray {
    values: Bitmask,
    is_valid: Bitmask,
}

impl BoolArray {
    // Constructors.

    pub fn from_slice(values: BitSlice, is_valid: BitSlice) -> Self {
        Self {
            values: Bitmask::from_slice(values),
            is_valid: Bitmask::from_slice(is_valid),
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

    pub fn blend(&self, if_true: &AnyArray, if_false_or_null: &AnyArray) -> AnyArray {
        match (if_true, if_false_or_null) {
            (AnyArray::Bool(if_true), AnyArray::Bool(if_false)) => {
                let mut builder = BoolArray::with_capacity(self.len());
                for i in 0..self.len() {
                    match self.get(i) {
                        Some(true) => builder.push(if_true.get(i)),
                        _ => builder.push(if_false.get(i)),
                    }
                }
                AnyArray::Bool(builder)
            }
            (AnyArray::I64(if_true), AnyArray::I64(if_false)) => {
                let mut builder = I64Array::with_capacity(self.len());
                for i in 0..self.len() {
                    match self.get(i) {
                        Some(true) => builder.push(if_true.get(i)),
                        _ => builder.push(if_false.get(i)),
                    }
                }
                AnyArray::I64(builder)
            }
            (AnyArray::F64(if_true), AnyArray::F64(if_false)) => {
                let mut builder = F64Array::with_capacity(self.len());
                for i in 0..self.len() {
                    match self.get(i) {
                        Some(true) => builder.push(if_true.get(i)),
                        _ => builder.push(if_false.get(i)),
                    }
                }
                AnyArray::F64(builder)
            }
            (AnyArray::Date(if_true), AnyArray::Date(if_false)) => {
                let mut builder = DateArray::with_capacity(self.len());
                for i in 0..self.len() {
                    match self.get(i) {
                        Some(true) => builder.push(if_true.get(i)),
                        _ => builder.push(if_false.get(i)),
                    }
                }
                AnyArray::Date(builder)
            }
            (AnyArray::Timestamp(if_true), AnyArray::Timestamp(if_false)) => {
                let mut builder = TimestampArray::with_capacity(self.len());
                for i in 0..self.len() {
                    match self.get(i) {
                        Some(true) => builder.push(if_true.get(i)),
                        _ => builder.push(if_false.get(i)),
                    }
                }
                AnyArray::Timestamp(builder)
            }
            (AnyArray::String(if_true), AnyArray::String(if_false)) => {
                let mut builder = StringArray::with_capacity(self.len());
                for i in 0..self.len() {
                    match self.get(i) {
                        Some(true) => builder.push(if_true.get(i)),
                        _ => builder.push(if_false.get(i)),
                    }
                }
                AnyArray::String(builder)
            }
            (if_true, if_false) => panic!(
                "{} does not match {}",
                if_true.data_type(),
                if_false.data_type()
            ),
        }
    }

    pub fn blend_or_null(&self, if_true: &AnyArray, if_false: &AnyArray) -> AnyArray {
        match (if_true, if_false) {
            (AnyArray::Bool(if_true), AnyArray::Bool(if_false)) => {
                let mut builder = BoolArray::with_capacity(self.len());
                for i in 0..self.len() {
                    match self.get(i) {
                        Some(true) => builder.push(if_true.get(i)),
                        Some(false) => builder.push(if_false.get(i)),
                        None => builder.push(None),
                    }
                }
                AnyArray::Bool(builder)
            }
            (AnyArray::I64(if_true), AnyArray::I64(if_false)) => {
                let mut builder = I64Array::with_capacity(self.len());
                for i in 0..self.len() {
                    match self.get(i) {
                        Some(true) => builder.push(if_true.get(i)),
                        Some(false) => builder.push(if_false.get(i)),
                        None => builder.push(None),
                    }
                }
                AnyArray::I64(builder)
            }
            (AnyArray::F64(if_true), AnyArray::F64(if_false)) => {
                let mut builder = F64Array::with_capacity(self.len());
                for i in 0..self.len() {
                    match self.get(i) {
                        Some(true) => builder.push(if_true.get(i)),
                        Some(false) => builder.push(if_false.get(i)),
                        None => builder.push(None),
                    }
                }
                AnyArray::F64(builder)
            }
            (AnyArray::Date(if_true), AnyArray::Date(if_false)) => {
                let mut builder = DateArray::with_capacity(self.len());
                for i in 0..self.len() {
                    match self.get(i) {
                        Some(true) => builder.push(if_true.get(i)),
                        Some(false) => builder.push(if_false.get(i)),
                        None => builder.push(None),
                    }
                }
                AnyArray::Date(builder)
            }
            (AnyArray::Timestamp(if_true), AnyArray::Timestamp(if_false)) => {
                let mut builder = TimestampArray::with_capacity(self.len());
                for i in 0..self.len() {
                    match self.get(i) {
                        Some(true) => builder.push(if_true.get(i)),
                        Some(false) => builder.push(if_false.get(i)),
                        None => builder.push(None),
                    }
                }
                AnyArray::Timestamp(builder)
            }
            (AnyArray::String(if_true), AnyArray::String(if_false)) => {
                let mut builder = StringArray::with_capacity(self.len());
                for i in 0..self.len() {
                    match self.get(i) {
                        Some(true) => builder.push(if_true.get(i)),
                        Some(false) => builder.push(if_false.get(i)),
                        None => builder.push(None),
                    }
                }
                AnyArray::String(builder)
            }
            (if_true, if_false) => panic!(
                "{} does not match {}",
                if_true.data_type(),
                if_false.data_type()
            ),
        }
    }

    // Logical operators.

    pub fn not(&self) -> Self {
        array_unary_operator!(self, value, !value)
    }

    pub fn and(&self, other: &Self) -> Self {
        assert_eq!(self.len(), other.len());

        let mut result = Self::with_capacity(self.len());
        for i in 0..self.len() {
            result.push(match (self.get(i), other.get(i)) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                (_, _) => None,
            });
        }
        result
    }

    pub fn or(&self, other: &Self) -> Self {
        assert_eq!(self.len(), other.len());

        let mut result = Self::with_capacity(self.len());
        for i in 0..self.len() {
            result.push(match (self.get(i), other.get(i)) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                (_, _) => None,
            });
        }
        result
    }

    pub fn and_not(&self, other: &Self) -> Self {
        assert_eq!(self.len(), other.len());

        let mut result = Self::with_capacity(self.len());
        for i in 0..self.len() {
            result.push(match (self.get(i), other.get(i)) {
                (Some(false), _) | (_, Some(true)) => Some(false),
                (Some(true), Some(false)) => Some(true),
                (_, _) => None,
            });
        }
        result
    }

    // Logical reduction operators.

    fn logical_reduction(
        &self,
        stride: usize,
        default: bool,
        reduce: impl Fn(bool, bool) -> bool,
    ) -> Self {
        assert!(self.len() == 0 || self.len() % stride == 0);

        let mut result = if default {
            Self::trues(stride)
        } else {
            Self::falses(stride)
        };
        for i in 0..self.len() {
            let prev = result.get(i % stride).unwrap();
            if let Some(next) = self.get(i) {
                result.set(i % stride, Some(reduce(prev, next)));
            }
        }
        result
    }

    pub fn any(&self, stride: usize) -> Self {
        self.logical_reduction(stride, false, |prev, next| prev || next)
    }

    pub fn all(&self, stride: usize) -> Self {
        self.logical_reduction(stride, true, |prev, next| prev && next)
    }

    pub fn none(&self, stride: usize) -> Self {
        self.logical_reduction(stride, true, |prev, next| prev && !next)
    }

    pub fn count(&self, stride: usize) -> I64Array {
        assert!(self.len() == 0 || self.len() % stride == 0);

        if self.len() == 0 {
            return I64Array::zeros(stride);
        }

        let mut builder = I64Array::zeros(stride);
        for i in 0..stride {
            for j in 0..self.len() / stride {
                if let Some(true) = self.get(j * stride + i) {
                    let prev = builder.get(i).unwrap();
                    builder.set(i, Some(prev + 1));
                }
            }
        }

        builder
    }

    // Casts.

    pub fn cast_i64(&self) -> I64Array {
        cast_operator!(self, value, if value { 1 } else { 0 }, I64Array)
    }

    pub fn cast_f64(&self) -> F64Array {
        cast_operator!(self, value, if value { 1.0 } else { 0.0 }, F64Array)
    }

    pub fn cast_string(&self) -> StringArray {
        cast_to_string!(
            self,
            value,
            if value { "true" } else { "false" },
            StringArray
        )
    }
}

impl Array for BoolArray {
    type Element = bool;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Bitmask::with_capacity(capacity),
            is_valid: Bitmask::with_capacity(capacity),
        }
    }

    fn nulls(len: usize) -> Self {
        Self {
            values: Bitmask::falses(len),
            is_valid: Bitmask::falses(len),
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn get(&self, index: usize) -> Option<bool> {
        if index < self.is_valid.len() && self.is_valid.get(index) {
            Some(self.values.get(index))
        } else {
            None
        }
    }

    fn bytes(&self, index: usize) -> Option<&[u8]> {
        static TRUE: [u8; 1] = 1u8.to_ne_bytes();
        static FALSE: [u8; 1] = 1u8.to_ne_bytes();
        if let Some(value) = self.get(index) {
            if value {
                Some(&TRUE)
            } else {
                Some(&FALSE)
            }
        } else {
            None
        }
    }

    fn slice(&self, range: Range<usize>) -> Self {
        Self::from_slice(
            self.values.slice(range.start..range.end),
            self.is_valid.slice(range.start..range.end),
        )
    }

    fn set(&mut self, index: usize, value: Option<bool>) {
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

    fn push(&mut self, value: Option<bool>) {
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
        DataType::Bool
    }

    fn as_any(self) -> AnyArray {
        AnyArray::Bool(self)
    }
}
