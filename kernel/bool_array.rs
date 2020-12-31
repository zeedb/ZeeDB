use crate::{any_array::*, bitmask::*, primitive_array::*, string_array::*};
use std::{cmp::Ordering, ops::Range};
use twox_hash::xxh3;

#[derive(Clone, Debug)]
pub struct BoolArray {
    values: Bitmask,
    is_valid: Bitmask,
}

macro_rules! logical_reduction {
    ($self:ident, $stride:ident, $default:literal, $prev:ident, $next:ident, $op:expr) => {{
        assert_eq!($self.len() % $stride, 0);

        if $self.len() == 0 {
            return Self::from(vec![$default].repeat($stride));
        }

        let mut builder = Self::nulls($stride);
        for i in 0..$stride {
            for j in 0..$self.len() / $stride {
                if let Some($next) = $self.get(j * $stride + i) {
                    let $prev = builder.get(i).unwrap_or($default);
                    builder.set(i, Some($op));
                }
            }
        }

        builder
    }};
}

impl BoolArray {
    // Constructors.

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

    pub fn from_slice(values: BitSlice, is_valid: BitSlice) -> Self {
        Self {
            values: Bitmask::from_slice(values),
            is_valid: Bitmask::from_slice(is_valid),
        }
    }

    pub fn nulls(len: usize) -> Self {
        Self {
            values: Bitmask::falses(len),
            is_valid: Bitmask::falses(len),
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

    pub fn get(&self, index: usize) -> Option<bool> {
        if self.is_valid.get(index) {
            Some(self.values.get(index))
        } else {
            None
        }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        Self::from_slice(
            self.values.slice(range.start..range.end),
            self.is_valid.slice(range.start..range.end),
        )
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

    pub fn extend(&mut self, other: &Self) {
        self.values.extend(&other.values);
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

    pub fn select(&self, if_true: &Array, if_false: &Array) -> Array {
        match (if_true, if_false) {
            (Array::Bool(if_true), Array::Bool(if_false)) => {
                let mut builder = BoolArray::with_capacity(self.len());
                for i in 0..self.len() {
                    match self.get(i) {
                        Some(true) => builder.push(if_true.get(i)),
                        Some(false) => builder.push(if_false.get(i)),
                        None => builder.push(None),
                    }
                }
                Array::Bool(builder)
            }
            (Array::I64(if_true), Array::I64(if_false)) => {
                let mut builder = I64Array::with_capacity(self.len());
                for i in 0..self.len() {
                    match self.get(i) {
                        Some(true) => builder.push(if_true.get(i)),
                        Some(false) => builder.push(if_false.get(i)),
                        None => builder.push(None),
                    }
                }
                Array::I64(builder)
            }
            (Array::F64(if_true), Array::F64(if_false)) => {
                let mut builder = F64Array::with_capacity(self.len());
                for i in 0..self.len() {
                    match self.get(i) {
                        Some(true) => builder.push(if_true.get(i)),
                        Some(false) => builder.push(if_false.get(i)),
                        None => builder.push(None),
                    }
                }
                Array::F64(builder)
            }
            (Array::Date(if_true), Array::Date(if_false)) => {
                let mut builder = DateArray::with_capacity(self.len());
                for i in 0..self.len() {
                    match self.get(i) {
                        Some(true) => builder.push(if_true.get(i)),
                        Some(false) => builder.push(if_false.get(i)),
                        None => builder.push(None),
                    }
                }
                Array::Date(builder)
            }
            (Array::Timestamp(if_true), Array::Timestamp(if_false)) => {
                let mut builder = TimestampArray::with_capacity(self.len());
                for i in 0..self.len() {
                    match self.get(i) {
                        Some(true) => builder.push(if_true.get(i)),
                        Some(false) => builder.push(if_false.get(i)),
                        None => builder.push(None),
                    }
                }
                Array::Timestamp(builder)
            }
            (Array::String(if_true), Array::String(if_false)) => {
                let mut builder = StringArray::with_capacity(self.len());
                for i in 0..self.len() {
                    match self.get(i) {
                        Some(true) => builder.push(if_true.get(i)),
                        Some(false) => builder.push(if_false.get(i)),
                        None => builder.push(None),
                    }
                }
                Array::String(builder)
            }
            (if_true, if_false) => panic!(
                "{} does not match {}",
                if_true.data_type(),
                if_false.data_type()
            ),
        }
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

    pub fn is_scalar(&self, other: Option<bool>) -> BoolArray {
        let mut builder = BoolArray::with_capacity(self.len());
        for i in 0..self.len() {
            builder.push(Some(self.get(i) == other))
        }
        builder
    }

    pub fn equal_scalar(&self, other: Option<bool>) -> BoolArray {
        scalar_comparison_operator!(self, other, left, right, left == right)
    }

    pub fn less_scalar(&self, other: Option<bool>) -> BoolArray {
        scalar_comparison_operator!(self, other, left, right, left < right)
    }

    pub fn less_equal_scalar(&self, other: Option<bool>) -> BoolArray {
        scalar_comparison_operator!(self, other, left, right, left <= right)
    }

    pub fn greater_scalar(&self, other: Option<bool>) -> BoolArray {
        scalar_comparison_operator!(self, other, left, right, left > right)
    }

    pub fn greater_equal_scalar(&self, other: Option<bool>) -> BoolArray {
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

    // Logical operators.

    pub fn not(&self) -> Self {
        array_unary_operator!(self, value, !value)
    }

    pub fn and(&self, other: &Self) -> Self {
        array_binary_operator!(self, other, left, right, left && right)
    }

    pub fn or(&self, other: &Self) -> Self {
        assert_eq!(self.len(), other.len());

        let mut result = Self::with_capacity(self.len());
        for i in 0..self.len() {
            result.push(match (self.get(i), other.get(i)) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (_, _) => None,
            });
        }
        result
    }

    pub fn and_not(&self, other: &Self) -> Self {
        array_binary_operator!(self, other, left, right, left && !right)
    }

    // Logical reduction operators.

    pub fn any(&self, stride: usize) -> Self {
        logical_reduction!(self, stride, false, prev, next, prev || next)
    }

    pub fn all(&self, stride: usize) -> Self {
        logical_reduction!(self, stride, true, prev, next, prev && next)
    }

    pub fn none(&self, stride: usize) -> Self {
        logical_reduction!(self, stride, true, prev, next, prev && !next)
    }

    pub fn count(&self, stride: usize) -> I64Array {
        assert_eq!(self.len() % stride, 0);

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

    // Support operations for data structures.

    pub fn cmp(&self, i: usize, j: usize) -> Ordering {
        self.get(i).partial_cmp(&self.get(j)).unwrap()
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

    // Casts.

    pub fn cast_i64(&self) -> I64Array {
        cast_operator!(self, value, if value { 1 } else { 0 }, I64Array)
    }

    pub fn cast_f64(&self) -> F64Array {
        cast_operator!(self, value, if value { 1.0 } else { 0.0 }, F64Array)
    }

    pub fn cast_string(&self) -> StringArray {
        cast_operator!(
            self,
            value,
            if value { "true" } else { "false" },
            StringArray
        )
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
