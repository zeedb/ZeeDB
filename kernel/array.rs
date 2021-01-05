use crate::{AnyArray, BoolArray, DataType, I32Array, U64Array};
use std::{cmp::Ordering, ops::Range};
use twox_hash::xxh3;

pub trait Array<'a>: Sized + Clone {
    type Element: Sized + PartialEq + PartialOrd;

    // Constructors.

    fn new() -> Self;
    fn with_capacity(capacity: usize) -> Self;
    fn nulls(len: usize) -> Self;

    fn from_values(values: Vec<Self::Element>) -> Self {
        let mut into = Self::new();
        for value in values {
            into.push(Some(value));
        }
        into
    }

    fn from_options(values: Vec<Option<Self::Element>>) -> Self {
        let mut into = Self::new();
        for value in values {
            into.push(value);
        }
        into
    }

    // Basic container operations.

    fn len(&self) -> usize;
    fn get(&'a self, index: usize) -> Option<Self::Element>;
    fn bytes(&self, index: usize) -> Option<&[u8]>;
    fn slice(&self, range: Range<usize>) -> Self;
    fn set(&mut self, index: usize, value: Option<Self::Element>);
    fn push(&mut self, value: Option<Self::Element>);

    // Reflection.

    fn data_type(&self) -> DataType;

    fn as_any(self) -> AnyArray;

    // Complex container operations.

    fn extend(&mut self, other: &'a Self) {
        for i in 0..other.len() {
            self.push(other.get(i))
        }
    }

    fn cat(arrays: &'a Vec<Self>) -> Self {
        let mut builder = Self::with_capacity(arrays.iter().map(|a| a.len()).sum());
        for array in arrays {
            builder.extend(&array);
        }
        builder
    }

    fn repeat(&'a self, n: usize) -> Self {
        let mut builder = Self::with_capacity(self.len() * n);
        for _ in 0..n {
            builder.extend(self);
        }
        builder
    }

    // Vector operations.

    fn gather(&'a self, indexes: &I32Array) -> Self {
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

    fn compress(&'a self, mask: &BoolArray) -> Self {
        assert_eq!(self.len(), mask.len());

        let mut into = Self::new();
        for i in 0..self.len() {
            if mask.get(i) == Some(true) {
                into.push(self.get(i));
            }
        }
        into
    }

    fn scatter(&'a self, indexes: &I32Array, into: &mut Self) {
        assert_eq!(self.len(), indexes.len());

        for i in 0..indexes.len() {
            if let Some(j) = indexes.get(i) {
                into.set(j as usize, self.get(i))
            }
        }
    }

    fn transpose(&'a self, stride: usize) -> Self {
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

    fn sort(&'a self) -> I32Array {
        let mut indexes: Vec<_> = (0..self.len() as i32).collect();
        indexes.sort_by(|i, j| self.cmp(*i as usize, *j as usize));
        I32Array::from_values(indexes)
    }

    // Fundamental operator support.

    fn unary_operator<'b, A: Array<'b>>(&'a self, f: impl Fn(Self::Element) -> A::Element) -> A {
        self.unary_null_operator(|a| match a {
            Some(a) => Some(f(a)),
            None => None,
        })
    }

    fn unary_null_operator<'b, A: Array<'b>>(
        &'a self,
        f: impl Fn(Option<Self::Element>) -> Option<A::Element>,
    ) -> A {
        let mut result = A::with_capacity(self.len());
        for i in 0..self.len() {
            result.push(f(self.get(i)));
        }
        result
    }

    fn binary_operator<'b, A: Array<'b>>(
        &'a self,
        other: &'a Self,
        f: impl Fn(Self::Element, Self::Element) -> A::Element,
    ) -> A {
        assert_eq!(self.len(), other.len());

        self.binary_null_operator(other, |a, b| match (a, b) {
            (Some(a), Some(b)) => Some(f(a, b)),
            _ => None,
        })
    }

    fn binary_null_operator<'b, A: Array<'b>>(
        &'a self,
        other: &'a Self,
        f: impl Fn(Option<Self::Element>, Option<Self::Element>) -> Option<A::Element>,
    ) -> A {
        let mut result = A::with_capacity(self.len());
        for i in 0..self.len() {
            result.push(f(self.get(i), other.get(i)));
        }
        result
    }

    // Array comparison operators.

    fn is(&'a self, other: &'a Self) -> BoolArray {
        self.binary_null_operator(other, |a, b| Some(a == b))
    }

    fn equal(&'a self, other: &'a Self) -> BoolArray {
        self.binary_operator(other, |a, b| a == b)
    }

    fn not_equal(&'a self, other: &'a Self) -> BoolArray {
        self.binary_operator(other, |a, b| a != b)
    }

    fn less(&'a self, other: &'a Self) -> BoolArray {
        self.binary_operator(other, |a, b| a < b)
    }

    fn less_equal(&'a self, other: &'a Self) -> BoolArray {
        self.binary_operator(other, |a, b| a <= b)
    }

    fn greater(&'a self, other: &'a Self) -> BoolArray {
        self.binary_operator(other, |a, b| a > b)
    }

    fn greater_equal(&'a self, other: &'a Self) -> BoolArray {
        self.binary_operator(other, |a, b| a >= b)
    }

    fn is_null(&'a self) -> BoolArray {
        self.unary_null_operator(|a| Some(a.is_none()))
    }

    fn coalesce(&'a self, other: &'a Self) -> Self {
        self.binary_null_operator(other, |a, b| match (a, b) {
            (Some(left), _) => Some(left),
            (_, Some(right)) => Some(right),
            (None, None) => None,
        })
    }

    fn null_if(&'a self, other: &'a Self) -> Self {
        self.binary_null_operator(other, |a, b| match (a, b) {
            (Some(left), Some(right)) if left == right => None,
            (Some(left), _) => Some(left),
            (_, _) => None,
        })
    }

    // Scalar comparison operators.

    fn is_scalar(&'a self, other: Option<Self::Element>) -> BoolArray {
        self.unary_null_operator(|a| Some(a == other))
    }

    fn equal_scalar(&'a self, other: Option<Self::Element>) -> BoolArray {
        self.unary_operator(|a| Some(a) != other)
    }

    fn less_scalar(&'a self, other: Option<Self::Element>) -> BoolArray {
        self.unary_operator(|a| Some(a) < other)
    }

    fn less_equal_scalar(&'a self, other: Option<Self::Element>) -> BoolArray {
        self.unary_operator(|a| Some(a) <= other)
    }

    fn greater_scalar(&'a self, other: Option<Self::Element>) -> BoolArray {
        self.unary_operator(|a| Some(a) > other)
    }

    fn greater_equal_scalar(&'a self, other: Option<Self::Element>) -> BoolArray {
        self.unary_operator(|a| Some(a) >= other)
    }

    // Support operations for data structures.

    fn cmp(&'a self, i: usize, j: usize) -> Ordering {
        self.get(i).partial_cmp(&self.get(j)).unwrap()
    }

    fn hash(&self, state: &mut U64Array) {
        for i in 0..self.len() {
            if let Some(bytes) = self.bytes(i) {
                let seed = state.get(i).unwrap();
                let hash = xxh3::hash64_with_seed(bytes, seed);
                state.set(i, Some(hash))
            }
        }
    }
}
