use crate::{Array, Bitmask, BoolArray};

macro_rules! array_like {
    ($T:ident, $t:ty) => {
        #[derive(Debug, Clone, Eq, PartialEq, Default)]
        pub struct $T {
            values: Vec<$t>,
            is_valid: Bitmask,
        }

        impl $T {
            // Constructors.

            pub fn with_capacity(capacity: usize) -> Self {
                Self {
                    values: Vec::with_capacity(capacity),
                    is_valid: Bitmask::with_capacity(capacity),
                }
            }

            pub fn zeros(len: usize) -> Self {
                Self {
                    values: vec![Default::default()].repeat(len),
                    is_valid: Bitmask::trues(len),
                }
            }

            pub fn from_values(values: Vec<$t>) -> Self {
                let mut into = Self::default();
                for value in values {
                    into.push(Some(value));
                }
                into
            }

            pub fn from_options(values: Vec<Option<$t>>) -> Self {
                let mut into = Self::default();
                for value in values {
                    into.push(value);
                }
                into
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

            // Fundamental operator support.

            fn unary_operator<A: Array>(&self, f: impl Fn($t) -> A::Element) -> A {
                self.unary_null_operator(|a| match a {
                    Some(a) => Some(f(a)),
                    None => None,
                })
            }

            fn unary_null_operator<A: Array>(
                &self,
                f: impl Fn(Option<$t>) -> Option<A::Element>,
            ) -> A {
                let mut result = A::with_capacity(self.len());
                for i in 0..self.len() {
                    result.push(f(self.get(i)));
                }
                result
            }

            fn binary_operator<A: Array>(
                &self,
                other: &Self,
                f: impl Fn($t, $t) -> A::Element,
            ) -> A {
                assert_eq!(self.len(), other.len());

                self.binary_null_operator(other, |a, b| match (a, b) {
                    (Some(a), Some(b)) => Some(f(a, b)),
                    _ => None,
                })
            }

            fn binary_null_operator<A: Array>(
                &self,
                other: &Self,
                f: impl Fn(Option<$t>, Option<$t>) -> Option<A::Element>,
            ) -> A {
                let mut result = A::with_capacity(self.len());
                for i in 0..self.len() {
                    result.push(f(self.get(i), other.get(i)));
                }
                result
            }

            // Array comparison operators.

            pub fn equal(&self, other: &Self) -> BoolArray {
                self.binary_operator(other, |a, b| a == b)
            }

            // Scalar comparison operators.

            pub fn equal_scalar(&self, other: Option<$t>) -> BoolArray {
                self.unary_operator(|a| Some(a) == other)
            }

            // Complex operations.

            pub fn sort(&self) -> I32Array {
                let mut indexes: Vec<_> = (0..self.len() as i32).collect();
                indexes.sort_by_key(|i| self.get(*i as usize));
                I32Array::from_values(indexes)
            }

            pub fn compress(&self, mask: &BoolArray) -> Self {
                assert_eq!(self.len(), mask.len());

                let mut into = Self::default();
                for i in 0..self.len() {
                    if mask.get(i) == Some(true) {
                        into.push(self.get(i));
                    }
                }
                into
            }
        }
    };
}

array_like!(I32Array, i32);

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
