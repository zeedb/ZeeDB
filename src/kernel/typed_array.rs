use crate::bitmask::*;

pub struct BoolArray {
    values: Bitmask,
    is_valid: Bitmask,
}

pub struct I32Array {
    values: Vec<i32>,
    is_valid: Bitmask,
}

pub struct I64Array {
    values: Vec<i64>,
    is_valid: Bitmask,
}

pub struct U64Array {
    values: Vec<u64>,
    is_valid: Bitmask,
}

pub struct F64Array {
    values: Vec<f64>,
    is_valid: Bitmask,
}

pub struct DateArray {
    values: Vec<i32>,
    is_valid: Bitmask,
}

pub struct TimestampArray {
    values: Vec<i64>,
    is_valid: Bitmask,
}

pub struct StringArray {
    buffer: String,
    offsets: Vec<i32>,
    is_valid: Bitmask,
}

macro_rules! primitive_ops {
    ($T:ty, $t:ty) => {
        impl $T {
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

            pub fn nulls(len: usize) -> Self {
                Self {
                    values: vec![Default::default()].repeat(len),
                    is_valid: Bitmask::falses(len),
                }
            }

            pub fn from_slice(values: &[$t], is_valid: &[u8]) -> Self {
                assert_eq!((values.len() + 7) / 8, is_valid.len());

                Self {
                    values: values.to_vec(),
                    is_valid: Bitmask::from_slice(is_valid, values.len()),
                }
            }

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

            pub fn select(&self, mask: &BoolArray, default: &Self) -> Self {
                todo!()
            }

            pub fn sort(&self) -> I32Array {
                todo!()
            }

            pub fn is(&self, other: Self) -> BoolArray {
                todo!()
            }

            pub fn equal(&self, other: Self) -> BoolArray {
                todo!()
            }

            pub fn less(&self, other: Self) -> BoolArray {
                todo!()
            }

            pub fn less_equal(&self, other: Self) -> BoolArray {
                todo!()
            }

            pub fn greater(&self, other: Self) -> BoolArray {
                todo!()
            }

            pub fn greater_equal(&self, other: Self) -> BoolArray {
                todo!()
            }

            pub fn is_scalar(&self, other: Option<$t>) -> BoolArray {
                todo!()
            }

            pub fn equal_scalar(&self, other: Option<$t>) -> BoolArray {
                todo!()
            }

            pub fn less_scalar(&self, other: Option<$t>) -> BoolArray {
                todo!()
            }

            pub fn less_equal_scalar(&self, other: Option<$t>) -> BoolArray {
                todo!()
            }

            pub fn greater_scalar(&self, other: Option<$t>) -> BoolArray {
                todo!()
            }

            pub fn greater_equal_scalar(&self, other: Option<$t>) -> BoolArray {
                todo!()
            }

            pub fn hash(&self, state: &mut U64Array) {
                todo!()
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
            pub fn div(&self, other: &Self) -> Self {
                todo!()
            }

            pub fn mul(&self, other: &Self) -> Self {
                todo!()
            }

            pub fn add(&self, other: &Self) -> Self {
                todo!()
            }

            pub fn sub(&self, other: &Self) -> Self {
                todo!()
            }

            pub fn div_scalar(&self, other: Option<$t>) -> Self {
                todo!()
            }

            pub fn mul_scalar(&self, other: Option<$t>) -> Self {
                todo!()
            }

            pub fn add_scalar(&self, other: Option<$t>) -> Self {
                todo!()
            }

            pub fn sub_scalar(&self, other: Option<$t>) -> Self {
                todo!()
            }
        }
    };
}

math_ops!(I64Array, i32);
math_ops!(F64Array, i32);

impl BoolArray {
    pub fn new() -> Self {
        Self {
            values: Bitmask::new(),
            is_valid: Bitmask::new(),
        }
    }

    pub fn nulls(len: usize) -> Self {
        Self {
            values: Bitmask::falses(len),
            is_valid: Bitmask::falses(len),
        }
    }

    pub fn from_slice(values: &[u8], is_valid: &[u8], len: usize) -> Self {
        Self {
            values: Bitmask::from_slice(values, len),
            is_valid: Bitmask::from_slice(is_valid, values.len()),
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

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

    pub fn select(&self, mask: &BoolArray, default: &Self) -> Self {
        todo!()
    }

    pub fn sort(&self) -> I32Array {
        todo!()
    }

    pub fn get(&self, index: usize) -> Option<bool> {
        if self.is_valid.get(index) {
            Some(self.values.get(index))
        } else {
            None
        }
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

impl StringArray {
    pub fn new() -> Self {
        Self {
            buffer: String::new(),
            offsets: vec![0],
            is_valid: Bitmask::new(),
        }
    }

    pub fn from_slice(values: &str, offsets: &[i32], is_valid: &[u8]) -> Self {
        assert_eq!((offsets.len() - 1 + 7) / 8, is_valid.len());

        Self {
            buffer: values.to_string(),
            offsets: offsets.to_vec(),
            is_valid: Bitmask::from_slice(is_valid, values.len()),
        }
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

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
        todo!()
    }

    pub fn select(&self, mask: &BoolArray, default: &Self) -> Self {
        todo!()
    }

    pub fn sort(&self) -> I32Array {
        todo!()
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
}

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
