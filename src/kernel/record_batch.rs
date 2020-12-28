use crate::any_array::*;
use crate::bool_array::*;
use crate::data_type::*;
use crate::primitive_array::*;
use std::{cmp::Ordering, fmt::Debug, ops::Range};

#[derive(Clone)]
pub struct RecordBatch {
    pub columns: Vec<(String, Array)>,
}

impl RecordBatch {
    pub fn new(columns: Vec<(String, Array)>) -> Self {
        Self { columns }
    }

    pub fn nulls(mut columns: Vec<(String, DataType)>, len: usize) -> Self {
        Self {
            columns: columns
                .drain(..)
                .map(|(name, data_type)| (name, Array::nulls(data_type, len)))
                .collect(),
        }
    }

    pub fn zip(left: Self, right: Self) -> Self {
        assert_eq!(left.len(), right.len());

        let mut combined = vec![];
        combined.extend(left.columns);
        combined.extend(right.columns);

        RecordBatch::new(combined)
    }

    pub fn cat(batches: &Vec<RecordBatch>) -> RecordBatch {
        todo!()
    }

    pub fn repeat(&self, n: usize) -> Self {
        todo!()
    }

    pub fn extend(&mut self, other: &Self) -> Self {
        todo!()
    }

    pub fn len(&self) -> usize {
        self.columns[0].1.len()
    }

    pub fn schema(&self) -> Vec<(String, DataType)> {
        self.columns
            .iter()
            .map(|(n, c)| (n.clone(), c.data_type()))
            .collect()
    }

    pub fn find(&self, column: &str) -> Option<&Array> {
        todo!()
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        todo!()
    }

    pub fn gather(&self, indexes: &I32Array) -> Self {
        Self::new(
            self.columns
                .iter()
                .map(|(n, c)| (n.clone(), c.gather(indexes)))
                .collect(),
        )
    }

    pub fn compress(&self, mask: &BoolArray) -> Self {
        Self::new(
            self.columns
                .iter()
                .map(|(n, c)| (n.clone(), c.compress(mask)))
                .collect(),
        )
    }

    pub fn scatter(&self, indexes: &I32Array, into: &mut Self) {
        assert_eq!(self.columns.len(), into.columns.len());

        for i in 0..self.columns.len() {
            assert_eq!(self.columns[i].0, into.columns[i].0);

            self.columns[i].1.scatter(indexes, &mut into.columns[i].1);
        }
    }

    /// Transpose each column, where each column is interpreted as a column-major matrix of size [stride X _].
    pub fn transpose(&self, stride: usize) -> Self {
        todo!()
    }

    pub fn sort(&self, desc: Vec<bool>) -> I32Array {
        let mut indexes: Vec<_> = (0..self.len() as i32).collect();
        indexes.sort_by(|i, j| {
            for c in 0..self.columns.len() {
                match self.columns[c].1.cmp(*i as usize, *j as usize) {
                    Ordering::Less => {
                        if desc[c] {
                            return Ordering::Greater;
                        } else {
                            return Ordering::Less;
                        }
                    }
                    Ordering::Equal => {
                        // Continue to next match column.
                    }
                    Ordering::Greater => {
                        if desc[c] {
                            return Ordering::Less;
                        } else {
                            return Ordering::Greater;
                        }
                    }
                }
            }
            Ordering::Equal
        });
        I32Array::from(indexes)
    }
}

impl Debug for RecordBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", crate::fixed_width(&vec![self.clone()]))
    }
}
