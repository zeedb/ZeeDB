use std::{cmp::Ordering, fmt::Debug, ops::Range};

use serde::{Deserialize, Serialize};

use crate::{any_array::*, array_like::*, bool_array::*, data_type::*};

#[derive(Clone, Serialize, Deserialize)]
pub struct RecordBatch {
    pub columns: Vec<(String, AnyArray)>,
}

impl RecordBatch {
    pub fn new(columns: Vec<(String, AnyArray)>) -> Self {
        assert!(!columns.is_empty());

        for i in 1..columns.len() {
            let (left_name, left_column) = &columns[0];
            let (right_name, right_column) = &columns[i];
            assert_eq!(
                left_column.len(),
                right_column.len(),
                "column {} has length {} but column {} has length {}",
                left_name,
                left_column.len(),
                right_name,
                right_column.len()
            );
        }

        Self { columns }
    }

    pub fn nulls(mut columns: Vec<(String, DataType)>, len: usize) -> Self {
        assert!(!columns.is_empty());

        Self {
            columns: columns
                .drain(..)
                .map(|(name, data_type)| (name, AnyArray::nulls(data_type, len)))
                .collect(),
        }
    }

    pub fn empty(mut columns: Vec<(String, DataType)>) -> Self {
        assert!(!columns.is_empty());

        Self {
            columns: columns
                .drain(..)
                .map(|(name, data_type)| (name, AnyArray::new(data_type)))
                .collect(),
        }
    }

    pub fn zip(left: Self, right: Self) -> Self {
        assert_eq!(left.len(), right.len());

        let mut combined = vec![];
        combined.extend(left.columns);
        combined.extend(right.columns);

        Self::new(combined)
    }

    pub fn cat(mut batches: Vec<Self>) -> Option<Self> {
        if batches.is_empty() {
            return None;
        }
        let n_columns = batches[0].columns.len();
        let names: Vec<String> = batches[0]
            .columns
            .iter()
            .map(|(name, _)| name.clone())
            .collect();
        let mut transpose: Vec<Vec<AnyArray>> = (0..n_columns).map(|_| vec![]).collect();
        batches.drain(..).for_each(|mut batch| {
            batch
                .columns
                .drain(..)
                .enumerate()
                .for_each(|(i, (_, array))| {
                    transpose[i].push(array);
                });
        });
        let columns: Vec<(String, AnyArray)> = transpose
            .drain(..)
            .enumerate()
            .map(|(i, arrays)| (names[i].clone(), AnyArray::cat(arrays)))
            .collect();
        Some(Self::new(columns))
    }

    pub fn repeat(&self, n: usize) -> Self {
        let columns = self
            .columns
            .iter()
            .map(|(name, array)| (name.clone(), array.repeat(n)))
            .collect();
        Self::new(columns)
    }

    pub fn extend(&mut self, other: &Self) {
        for i in 0..self.columns.len() {
            let (left_name, left_array) = &mut self.columns[i];
            let (right_name, right_array) = &other.columns[i];
            assert_eq!(left_name, right_name);
            left_array.extend(&right_array)
        }
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

    pub fn find(&self, column: &str) -> Option<&AnyArray> {
        self.columns
            .iter()
            .find(|(name, _)| name == column)
            .map(|(_, array)| array)
    }

    pub fn find_always(&self, column: &str) -> &AnyArray {
        match self.columns.iter().find(|(name, _)| name == column) {
            Some((_, array)) => array,
            None => {
                let column_names: Vec<_> =
                    self.columns.iter().map(|(name, _)| name.clone()).collect();
                panic!("{} is not in {}", column, column_names.join(", "))
            }
        }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        Self::new(
            self.columns
                .iter()
                .map(|(name, array)| (name.clone(), array.slice(range.start..range.end)))
                .collect(),
        )
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
        let columns = self
            .columns
            .iter()
            .map(|(name, array)| (name.clone(), array.transpose(stride)))
            .collect();
        Self::new(columns)
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
        I32Array::from_values(indexes)
    }

    pub fn rename(mut self, renames: &Vec<(String, String)>) -> Self {
        let columns: Vec<(String, AnyArray)> = self
            .columns
            .drain(..)
            .flat_map(|(old_name, array)| {
                renames
                    .iter()
                    .find(|(from_name, _)| from_name == &old_name)
                    .map(|(_, new_name)| (new_name.clone(), array))
            })
            .collect();
        assert_eq!(renames.len(), columns.len());
        Self::new(columns)
    }

    pub fn with_names(mut self, column_names: &Vec<String>) -> Self {
        let columns = column_names
            .iter()
            .zip(self.columns.drain(..))
            .map(|(name, (_, array))| (name.clone(), array))
            .collect();
        Self::new(columns)
    }
}

impl Debug for RecordBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", crate::fixed_width(self))
    }
}
