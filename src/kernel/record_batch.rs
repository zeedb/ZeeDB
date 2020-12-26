use crate::any_array::*;
use crate::data_type::*;
use crate::typed_array::*;

pub struct RecordBatch {
    pub columns: Vec<(String, Array)>,
}

impl RecordBatch {
    pub fn new(columns: Vec<(String, Array)>) -> Self {
        Self { columns }
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

    pub fn select(&self, mask: &BoolArray, default: &Self) -> Self {
        assert_eq!(self.columns.len(), default.columns.len());

        Self::new(
            (0..self.columns.len())
                .map(|i| {
                    let (self_name, self_column) = &self.columns[i];
                    let (default_name, default_column) = &default.columns[i];

                    assert_eq!(self_name, default_name);

                    (self_name.clone(), self_column.select(mask, default_column))
                })
                .collect(),
        )
    }

    pub fn sort(&self) -> I32Array {
        todo!()
    }
}
