use crate::ColumnStatistics;
use kernel::RecordBatch;
use std::collections::HashMap;

#[derive(Clone)]
pub struct TableStatistics {
    cardinality: usize,
    columns: HashMap<String, ColumnStatistics>,
}

impl TableStatistics {
    pub fn empty() -> Self {
        Self {
            cardinality: 0,
            columns: HashMap::new(),
        }
    }

    pub fn insert(&mut self, values: &RecordBatch) {
        self.cardinality += values.len();
        for (name, array) in &values.columns {
            self.columns
                .entry(name.clone())
                .or_insert_with(|| ColumnStatistics::new(array.data_type()))
                .insert(array)
        }
    }

    pub fn approx_cardinality(&self) -> usize {
        self.cardinality
    }

    pub fn column(&self, column: &String) -> Option<&ColumnStatistics> {
        self.columns.get(column)
    }
}
