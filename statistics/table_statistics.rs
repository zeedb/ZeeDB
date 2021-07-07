use std::collections::{hash_map::Entry, HashMap};

use kernel::{DataType, RecordBatch};
use serde::{Deserialize, Serialize};

use crate::ColumnStatistics;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TableStatistics {
    cardinality: usize,
    columns: HashMap<String, ColumnStatistics>,
}

impl TableStatistics {
    pub fn insert(&mut self, values: &RecordBatch) {
        self.cardinality += values.len();
        for (name, array) in &values.columns {
            self.columns
                .entry(name.clone())
                .or_insert_with(|| ColumnStatistics::new(array.data_type()))
                .insert(array)
        }
    }

    pub fn merge(&mut self, other: TableStatistics) {
        self.cardinality += other.cardinality;
        for (column, statistics) in other.columns {
            match self.columns.entry(column) {
                Entry::Occupied(mut entry) => entry.get_mut().merge(&statistics),
                Entry::Vacant(entry) => {
                    entry.insert(statistics);
                }
            }
        }
    }

    pub fn create_column(&mut self, column: String, data_type: DataType) {
        self.columns
            .entry(column)
            .or_insert_with(|| ColumnStatistics::new(data_type));
    }

    pub fn approx_cardinality(&self) -> usize {
        self.cardinality
    }

    pub fn column(&self, column: &str) -> Option<&ColumnStatistics> {
        self.columns.get(column)
    }
}
