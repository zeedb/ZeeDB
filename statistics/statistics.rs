use std::{
    collections::HashMap,
    ops::{Index, IndexMut},
};

use context::ContextKey;

use crate::TableStatistics;

pub const STATISTICS_KEY: ContextKey<Statistics> = ContextKey::new("STATISTICS");

#[derive(Clone, Debug)]
pub struct Statistics {
    // TODO store an LRU cache of statistics, but fall back to querying them from metadata schema.
    store: HashMap<i64, TableStatistics>,
}

impl Statistics {
    pub fn insert(&mut self, table_id: i64, table_statistics: TableStatistics) {
        self.store.insert(table_id, table_statistics);
    }

    pub fn remove(&mut self, table_id: i64) {
        self.store.remove(&table_id);
    }

    pub fn get(&self, table_id: i64) -> Option<&TableStatistics> {
        self.store.get(&table_id)
    }

    pub fn get_mut(&mut self, table_id: i64) -> Option<&mut TableStatistics> {
        self.store.get_mut(&table_id)
    }
}

impl Index<i64> for Statistics {
    type Output = TableStatistics;

    fn index(&self, table_id: i64) -> &Self::Output {
        &self.store[&table_id]
    }
}

impl IndexMut<i64> for Statistics {
    fn index_mut(&mut self, table_id: i64) -> &mut Self::Output {
        self.store.get_mut(&table_id).unwrap()
    }
}

impl Default for Statistics {
    fn default() -> Self {
        let mut statistics = Statistics {
            store: HashMap::new(),
        };
        for (table_id, columns) in catalog::bootstrap_statistics() {
            let mut table_statistics = TableStatistics::default();
            for (column_name, data_type) in columns {
                table_statistics.create_column(column_name.to_string(), data_type);
            }
            statistics.insert(table_id, table_statistics);
        }
        statistics
    }
}
