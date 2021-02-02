use crate::TableStatistics;
use context::ContextKey;
use std::{
    collections::HashMap,
    ops::{Index, IndexMut},
};

pub const STATISTICS_KEY: ContextKey<Statistics> = ContextKey::new("STATISTICS");

#[derive(Clone, Default, Debug)]
pub struct Statistics {
    // TODO store an LRU cache of statistics, but fall back to querying them from metadata schema.
    store: HashMap<i64, TableStatistics>,
}

impl Index<i64> for Statistics {
    type Output = TableStatistics;

    fn index(&self, index: i64) -> &Self::Output {
        &self.store[&index]
    }
}

impl IndexMut<i64> for Statistics {
    fn index_mut(&mut self, index: i64) -> &mut Self::Output {
        self.store.get_mut(&index).unwrap()
    }
}
