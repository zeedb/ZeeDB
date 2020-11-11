use crate::cluster::*;
use std::sync::Arc;

pub struct Storage {
    tables: Vec<Cluster>,
}

impl Storage {
    pub fn new() -> Self {
        // Initialize system tables catalog, table, column
        Self {
            tables: fixtures::bootstrap_metadata_arrow()
                .drain(..)
                .map(|schema| Cluster::empty(Arc::new(schema)))
                .collect(),
        }
    }

    pub fn table(&self, id: usize) -> &Cluster {
        &self.tables[id]
    }
}
