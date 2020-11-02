use crate::cluster::*;
use std::sync::Arc;

pub struct Storage {
    tables: Vec<Cluster>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            tables: fixtures::bootstrap_metadata_arrow()
                .drain(..)
                .map(|schema| Cluster::new(Arc::new(schema)))
                .collect(),
        }
    }

    pub fn table(&self, id: usize) -> &Cluster {
        &self.tables[id]
    }
}
