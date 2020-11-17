use crate::heap::*;
use std::sync::Arc;

pub struct Storage {
    tables: Vec<Heap>,
}

impl Storage {
    pub fn new() -> Self {
        // Initialize system tables catalog, table, column
        Self {
            tables: fixtures::bootstrap_metadata_arrow()
                .drain(..)
                .map(|schema| Heap::empty(Arc::new(schema)))
                .collect(),
        }
    }

    pub fn table(&self, id: usize) -> &Heap {
        &self.tables[id]
    }
}
