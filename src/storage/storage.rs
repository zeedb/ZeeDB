use crate::heap::*;
use std::sync::Arc;

pub struct Storage {
    tables: Vec<Heap>,
}

impl Storage {
    pub fn new() -> Self {
        // Initialize system tables catalog, table, column
        Self {
            tables: catalog::bootstrap_storage()
                .drain(..)
                .map(|schema| Heap::empty(Arc::new(schema)))
                .collect(),
        }
    }

    pub fn table(&self, id: i64) -> &Heap {
        &self.tables[id as usize]
    }
}
