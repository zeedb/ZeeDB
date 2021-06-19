use std::sync::atomic::{AtomicI64, Ordering};

use context::ContextKey;

pub const SEQUENCES_KEY: ContextKey<Sequences> = ContextKey::new("SEQUENCES_KEY");

pub struct Sequences {
    pub catalog_id: AtomicI64,
    pub table_id: AtomicI64,
    pub index_id: AtomicI64,
}

impl Default for Sequences {
    fn default() -> Self {
        Self {
            // The first 100 ids are reserved for system use.
            catalog_id: AtomicI64::new(100),
            table_id: AtomicI64::new(100),
            index_id: AtomicI64::new(100),
        }
    }
}

impl Sequences {
    pub fn next_catalog_id(&self) -> i64 {
        self.catalog_id.fetch_add(1, Ordering::Relaxed)
    }
    pub fn next_table_id(&self) -> i64 {
        self.table_id.fetch_add(1, Ordering::Relaxed)
    }
    pub fn next_index_id(&self) -> i64 {
        self.index_id.fetch_add(1, Ordering::Relaxed)
    }
}
