use std::sync::atomic::{AtomicI64, Ordering};

/// Sequences are stored on the coordinator and used to assign IDs to new objects.
/// The first 100 sequences are reserved for system use.

static CATALOG_ID: AtomicI64 = AtomicI64::new(100);
static TABLE_ID: AtomicI64 = AtomicI64::new(100);
static INDEX_ID: AtomicI64 = AtomicI64::new(100);

pub fn next_catalog_id() -> i64 {
    CATALOG_ID.fetch_add(1, Ordering::Relaxed)
}
pub fn next_table_id() -> i64 {
    TABLE_ID.fetch_add(1, Ordering::Relaxed)
}
pub fn next_index_id() -> i64 {
    INDEX_ID.fetch_add(1, Ordering::Relaxed)
}
