use crate::page::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::fmt::Display;
use std::sync::{Arc, RwLock};

// Heap represents a logical table as a list of pages.
// New tuples are added to the end of the heap.
// Deleted tuples are periodically garbage-collected and the heap is compacted.
// During the GC process, the heap is partially sorted on the cluster-by key.
// A good cluster-by key will cluster frequently-updated rows together.
pub struct Heap {
    pages: RwLock<Vec<Arc<Page>>>,
}

impl Heap {
    pub fn empty(schema: Arc<Schema>) -> Self {
        Self {
            pages: RwLock::new(vec![Arc::new(Page::empty(schema))]),
        }
    }

    pub fn scan(&self) -> Vec<Arc<Page>> {
        self.pages
            .read()
            .unwrap()
            .iter()
            .map(|page| page.clone())
            .collect()
    }

    pub fn insert(&self, records: &RecordBatch, txn: u64) {
        // Insert however many records fit in the last page.
        let offset = self
            .pages
            .read()
            .unwrap()
            .last()
            .unwrap()
            .insert(records, txn);
        // If there are leftover records, add a page and try again.
        if offset < records.num_rows() {
            self.pages
                .write()
                .unwrap()
                .push(Arc::new(Page::empty(records.schema().clone())));
            self.insert(&slice(records, offset, records.num_rows() - offset), txn);
        }
    }
}

impl Display for Heap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for page in self.scan().iter() {
            page.fmt(f)?;
        }
        Ok(())
    }
}

fn slice(records: &RecordBatch, offset: usize, length: usize) -> RecordBatch {
    let columns = records
        .columns()
        .iter()
        .map(|column| column.slice(offset, length))
        .collect();
    RecordBatch::try_new(records.schema().clone(), columns).unwrap()
}
