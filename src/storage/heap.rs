use crate::page::*;
use arrow::record_batch::*;
use std::fmt;
use std::sync::Arc;

// Heap represents a logical table as a list of pages.
// New tuples are added to the end of the heap.
// Deleted tuples are periodically garbage-collected and the heap is compacted.
// During the GC process, the heap is partially sorted on the cluster-by key.
// A good cluster-by key will cluster frequently-updated rows together.
pub struct Heap {
    pages: Vec<Arc<Page>>,
}

impl Heap {
    pub fn empty() -> Self {
        Self { pages: vec![] }
    }

    pub fn scan(&self) -> Vec<Arc<Page>> {
        self.pages.iter().map(|page| page.clone()).collect()
    }

    pub fn insert(&mut self, records: &RecordBatch, txn: u64) {
        if self.pages.is_empty() {
            self.pages.push(Arc::new(Page::empty(records.schema())));
        }
        // Insert however many records fit in the last page.
        let offset = self.pages.last().unwrap().insert(records, txn);
        // If there are leftover records, add a page and try again.
        if offset < records.num_rows() {
            self.pages.push(Arc::new(Page::empty(records.schema())));
            self.insert(&slice(records, offset, records.num_rows() - offset), txn);
        }
    }

    pub fn is_uninitialized(&self) -> bool {
        self.pages.is_empty()
    }

    pub fn print(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        if let Some(page) = self.pages.last() {
            page.print(f, indent)?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for Heap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.print(f, 0)
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
