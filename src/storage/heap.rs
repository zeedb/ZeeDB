use crate::page::*;
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::*;
use std::fmt;
use std::sync::Arc;

// Heap represents a logical table as a list of pages.
// New tuples are added to the end of the heap.
// Deleted tuples are periodically garbage-collected and the heap is compacted.
// During the GC process, the heap is partially sorted on the cluster-by key.
// A good cluster-by key will cluster frequently-updated rows together.
pub struct Heap {
    pages: Vec<Page>,
}

impl Heap {
    pub fn empty() -> Self {
        Self { pages: vec![] }
    }

    pub fn scan(&self) -> Vec<Page> {
        self.pages.iter().map(|page| page.clone()).collect()
    }

    pub fn insert(&mut self, records: &RecordBatch, txn: u64) -> (Arc<dyn Array>, Arc<dyn Array>) {
        if self.pages.is_empty() {
            self.pages.push(Page::empty(records.schema()));
        }
        // Allocate arrays to keep track of where we insert the rows.
        let mut pids = UInt64Builder::new(records.num_rows());
        let mut tids = UInt32Builder::new(records.num_rows());
        // Insert, adding new pages if needed.
        let mut offset = 0;
        self.insert_more(records, txn, &mut pids, &mut tids, &mut offset);

        (Arc::new(pids.finish()), Arc::new(tids.finish()))
    }

    pub fn insert_more(
        &mut self,
        records: &RecordBatch,
        txn: u64,
        pids: &mut UInt64Builder,
        tids: &mut UInt32Builder,
        offset: &mut usize,
    ) {
        // Insert however many records fit in the last page.
        let last = self.pages.last().unwrap();
        last.insert(records, txn, pids, tids, offset);
        // If there are leftover records, add a page and try again.
        if *offset < records.num_rows() {
            self.pages.push(Page::empty(records.schema()));
            self.insert_more(records, txn, pids, tids, offset);
        }
    }

    pub fn truncate(&mut self) {
        self.pages = vec![];
    }

    pub fn schema(&self) -> Option<Arc<Schema>> {
        self.pages.first().map(|page| page.schema())
    }

    pub(crate) fn is_uninitialized(&self) -> bool {
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
