use std::{fmt, sync::Arc};

use kernel::*;

use crate::page::*;

// Heap represents a logical table as a list of pages.
// New tuples are added to the end of the heap.
// Deleted tuples are periodically garbage-collected and the heap is compacted.
#[derive(Clone)]
pub struct Heap {
    pages: Vec<Arc<Page>>,
}

impl Heap {
    // TODO replace with Default
    pub fn empty() -> Self {
        Self { pages: vec![] }
    }

    pub fn scan(&self) -> Vec<Arc<Page>> {
        self.pages.clone()
    }

    pub fn bitmap_scan(&self, sorted_tids: &Vec<i64>) -> Vec<Arc<Page>> {
        if sorted_tids.is_empty() {
            return vec![];
        }
        // Collect tids into 1 batch per page, and scan each page.
        let mut i = 0;
        let mut j = 0;
        let mut matches = vec![];
        while i < sorted_tids.len() && j < self.pages.len() {
            let pid = sorted_tids[i] as usize / PAGE_SIZE;
            if pid < j {
                // Go to the next tid.
                i += 1
            } else if j < pid {
                // Go to the next page.
                j += 1
            } else {
                // Retain the current page.
                matches.push(self.pages[j].clone());
                // Go to the next page.
                j += 1
            }
        }
        matches
    }

    pub fn insert(&mut self, records: &RecordBatch, txn: i64) -> I64Array {
        if self.pages.is_empty() {
            self.pages
                .push(Arc::new(Page::empty(self.pages.len(), records.schema())));
        }
        // Allocate arrays to keep track of where we insert the rows.
        let mut tids = I64Array::with_capacity(records.len());
        // Insert, adding new pages if needed.
        let mut offset = 0;
        self.insert_more(records, txn, &mut tids, &mut offset);

        tids
    }

    pub fn insert_more(
        &mut self,
        records: &RecordBatch,
        txn: i64,
        tids: &mut I64Array,
        offset: &mut usize,
    ) {
        // Insert however many records fit in the last page.
        let last = self.pages.last_mut().unwrap();
        last.insert(records, txn, tids, offset);
        // If there are leftover records, add a page and try again.
        if *offset < records.len() {
            self.pages
                .push(Arc::new(Page::empty(self.pages.len(), records.schema())));
            self.insert_more(records, txn, tids, offset);
        }
    }

    pub fn page(&self, pid: usize) -> &Page {
        &self.pages[pid]
    }

    pub fn truncate(&mut self) {
        self.pages = vec![];
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

    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }
}

impl std::fmt::Debug for Heap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.print(f, 0)
    }
}
