use crate::page::*;
use kernel::*;
use sketch::Sketch;
use std::{collections::HashMap, fmt};

// Heap represents a logical table as a list of pages.
// New tuples are added to the end of the heap.
// Deleted tuples are periodically garbage-collected and the heap is compacted.
#[derive(Clone)]
pub struct Heap {
    pages: Vec<Page>,
    stats: HashMap<String, Sketch>,
}

impl Heap {
    pub fn empty() -> Self {
        Self {
            pages: vec![],
            stats: HashMap::new(),
        }
    }

    pub fn scan(&self) -> Vec<Page> {
        self.pages.clone()
    }

    pub fn bitmap_scan(&self, mut tids: Vec<i64>, projects: &Vec<String>) -> Vec<RecordBatch> {
        if tids.is_empty() {
            return vec![RecordBatch::empty(self.pages[0].schema(projects))];
        }
        // Sort tids so we can access pages in-order.
        tids.sort();
        // Collect tids into 1 batch per page, and scan each page.
        let mut batches = vec![];
        let mut i = 0;
        while i < tids.len() {
            let pid = tids[i] as usize / PAGE_SIZE;
            let mut rids = I32Array::new();
            while i < tids.len() && pid == tids[i] as usize / PAGE_SIZE {
                let rid = tids[i] as usize % PAGE_SIZE;
                rids.push(Some(rid as i32));
                i += 1;
            }
            let batch = self.page(pid).select(projects);
            batches.push(batch.gather(&rids));
        }
        batches
    }

    pub fn insert(&mut self, records: &RecordBatch, txn: i64) -> I64Array {
        if self.pages.is_empty() {
            self.pages
                .push(Page::empty(self.pages.len(), records.schema()));
            for (name, array) in &records.columns {
                self.stats
                    .insert(name.clone(), Sketch::new(array.data_type()));
            }
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
                .push(Page::empty(self.pages.len(), records.schema()));
            self.insert_more(records, txn, tids, offset);
        }
        // Update counters.
        for (name, array) in &records.columns {
            match name.as_str() {
                "$xmin" | "$xmax" | "$tid" => {}
                name => {
                    let counter = self.stats.get_mut(name).expect(name);
                    counter.insert(array);
                }
            }
        }
    }

    pub fn page(&self, pid: usize) -> &Page {
        &self.pages[pid]
    }

    pub fn truncate(&mut self) {
        self.pages = vec![];
    }

    pub fn approx_cardinality(&self) -> usize {
        if let Some(last) = self.pages.last() {
            last.approx_num_rows() + PAGE_SIZE * (self.pages.len() - 1)
        } else {
            0
        }
    }

    pub fn approx_count_distinct(&self, column: &String) -> usize {
        if self.pages.is_empty() {
            return 0;
        }
        self.stats.get(column).expect(column).count_distinct() as usize
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
