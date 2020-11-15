use crate::page::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::fmt::Display;
use std::ops::Deref;
use std::ops::{Bound, RangeBounds, RangeInclusive};
use std::sync::{Arc, RwLock, RwLockReadGuard};

// Heap represents a logical table as a list of pages.
// New tuples are added to the end of the heap.
// Deleted tuples are periodically garbage-collected and the heap is compacted.
// During the GC process, the heap is partially sorted on the cluster-by key.
// A good cluster-by key will cluster frequently-updated rows together.
pub struct Heap {
    // The clustering of the table is tracked by an immutable trie, which is under a mutable lock to permit re-organization.
    // The leaves of the trie are usually a single page, but can be arbitrarily large when many tuples have the same cluster-by key.
    pages: RwLock<Vec<Page>>,
}

type Key = [u8; 8];

impl Heap {
    pub fn empty(schema: Arc<Schema>) -> Self {
        Self {
            pages: RwLock::new(vec![Page::empty(schema)]),
        }
    }

    pub fn range(&self, range: impl RangeBounds<Key>) -> Range {
        Range {
            lock: self.pages.read().unwrap(),
            offset: 0,
            range: range_inclusive(range).unwrap_or([u8::MAX; 8]..=[0; 8]),
        }
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
                .push(Page::empty(records.schema().clone()));
            self.insert(&slice(records, offset, records.num_rows() - offset), txn);
        }
    }
}

impl Display for Heap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut csv_bytes = vec![];
        for page in self.range(..).iter() {
            arrow::csv::Writer::new(&mut csv_bytes)
                .write(&*page.select())
                .unwrap();
        }
        let csv = String::from_utf8(csv_bytes).unwrap();
        f.write_str(csv.as_str())
    }
}

pub struct Range<'a> {
    lock: RwLockReadGuard<'a, Vec<Page>>,
    offset: usize,
    range: RangeInclusive<Key>,
}

pub struct RangeIter<'a> {
    lock: &'a RwLockReadGuard<'a, Vec<Page>>,
    offset: usize,
    range: &'a RangeInclusive<Key>,
}

impl<'a> Range<'a> {
    pub fn iter(&'a self) -> RangeIter<'a> {
        RangeIter {
            lock: &self.lock,
            offset: self.offset,
            range: &self.range,
        }
    }
}

impl<'a> Iterator for RangeIter<'a> {
    type Item = &'a Page;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(page) = self.lock.deref().get(self.offset) {
                let (x1, x2) = (*self.range.start(), *self.range.end());
                let (y1, y2) = page.range().into_inner();
                if x1 <= y2 && y1 <= x2 {
                    self.offset += 1;
                    return Some(page);
                } else {
                    self.offset += 1;
                }
            } else {
                return None;
            }
        }
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

fn range_inclusive(range: impl RangeBounds<Key>) -> Option<RangeInclusive<Key>> {
    let range_start = match range.start_bound() {
        Bound::Included(start) => *start,
        Bound::Excluded(start) => {
            if let Some(start) = inc(*start) {
                start
            } else {
                return None;
            }
        }
        Bound::Unbounded => [0; 8],
    };
    let range_end = match range.end_bound() {
        Bound::Included(end) => *end,
        Bound::Excluded(end) => {
            if let Some(end) = dec(*end) {
                end
            } else {
                return None;
            }
        }
        Bound::Unbounded => [u8::MAX; 8],
    };
    Some(range_start..=range_end)
}

fn inc(key: Key) -> Option<Key> {
    if key == [u8::MAX; 8] {
        None
    } else {
        Some((u64::from_be_bytes(key) + 1).to_be_bytes())
    }
}
fn dec(key: Key) -> Option<Key> {
    if key == [0; 8] {
        None
    } else {
        Some((u64::from_be_bytes(key) - 1).to_be_bytes())
    }
}
