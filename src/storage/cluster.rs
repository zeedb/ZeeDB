use crate::bytes::*;
use crate::page::*;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::ops::Deref;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, RwLock};

// Cluster represents a logical table as a list of heaps, clustered on a non-unique key.
// A good cluster-by key will cluster frequently-updated rows together.
pub struct Cluster {
    // The clustering of the table is tracked by an immutable trie, which is under a mutable lock to permit re-organization.
    // The leaves of the trie are usually a single page, but can be arbitrarily large when many tuples have the same cluster-by key.
    index: RwLock<BTreeMap<Key, Vec<Page>>>,
}

type Key = [u8; 8];

impl Cluster {
    pub fn empty(schema: Arc<Schema>) -> Self {
        let mut index = BTreeMap::new();
        index.insert([0u8; 8], vec![Page::Mutable(Pax::empty(schema))]);
        Self {
            index: RwLock::new(index),
        }
    }

    pub fn select(&self, range: impl RangeBounds<Key>) -> Vec<RecordBatchRef> {
        self.range(range)
            .iter()
            .flat_map(|(_, _, heap)| heap)
            .map(|page| page.select())
            .collect()
    }

    pub fn insert(&self, range: impl RangeBounds<Key>, input: Vec<RecordBatch>, txn: u64) {
        // Insert rows into existing pages, keeping track of any ranges where we ran out of space.
        let mut incomplete = vec![];
        for (start, end, heap) in self.range(range) {
            if let Some(offset) = insert_into_existing_pages(&input, start, end, &heap, txn) {
                incomplete.push((offset, start, end))
            }
        }
        // Insert leftover rows by restructuring the index.
        for (offset, start, end) in incomplete {
            insert_into_new_pages(
                &input,
                offset,
                start,
                end,
                &mut *self.index.write().unwrap(),
                txn,
            )
        }
    }

    pub fn update(&self, range: impl RangeBounds<Key>) -> Vec<Pax> {
        self.range_mut(range)
            .iter()
            .flat_map(|(_, _, heap)| heap.clone())
            .collect()
    }

    fn range_mut(&self, range: impl RangeBounds<Key>) -> Vec<(Key, Key, Vec<Pax>)> {
        if let Some((range_start, range_end)) = range_inclusive(range) {
            let mut write_lock = self.index.write().unwrap();
            let starts: Vec<Key> = write_lock.keys().cloned().collect();
            fn melt(heap: &mut Vec<Page>) -> Vec<Pax> {
                heap.iter_mut()
                    .map(|page| match page {
                        Page::Mutable(pax) => pax.clone(),
                        Page::Frozen(arrow) => {
                            let pax = arrow.melt();
                            *page = Page::Mutable(pax.clone());
                            pax
                        }
                    })
                    .collect()
            }
            starts
                .iter()
                .enumerate()
                .filter_map(|(i, start)| {
                    let start = *start;
                    let end = if i + 1 < starts.len() {
                        starts[i + 1]
                    } else {
                        [u8::MAX; 8]
                    };
                    // If [start, end] overlaps [range_start, range_end], include this heap.
                    if range_start <= end && start <= range_end {
                        Some((start, end, melt(write_lock.get_mut(&start).unwrap())))
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            vec![]
        }
    }

    fn range(&self, range: impl RangeBounds<Key>) -> Vec<(Key, Key, Vec<Page>)> {
        if let Some((range_start, range_end)) = range_inclusive(range) {
            let read_lock = self.index.read().unwrap();
            let starts: Vec<Key> = read_lock.keys().cloned().collect();
            starts
                .iter()
                .enumerate()
                .filter_map(|(i, start)| {
                    let start = *start;
                    let end = if i + 1 < starts.len() {
                        starts[i + 1]
                    } else {
                        [u8::MAX; 8]
                    };
                    // If [start, end] overlaps [range_start, range_end], include this heap.
                    if range_start <= end && start <= range_end {
                        Some((start, end, read_lock[&start].clone()))
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            vec![]
        }
    }
}

impl Display for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut csv_bytes = vec![];
        for record_batch in self.select(..) {
            arrow::csv::Writer::new(&mut csv_bytes)
                .write(&*record_batch)
                .unwrap();
        }
        let csv = String::from_utf8(csv_bytes).unwrap();
        f.write_str(csv.as_str())
    }
}

fn insert_into_existing_pages(
    input: &Vec<RecordBatch>,
    start: Key,
    end: Key,
    heap: &Vec<Page>,
    txn: u64,
) -> Option<usize> {
    let mut offset = 0;
    for page in heap {
        if let Page::Mutable(pax) = page {
            loop {
                if let Some(tuple) = next_tuple(&input, offset, start, end) {
                    if pax.insert(tuple, txn) {
                        offset += 1;
                    } else {
                        break;
                    }
                } else {
                    return None;
                }
            }
        }
    }
    Some(offset)
}

fn next_tuple(
    input: &Vec<RecordBatch>,
    mut offset: usize,
    start: Key,
    end: Key,
) -> Option<Vec<Box<dyn Any>>> {
    for batch in input {
        if offset < batch.num_rows() {
            let cluster_by = cluster_by_key(Arc::deref(batch.column(0)), offset);
            if start <= cluster_by && cluster_by <= end {
                let row = batch
                    .columns()
                    .iter()
                    .map(|column| get(Arc::deref(column), offset))
                    .collect();
                return Some(row);
            }
        } else {
            offset -= batch.num_rows()
        }
    }
    None
}

fn cluster_by_key(column: &dyn Array, offset: usize) -> Key {
    match column.data_type() {
        DataType::Boolean => {
            let column = column.as_any().downcast_ref::<BooleanArray>().unwrap();
            let value: bool = column.value(offset);
            bool_key(value)
        }
        DataType::Int64 => {
            let column = column.as_any().downcast_ref::<Int64Array>().unwrap();
            let value: i64 = column.value(offset);
            i64_key(value)
        }
        DataType::Float64 => {
            let column = column.as_any().downcast_ref::<Float64Array>().unwrap();
            let value: f64 = column.value(offset);
            f64_key(value)
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let column = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let value: i64 = column.value(offset);
            i64_key(value)
        }
        DataType::Date32(DateUnit::Day) => {
            let column = column.as_any().downcast_ref::<Date32Array>().unwrap();
            let value: i32 = column.value(offset);
            i32_key(value)
        }
        DataType::Utf8 => todo!(),
        other => panic!("{:?}", other),
    }
}

fn get(column: &dyn Array, offset: usize) -> Box<dyn Any> {
    match column.data_type() {
        DataType::Boolean => {
            let column = column.as_any().downcast_ref::<BooleanArray>().unwrap();
            Box::new(column.value(offset))
        }
        DataType::Int64 => {
            let column = column.as_any().downcast_ref::<Int64Array>().unwrap();
            Box::new(column.value(offset))
        }
        DataType::Float64 => {
            let column = column.as_any().downcast_ref::<Float64Array>().unwrap();
            Box::new(column.value(offset))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let column = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Box::new(column.value(offset))
        }
        DataType::Date32(DateUnit::Day) => {
            let column = column.as_any().downcast_ref::<Date32Array>().unwrap();
            Box::new(column.value(offset))
        }
        DataType::Utf8 => todo!(),
        other => panic!("{:?}", other),
    }
}

fn insert_into_new_pages(
    input: &Vec<RecordBatch>,
    offset: usize,
    start: Key,
    end: Key,
    index: &mut BTreeMap<Key, Vec<Page>>,
    txn: u64,
) {
    todo!()
}

fn range_inclusive(range: impl RangeBounds<Key>) -> Option<(Key, Key)> {
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
    Some((range_start, range_end))
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
