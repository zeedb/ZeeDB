use crate::isolation::*;
use crate::page::*;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::any::Any;
use std::collections::BTreeMap;
use std::ops::{Deref, RangeBounds};
use std::sync::{Arc, RwLock};

// Cluster represents a logical table as a list of heaps, clustered on a non-unique key.
// A good cluster-by key will cluster frequently-updated rows together.
pub struct Cluster {
    // The first column is the cluster-by key.
    schema: Arc<Schema>,
    // The clustering of the table is tracked by an immutable trie, which is under a mutable lock to permit re-organization.
    // The leaves of the trie are usually a single page, but can be arbitrarily large when many tuples have the same cluster-by key.
    index: RwLock<BTreeMap<Key, Vec<Arc<Page>>>>,
}

type Key = [u8; 8];

impl Cluster {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            schema,
            index: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn select(
        &self,
        range: impl RangeBounds<Key>,
        isolation: Isolation,
    ) -> Vec<RecordBatchRef> {
        self.index
            .read()
            .unwrap()
            .range(range)
            .flat_map(|(_, heap)| heap)
            .map(|page| RecordBatchRef::new(self.schema.clone(), page.clone(), isolation))
            .collect()
    }

    pub fn update(&self, range: impl RangeBounds<Key>) -> Vec<PaxRef> {
        self.index
            .write()
            .unwrap()
            .range_mut(range)
            .flat_map(|(_, heap)| heap)
            .map(|page| {
                if let Page::Frozen(arrow) = Arc::deref(page) {
                    *page = Arc::new(Page::Mutable(arrow.melt()));
                }
                PaxRef::new(page.clone())
            })
            .collect()
    }

    pub fn insert(&self, range: impl RangeBounds<Key>, input: Vec<RecordBatch>) {
        let mut lock = self.index.write().unwrap();
        let starts: Vec<Key> = lock.range(range).map(|(key, _)| key.clone()).collect();
        for start in starts {
            insert(&mut *lock, &self.schema, start, &input, 0);
        }
    }
}

fn insert(
    index: &mut BTreeMap<Key, Vec<Arc<Page>>>,
    schema: &Arc<Schema>,
    start: Key,
    input: &Vec<RecordBatch>,
    mut offset: usize,
) {
    for page in index.get_mut(&start).unwrap() {
        if offset < n_rows(input) {
            if let Page::Mutable(pax) = Arc::deref(page) {
                offset = pax.insert(input, offset);
            }
        }
    }
    if offset < n_rows(input) {
        merge(index, schema, start, input, offset);
    }
}

fn merge(
    index: &mut BTreeMap<Key, Vec<Arc<Page>>>,
    schema: &Arc<Schema>,
    start: Key,
    input: &Vec<RecordBatch>,
    offset: usize,
) {
    let existing_records: Vec<RecordBatchRef> = index
        .get(&start)
        .unwrap()
        .iter()
        .map(|page| RecordBatchRef::new(schema.clone(), page.clone(), Isolation::Serializable))
        .collect();
    let new_records: Vec<RecordBatch> = input
        .iter()
        .map(|batch| {
            let columns = batch
                .columns()
                .iter()
                .map(|column| column.slice(offset, column.len() - offset))
                .collect();
            RecordBatch::try_new(schema.clone(), columns).unwrap()
        })
        .collect();
    let pivot = median_cluster_by(&existing_records, &new_records);
    let (left, right) = split_on(pivot, &existing_records, &new_records);
    index.insert(start, left);
    index.insert(pivot, right);
}

fn median_cluster_by(
    existing_records: &Vec<RecordBatchRef>,
    new_records: &Vec<RecordBatch>,
) -> Key {
    todo!()
}

fn split_on(
    pivot: Key,
    existing_records: &Vec<RecordBatchRef>,
    new_records: &Vec<RecordBatch>,
) -> (Vec<Arc<Page>>, Vec<Arc<Page>>) {
    todo!()
}

fn n_rows(input: &Vec<RecordBatch>) -> usize {
    input.iter().map(|batch| batch.num_rows()).sum()
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
