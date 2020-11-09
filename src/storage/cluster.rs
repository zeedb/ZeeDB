use crate::bits::*;
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
    index: RwLock<BTreeMap<Key, Vec<Page>>>,
}

type Key = [u8; 8];

impl Cluster {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            schema,
            index: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn select(&self, range: impl RangeBounds<Key>) -> Vec<RecordBatchRef> {
        self.index
            .read()
            .unwrap()
            .range(range)
            .flat_map(|(_, heap)| heap)
            .map(|page| page.select())
            .collect()
    }

    pub fn update(&self, range: impl RangeBounds<Key>) -> Vec<Pax> {
        self.index
            .write()
            .unwrap()
            .range_mut(range)
            .flat_map(|(_, heap)| heap)
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

    pub fn insert(&self, range: impl RangeBounds<Key>, input: Vec<RecordBatch>) {
        todo!()
    }
}

fn insert_into_existing_page(
    page: &Page,
    range: impl RangeBounds<Key>,
    input: &Vec<RecordBatch>,
    valid: Bits,
) {
    todo!()
}

fn insert_into_new_pages(
    index: &mut BTreeMap<Key, Vec<Page>>,
    range: impl RangeBounds<Key>,
    input: &Vec<RecordBatch>,
    valid: Bits,
) {
    todo!()
}
