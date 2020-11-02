use crate::page::*;
use crate::trie::*;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::ops::RangeBounds;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

// Cluster represents a logical table as a list of heaps, clustered on a non-unique key.
// A good cluster-by key will cluster frequently-updated rows together.
pub struct Cluster {
    // The first column is the cluster-by key.
    schema: Arc<Schema>,
    index: Trie<Heap>,
}

struct Heap {
    pages: Vec<Page>,
}

type Key = [u8; 8];

impl Cluster {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            schema,
            index: Trie::new(),
        }
    }

    pub fn select(
        &self,
        range: impl RangeBounds<Key>,
        columns: &Vec<usize>,
        rows: &Option<BooleanArray>,
    ) -> Vec<RecordBatch> {
        self.index
            .range(range)
            .flat_map(|(_, heap)| heap.select(columns, rows))
            .collect()
    }

    pub fn update(&self, range: impl RangeBounds<Key>) -> Vec<Pax> {
        todo!()
    }

    pub fn insert(&self, input: Vec<RecordBatch>) {
        todo!()
    }
}

impl Heap {
    fn empty(schema: &Arc<Schema>) -> Self {
        Self { pages: vec![] }
    }

    fn select(&self, columns: &Vec<usize>, rows: &Option<BooleanArray>) -> Vec<RecordBatch> {
        self.pages
            .iter()
            .map(|page| match page {
                Page::Mutable(pax) => pax.select(columns, rows),
                Page::Frozen(arrow) => arrow.select(columns, rows),
            })
            .collect()
    }
}
