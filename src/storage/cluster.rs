use crate::page::*;
use crate::trie::*;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::mem;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

// Cluster represents a logical table as a list of heaps, clustered on a non-unique key.
// A good cluster-by key will cluster frequently-updated rows together.
pub struct Cluster {
    // The first column is the cluster-by key.
    schema: Arc<Schema>,
    index: AtomicPtr<Arc<Trie<Heap>>>,
}

struct Heap {
    pages: Vec<Page>,
}

type Key = [u8; 8];

impl Cluster {
    pub fn new(schema: Arc<Schema>) -> Self {
        let mut trie = Arc::new(Trie::empty());
        let index = AtomicPtr::new(&mut trie);
        mem::forget(trie);
        Self { schema, index }
    }

    pub fn select<R: RangeBounds<Key> + Clone>(&self, range: R) -> PageIter {
        let snapshot = self.load();
        PageIter::new(self, snapshot)
    }

    pub fn update<R: RangeBounds<Key> + Clone>(&self, range: R) -> PaxIter {
        // Get the current state of the index at the start of the operation.
        let current = self.index.load(Ordering::Relaxed);
        let snapshot = unsafe { &*current }.clone();
        // Scan the range, checking for pages that need melting.
        let mut new = None;
        for (key, heap) in snapshot.range(range.clone()) {
            if let Some(heap) = heap.melt() {
                new = match new {
                    None => Some(snapshot.remove(key).unwrap().insert(key, heap)),
                    Some(new) => Some(new.remove(key).unwrap().insert(key, heap)),
                }
            }
        }
        // If we had to modify the index, do a compare-and-swap.
        let snapshot = match new {
            Some(new) => {
                let new = Arc::new(new);
                if self.swap(current, new.clone()) {
                    new
                } else {
                    // If another thread concurrently modified the index, start over.
                    return self.update(range);
                }
            }
            None => snapshot.clone(),
        }
        .clone();
        // Now that we have guaranteed all pages are melted, fetch them.
        todo!()
    }

    fn load(&self) -> Arc<Trie<Heap>> {
        unsafe { &*self.index.load(Ordering::Relaxed) }.clone()
    }

    fn swap(&self, current: *mut Arc<Trie<Heap>>, mut new: Arc<Trie<Heap>>) -> bool {
        let removed = self
            .index
            .compare_and_swap(current, &mut new, Ordering::Relaxed);
        if removed == current {
            mem::drop(current);
            mem::forget(new);
            true
        } else {
            false
            // drop new, and its contents
        }
    }

    pub fn insert(&self, input: Vec<RecordBatch>) {
        todo!()
    }
}

impl Heap {
    fn empty(schema: &Arc<Schema>) -> Self {
        Self { pages: vec![] }
    }

    fn melt(&self) -> Option<Heap> {
        todo!()
    }

    fn select(&self, columns: &Vec<usize>) -> Vec<RecordBatch> {
        self.pages
            .iter()
            .map(|page| match page {
                Page::Mutable(pax) => pax.select(columns),
                Page::Frozen(arrow) => arrow.select(columns),
            })
            .collect()
    }
}

pub struct HeapIter {
    parent: Arc<Trie<Heap>>,
    next: Option<Key>,
    end: Key,
}

impl HeapIter {
    fn new(parent: Arc<Trie<Heap>>, start: Key, end: Key) -> Self {
        Self {
            parent,
            next: Some(start),
            end,
        }
    }

    fn empty(parent: Arc<Trie<Heap>>) -> Self {
        Self {
            parent,
            next: None,
            end: [0; 8],
        }
    }
}

impl Iterator for HeapIter {
    type Item = Arc<Heap>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(start) = mem::replace(&mut self.next, None) {
            if let Some((key, value)) = self.parent.find(start) {
                if key <= self.end {
                    self.next = if key == [u8::MAX; 8] {
                        None
                    } else {
                        Some((u64::from_be_bytes(key) + 1).to_be_bytes())
                    };
                    return Some(value.clone());
                }
            }
        }
        None
    }
}

pub struct PageIter<'a> {
    parent: &'a Cluster,
    offset: usize,
    head: Arc<Heap>,
    tail: HeapIter,
}

impl<'a> PageIter<'a> {
    fn new(parent: &'a Cluster, snapshot: Arc<Trie<Heap>>, start: Key, end: Key) -> Self {
        let tail = HeapIter::new(snapshot, start, end);
        if let Some(head) = tail.next() {
            Self {
                parent,
                offset: 0,
                head,
                tail,
            }
        } else {
            todo!()
        }
    }
}

impl<'a> Iterator for PageIter<'a> {
    type Item = &'a Page;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset < self.head.pages.len() {
            self.offset += 1;
            Some(&self.head.pages[self.offset - 1])
        } else if let Some(head) = self.tail.next() {
            self.offset = 0;
            self.head = head;
            self.next()
        } else {
            None
        }
    }
}
