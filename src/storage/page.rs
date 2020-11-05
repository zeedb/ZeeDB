use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

const PAGE_SIZE: usize = 1024;

#[derive(Debug)]
pub enum Page {
    Mutable(Pax),
    Frozen(Arrow),
}

// Mutable pages are in PAX layout with strings in a separate buffer at the end of the page.
//
// They contain a special column UNDO which holds a physical pointer to an undo record,
// which indicates that the record has been updated or deleted by the writer,
// which supports snapshot-isolation read-only queries without blocking writes.
//
// A  B  UNDO
// 1  x  null      A  B
// 2  y  +-------> 1  yy
// 3  z  null
#[derive(Debug)]
pub struct Pax {
    buffer: *mut u8,
    strings: *mut u8,
}

// Frozen pages are in arrow layout with strings inline.
#[derive(Debug)]
pub struct Arrow {
    buffer: *const u8,
}

// Undo temporarily stores undo information for tuples that have been modified by uncommitted transactions,
// to support snapshot-isolation queries without blocking writes.
pub struct Undo {
    txn: u64,
    deleted: bool,
    updated: Vec<(usize, Box<dyn Any>)>,
}

impl Pax {
    // allocate a mutable page that can hold PAGE_SIZE tuples.
    pub fn empty(schema: &Arc<Schema>) -> Self {
        todo!()
    }

    pub fn freeze(&self) -> Arrow {
        todo!()
    }
}

impl Arrow {
    pub fn melt(&self) -> Pax {
        todo!()
    }
}

// Reference to a RecordBatch that also maintains a reference to the underlying Page.
pub struct RecordBatchRef {
    batch: RecordBatch,
    // Ensure that the memory referenced by batch doesn't get reclaimed.
    _page: Arc<Page>,
}

impl RecordBatchRef {
    pub fn new(page: Arc<Page>) -> Self {
        Self {
            batch: match page.deref() {
                Page::Mutable(pax) => todo!(),
                Page::Frozen(arrow) => todo!(),
            },
            _page: page,
        }
    }
}

impl Deref for RecordBatchRef {
    type Target = RecordBatch;

    fn deref(&self) -> &Self::Target {
        &self.batch
    }
}

// Reference to a Pax that delegates to an underlying Page.
pub struct PaxRef {
    page: Arc<Page>,
}

impl PaxRef {
    pub fn new(page: Arc<Page>) -> Self {
        Self { page }
    }
}

impl Deref for PaxRef {
    type Target = Pax;

    fn deref(&self) -> &Self::Target {
        match self.page.deref() {
            Page::Mutable(pax) => pax,
            Page::Frozen(_) => panic!(),
        }
    }
}
