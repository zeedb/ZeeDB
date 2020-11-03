use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::any::Any;
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

impl Page {
    pub fn select(&self, columns: &Vec<usize>) -> RecordBatch {
        match self {
            Page::Mutable(pax) => pax.select(columns),
            Page::Frozen(arrow) => arrow.select(columns),
        }
    }
}

impl Pax {
    // allocate a mutable page that can hold PAGE_SIZE tuples.
    fn empty(schema: &Arc<Schema>) -> Self {
        todo!()
    }

    fn freeze(&self) -> Arrow {
        todo!()
    }

    pub fn select(&self, columns: &Vec<usize>) -> RecordBatch {
        todo!()
    }

    fn insert(&self, values: &Vec<RecordBatch>) -> usize {
        todo!()
    }
}

impl Arrow {
    fn melt(&self) -> Pax {
        todo!()
    }

    pub fn select(&self, columns: &Vec<usize>) -> RecordBatch {
        todo!()
    }
}
