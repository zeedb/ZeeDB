use crate::isolation::*;
use arrow::array::*;
use arrow::buffer::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::alloc::{alloc, dealloc, Layout};
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
    // Capacity of buffer in bytes.
    capacity: usize,
    strings: *mut u8,
    // Capacity of strings in bytes.
    string_capacity: usize,
    // Length of page in rows. Capacity in rows is always PAGE_SIZE.
    len: usize,
}

// Frozen pages are in arrow layout with strings inline.
#[derive(Debug)]
pub struct Arrow {
    buffer: *mut u8,
    // Capacity of buffer in bytes.
    capacity: usize,
    // Length of page in rows.
    len: usize,
}

// Undo temporarily stores undo information for tuples that have been modified by uncommitted transactions,
// to support snapshot-isolation queries without blocking writes.
pub struct Undo {
    txn: u64,
    updated: Vec<(usize, Option<Box<dyn Any>>)>,
}

impl Pax {
    // allocate a mutable page that can hold PAGE_SIZE tuples.
    pub fn empty(schema: &Arc<Schema>) -> Self {
        let mut capacity = 0;
        for field in schema.fields() {
            if field.is_nullable() {
                capacity = align(capacity + PAGE_SIZE / 8);
            }
            capacity = align(capacity + PAGE_SIZE * bit_width(field.data_type()) / 8);
        }
        let buffer = unsafe { alloc(Layout::from_size_align(capacity, 1).unwrap()) };
        let string_capacity = schema
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                DataType::Boolean
                | DataType::Int64
                | DataType::Float64
                | DataType::Timestamp(_, _)
                | DataType::Date64(_) => 0,
                DataType::Utf8 => 32,
                other => panic!("{:?}", other),
            })
            .sum();
        let strings = unsafe { alloc(Layout::from_size_align(string_capacity, 1).unwrap()) };
        Self {
            buffer,
            capacity,
            strings,
            string_capacity,
            len: 0,
        }
    }

    pub fn insert(&self, input: &Vec<RecordBatch>, offset: usize) -> usize {
        todo!()
    }

    pub fn freeze(&self) -> Arrow {
        todo!()
    }
}

impl Drop for Pax {
    fn drop(&mut self) {
        unsafe {
            dealloc(
                self.buffer,
                Layout::from_size_align(self.capacity, 1).unwrap(),
            );
            dealloc(
                self.strings,
                Layout::from_size_align(self.string_capacity, 1).unwrap(),
            );
        }
    }
}

impl Arrow {
    pub fn melt(&self) -> Pax {
        todo!()
    }
}

impl Drop for Arrow {
    fn drop(&mut self) {
        unsafe {
            dealloc(
                self.buffer,
                Layout::from_size_align(self.capacity, 1).unwrap(),
            );
        }
    }
}

// Reference to a RecordBatch that also maintains a reference to the underlying Page.
pub struct RecordBatchRef {
    batch: RecordBatch,
    // Ensure that the memory referenced by batch doesn't get reclaimed.
    _page: Arc<Page>,
}

impl RecordBatchRef {
    pub fn new(schema: Arc<Schema>, page: Arc<Page>, isolation: Isolation) -> Self {
        match page.clone().deref() {
            Page::Mutable(pax) => mutable_record_batch(schema, page, pax, isolation),
            Page::Frozen(arrow) => frozen_record_batch(schema, page, arrow),
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

struct RawColumn {
    not_null: Option<*mut u8>,
    values: *mut u8,
}

impl RawColumn {
    fn new(buffer: *mut u8, offset: &mut usize, len: usize, field: &Field) -> Self {
        // If the field is nullable, add a bit mask for nulls.
        let not_null = if field.is_nullable() {
            let copy = copy_slice(buffer, *offset, len / 8);
            *offset = align(*offset + PAGE_SIZE / 8);
            Some(copy)
        } else {
            None
        };
        // Add an array of values.
        let values = copy_slice(buffer, *offset, len * bit_width(field.data_type()) / 8);
        *offset = align(*offset + PAGE_SIZE * bit_width(field.data_type()) / 8);
        // Combine.
        Self { not_null, values }
    }

    fn set_not_null(&mut self, i: usize, value: bool) {
        match (self.not_null, value) {
            (Some(not_null), true) => unsafe { set_bit(not_null, i) },
            (Some(not_null), false) => unsafe { unset_bit(not_null, i) },
            (None, true) => panic!(),
            (None, false) => {}
        }
    }

    fn set(&mut self, i: usize, data: &DataType, value: Option<Box<dyn Any>>) {
        if let Some(value) = value {
            self.set_not_null(i, true);
            match data {
                DataType::Boolean => todo!(),
                DataType::Int64 => {
                    let left = self.values as *mut i64;
                    let right = *value.downcast_ref::<i64>().unwrap();
                    unsafe {
                        left.offset(i as isize).write(right);
                    }
                }
                DataType::Float64 => todo!(),
                DataType::Timestamp(_, _) => todo!(),
                DataType::Date64(_) => todo!(),
                DataType::Utf8 => todo!(),
                other => panic!("{:?}", other),
            }
        } else {
            self.set_not_null(i, false);
        }
    }

    fn freeze(self, data: &DataType, len: usize) -> Arc<dyn Array> {
        let mut builder = ArrayData::builder(data.clone());
        // If the field is nullable, add a bit mask for nulls.
        if let Some(not_null) = self.not_null {
            let buffer = unsafe { Buffer::from_raw_parts(not_null, len / 8, PAGE_SIZE / 8) };
            builder = builder.null_bit_buffer(buffer);
        }
        // Add an array of values.
        let buffer = unsafe {
            Buffer::from_raw_parts(
                self.values,
                len * bit_width(data) / 8,
                PAGE_SIZE * bit_width(data) / 8,
            )
        };
        builder = builder.add_buffer(buffer);
        // Append bit mask and array.
        make_array(builder.build())
    }
}

struct UndoColumn {
    values: *mut *const Undo,
}

impl UndoColumn {
    fn new(buffer: *mut u8, offset: usize) -> Self {
        unsafe {
            let values = buffer.offset(offset as isize) as *mut *const Undo;
            Self { values }
        }
    }

    fn get(&self, i: usize) -> Option<Undo> {
        unsafe {
            let ptr = self.values.offset(i as isize);
            if ptr.is_null() {
                None
            } else {
                Some(ptr.read().read())
            }
        }
    }
}

fn mutable_record_batch(
    schema: Arc<Schema>,
    page: Arc<Page>,
    pax: &Pax,
    isolation: Isolation,
) -> RecordBatchRef {
    // Read latest data from columns.
    let mut offset = 0;
    let mut columns: Vec<RawColumn> = schema
        .fields()
        .iter()
        .map(|field| RawColumn::new(pax.buffer, &mut offset, pax.len, field))
        .collect();
    // If we are reading in snapshot isolation, apply the undo logs.
    if let Isolation::Snapshot(txn) = isolation {
        // Read undo column.
        let undo_column = UndoColumn::new(pax.buffer, offset);
        // Apply undo logs.
        for i in 0..PAGE_SIZE {
            if let Some(undo) = undo_column.get(i) {
                if undo.txn > txn {
                    for (c, value) in undo.updated {
                        columns[c].set(i, schema.field(c).data_type(), value);
                    }
                }
            }
        }
    }
    // Convert RawColumn to Array.
    let arrays = columns
        .drain(..)
        .enumerate()
        .map(|(i, column)| column.freeze(schema.field(i).data_type(), pax.len))
        .collect();
    RecordBatchRef {
        batch: RecordBatch::try_new(schema, arrays).unwrap(),
        _page: page.clone(),
    }
}

fn frozen_record_batch(schema: Arc<Schema>, page: Arc<Page>, arrow: &Arrow) -> RecordBatchRef {
    let mut offset = 0;
    let mut arrays = vec![];
    for field in schema.fields() {
        let mut builder = ArrayData::builder(field.data_type().clone());
        // If the field is nullable, add a bit mask for nulls.
        if field.is_nullable() {
            let bytes = arrow.len / 8;
            let buffer =
                unsafe { Buffer::from_unowned(arrow.buffer.offset(offset as isize), bytes, bytes) };
            builder = builder.null_bit_buffer(buffer);
            offset = align(offset + bytes);
        }
        // Add an array of values.
        let bytes = arrow.len * bit_width(field.data_type()) / 8;
        let buffer =
            unsafe { Buffer::from_unowned(arrow.buffer.offset(offset as isize), bytes, bytes) };
        builder = builder.add_buffer(buffer);
        offset = align(offset + bytes);
        // Append bit mask and array.
        arrays.push(make_array(builder.build()));
    }
    RecordBatchRef {
        batch: RecordBatch::try_new(schema, arrays).unwrap(),
        _page: page.clone(),
    }
}

fn copy_slice(buffer: *const u8, offset: usize, len: usize) -> *mut u8 {
    unsafe {
        let dest = alloc(Layout::from_size_align(len, 1).unwrap());
        let source = buffer.offset(offset as isize);
        source.copy_to_nonoverlapping(dest, len);
        dest
    }
}

fn bit_width(data: &DataType) -> usize {
    match data {
        DataType::Boolean => 1,
        DataType::Int64 => Int64Type::get_bit_width(),
        DataType::Float64 => Float64Type::get_bit_width(),
        DataType::Timestamp(_, _) => TimestampMicrosecondType::get_bit_width(),
        DataType::Date64(_) => Date64Type::get_bit_width(),
        DataType::Utf8 => todo!(),
        other => panic!("{:?}", other),
    }
}

fn align(offset: usize) -> usize {
    (offset + 64 - 1) / 64
}

static BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];

unsafe fn set_bit(data: *mut u8, i: usize) {
    *data.add(i >> 3) |= BIT_MASK[i & 7];
}

unsafe fn unset_bit(data: *mut u8, i: usize) {
    *data.add(i >> 3) ^= BIT_MASK[i & 7];
}
