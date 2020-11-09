use arrow::array::*;
use arrow::buffer::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::alloc::{alloc, dealloc, Layout};
use std::any::Any;
use std::fmt::Display;
use std::mem::size_of;
use std::ops::Deref;
use std::sync::atomic::*;
use std::sync::Arc;

const PAGE_SIZE: usize = 1024;

pub enum Page {
    Mutable(Pax),
    Frozen(Arrow),
}

// Mutable pages are in PAX layout with string values stored separately.
//
// They contain system columns XMIN and XMAX, which supports snapshot-isolation read-only queries without blocking writes.
// Because we only append to mutable pages oldest-to-newest, this format can be interpreted as Arrow, even for string columns.
//
// A  B  XMIN  XMAX
// 1  x  1     MAX
// 2  y  2     MAX
// 3  z  3     MAX
//       0     0
//       0     0
#[derive(Clone)]
pub struct Pax {
    schema: Arc<Schema>,
    columns: Vec<*mut u8>,
    num_rows: Arc<AtomicUsize>,
    xmin: *mut AtomicU64,
    xmax: *mut AtomicU64,
    _buffer: Arc<RawBuffer>,
}

// Frozen pages are in arrow layout with strings inline.
pub struct Arrow {
    schema: Arc<Schema>,
    columns: Vec<Arc<dyn Array>>,
    num_rows: usize,
    _buffer: Arc<RawBuffer>,
}

struct RawBuffer {
    bytes: *mut u8,
    capacity: usize,
}

// Reference to a RecordBatch that also maintains a reference to the underlying Page.
pub struct RecordBatchRef {
    batch: RecordBatch,
    // Ensure that the memory referenced by batch doesn't get reclaimed.
    _buffer: Arc<RawBuffer>,
}

impl Page {
    pub fn select(&self) -> RecordBatchRef {
        match self {
            Page::Mutable(pax) => pax.select(),
            Page::Frozen(arrow) => arrow.select(),
        }
    }
}

impl Pax {
    // Allocate a mutable page that can hold PAGE_SIZE tuples.
    pub fn empty(schema: &Arc<Schema>) -> Self {
        let mut column_capacity = 0;
        let mut column_offsets = vec![];
        for field in schema.fields() {
            column_offsets.push(column_capacity);
            // If column is nullable, allocate a null bitmap.
            if field.is_nullable() {
                column_capacity = align(column_capacity + PAGE_SIZE / 8);
            }
            // Allocate space for the values in their pax representation.
            if let DataType::Utf8 = field.data_type() {
                column_capacity = align(column_capacity + pax_length(field.data_type(), PAGE_SIZE));
            } else {
                column_capacity = align(column_capacity + pax_length(field.data_type(), PAGE_SIZE));
            }
        }
        let xmin_offset = column_capacity;
        column_capacity = align(column_capacity + PAGE_SIZE * size_of::<u64>());
        let xmax_offset = column_capacity;
        column_capacity = align(column_capacity + PAGE_SIZE * size_of::<u64>());
        // Allocate memory.
        let buffer = RawBuffer::new(column_capacity);
        Self {
            schema: schema.clone(),
            columns: column_offsets
                .iter()
                .map(|offset| unsafe { buffer.bytes.offset(*offset as isize) })
                .collect(),
            num_rows: Arc::new(AtomicUsize::new(0)),
            xmin: unsafe { buffer.bytes.offset(xmin_offset as isize) as *mut AtomicU64 },
            xmax: unsafe { buffer.bytes.offset(xmax_offset as isize) as *mut AtomicU64 },
            _buffer: Arc::new(buffer),
        }
    }

    pub fn freeze(&self) -> Arrow {
        todo!()
    }

    pub fn select(&self) -> RecordBatchRef {
        let num_rows = self.num_rows.load(Ordering::Relaxed);
        // View data columns as arrow format.
        let mut columns: Vec<Arc<dyn Array>> = (0..self.columns.len())
            .map(|i| select_column(self.schema.field(i), self.columns[i], num_rows))
            .collect();
        // Add system columns.
        let xmin = xcolumn("$xmin");
        columns.push(select_column(&xmin, self.xmin as *mut u8, num_rows));
        let xmax = xcolumn("$xmax");
        columns.push(select_column(&xmax, self.xmax as *mut u8, num_rows));
        // Add system columns to schema.
        let mut fields = self.schema.fields().clone();
        fields.push(xmin);
        fields.push(xmax);
        // Wrap RecordBatch in a reference that ensures the underlying buffer doesn't get destroyed.
        RecordBatchRef {
            batch: RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
            _buffer: self._buffer.clone(),
        }
    }

    pub fn insert(&self, mut tuple: Vec<Option<Box<dyn Any>>>, txn: u64) -> bool {
        let num_rows = self.num_rows.load(Ordering::Relaxed);
        // If there is no room left in the page, fail.
        if num_rows == PAGE_SIZE {
            return false;
        }
        // Reserve the next slot in the page.
        if self
            .num_rows
            .compare_and_swap(num_rows, num_rows + 1, Ordering::Relaxed)
            != num_rows
        {
            // If someone else concurrently reserved the next slot, try again.
            return self.insert(tuple, txn);
        }
        // Write the new row in the reserved slot.
        for (i, value) in tuple.drain(..).enumerate() {
            set(num_rows, self.schema.field(i), self.columns[i], value);
        }
        // Write the visibility system column
        unsafe {
            let xmin = &*self.xmin.offset(num_rows as isize);
            let xmax = &*self.xmax.offset(num_rows as isize);
            xmin.store(txn, Ordering::Relaxed);
            xmax.store(u64::MAX, Ordering::Relaxed);
        }
        true
    }

    pub fn delete(&self, row: usize, txn: u64) -> bool {
        let xmax = unsafe { &*self.xmax.offset(row as isize) };
        let current = xmax.load(Ordering::Relaxed);
        if current < txn {
            false
        } else if xmax.compare_and_swap(current, txn, Ordering::Relaxed) != current {
            self.delete(row, txn)
        } else {
            true
        }
    }
}

impl Display for Pax {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let record_batch = &*self.select();
        let mut csv_bytes = vec![];
        arrow::csv::Writer::new(&mut csv_bytes)
            .write(record_batch)
            .unwrap();
        let csv = String::from_utf8(csv_bytes).unwrap();
        f.write_str(csv.as_str())
    }
}

fn xcolumn(name: &str) -> Field {
    Field::new(name, DataType::UInt64, false)
}

fn select_column(field: &Field, column: *mut u8, num_rows: usize) -> Arc<dyn Array> {
    let mut array = ArrayDataBuilder::new(field.data_type().clone()).len(num_rows);
    // If column is nullable, add a null buffer and offset the values buffer.
    let values_offset = if field.is_nullable() {
        let null_buffer = unsafe {
            Buffer::from_unowned(
                column,
                pax_length(&DataType::Boolean, num_rows),
                pax_length(&DataType::Boolean, PAGE_SIZE),
            )
        };
        array = array.null_bit_buffer(null_buffer);
        align(pax_length(&DataType::Boolean, PAGE_SIZE))
    } else {
        0
    };
    // Add the values buffer.
    let values_buffer = unsafe {
        Buffer::from_unowned(
            column.offset(values_offset as isize),
            pax_length(field.data_type(), num_rows),
            pax_length(field.data_type(), PAGE_SIZE),
        )
    };
    array = array.add_buffer(values_buffer);
    // If column is a string, add a string buffer.
    if let DataType::Utf8 = field.data_type() {
        todo!()
    }
    make_array(array.build())
}

fn set(row: usize, field: &Field, column: *mut u8, value: Option<Box<dyn Any>>) {
    match (field.data_type(), value) {
        (_, None) => todo!(),
        (DataType::Boolean, Some(value)) => todo!(),
        (DataType::Int64, Some(value)) => {
            let column = column as *mut i64;
            let value = value.downcast_ref::<i64>().unwrap();
            unsafe {
                *column.offset(row as isize) = *value;
            }
        }
        (DataType::Float64, Some(value)) => todo!(),
        (DataType::Timestamp(_, _), Some(value)) => todo!(),
        (DataType::Date64(_), Some(value)) => todo!(),
        (DataType::Utf8, Some(value)) => todo!(),
        (other, _) => panic!("{:?}", other),
    }
}

impl Arrow {
    pub fn melt(&self) -> Pax {
        todo!()
    }

    pub fn select(&self) -> RecordBatchRef {
        RecordBatchRef {
            batch: RecordBatch::try_new(self.schema.clone(), self.columns.clone()).unwrap(),
            _buffer: self._buffer.clone(),
        }
    }
}

impl RawBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            bytes: unsafe { alloc(Layout::from_size_align(capacity, 1).unwrap()) },
            capacity,
        }
    }
}

impl Drop for RawBuffer {
    fn drop(&mut self) {
        unsafe {
            dealloc(
                self.bytes,
                Layout::from_size_align(self.capacity, 1).unwrap(),
            );
        }
    }
}

impl Deref for RecordBatchRef {
    type Target = RecordBatch;

    fn deref(&self) -> &Self::Target {
        &self.batch
    }
}

fn align(offset: usize) -> usize {
    ((offset + arrow::memory::ALIGNMENT - 1) / arrow::memory::ALIGNMENT) * arrow::memory::ALIGNMENT
}

fn pax_length(data: &DataType, num_rows: usize) -> usize {
    match data {
        DataType::Boolean => (num_rows + 8 - 1) / 8,
        DataType::Int64 => Int64Type::get_bit_width() / 8 * num_rows,
        DataType::UInt64 => UInt64Type::get_bit_width() / 8 * num_rows,
        DataType::Float64 => Float64Type::get_bit_width() / 8 * num_rows,
        DataType::Timestamp(_, _) => TimestampMicrosecondType::get_bit_width() / 8 * num_rows,
        DataType::Date64(_) => Date64Type::get_bit_width() / 8 * num_rows,
        DataType::Utf8 => 4, // offset width
        other => panic!("{:?}", other),
    }
}
