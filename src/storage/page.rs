use crate::byte_key::*;
use arrow::array::*;
use arrow::buffer::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::alloc::{alloc, dealloc, Layout};
use std::fmt::{Debug, Display};
use std::mem::size_of;
use std::ops::Deref;
use std::ops::RangeInclusive;
use std::sync::atomic::*;
use std::sync::Arc;

pub const PAGE_SIZE: usize = 1024;

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
//
// Stats are stored at the end of the page.
pub struct Page {
    schema: Arc<Schema>,
    columns: Vec<*mut u8>,
    xmin: *mut AtomicU64,
    xmax: *mut AtomicU64,
    stats: *mut u8,
    buffer: *mut u8,
    capacity: usize,
}

const OFFSET_NUM_ROWS: isize = 0;
const OFFSET_MIN_CLUSTER_BY: isize = size_of::<usize>() as isize;
const OFFSET_MAX_CLUSTER_BY: isize = OFFSET_MIN_CLUSTER_BY + size_of::<u64>() as isize;
const STATS_LENGTH: usize = OFFSET_MAX_CLUSTER_BY as usize + size_of::<u64>();

// Reference to a page that ensures it doesn't go out of memory.
pub struct Reference<'a> {
    value: RecordBatch,
    _page: &'a Page,
}

impl Page {
    // Allocate a mutable page that can hold PAGE_SIZE tuples.
    pub fn empty(schema: Arc<Schema>) -> Self {
        let mut capacity = 0;
        let mut column_offsets = vec![];
        for field in schema.fields() {
            column_offsets.push(capacity);
            // If column is nullable, allocate a null bitmap.
            if field.is_nullable() {
                capacity = align(capacity + PAGE_SIZE / 8);
            }
            // Allocate space for the values in their pax representation.
            if let DataType::Utf8 = field.data_type() {
                capacity = align(capacity + pax_length(field.data_type(), PAGE_SIZE));
            } else {
                capacity = align(capacity + pax_length(field.data_type(), PAGE_SIZE));
            }
        }
        let xmin_offset = capacity;
        capacity = align(capacity + PAGE_SIZE * size_of::<u64>());
        let xmax_offset = capacity;
        capacity = align(capacity + PAGE_SIZE * size_of::<u64>());
        let stats_offset = capacity;
        capacity = capacity + STATS_LENGTH;
        // Allocate memory.
        let buffer = unsafe { alloc(Layout::from_size_align(capacity, 1).unwrap()) };
        Self {
            schema,
            columns: column_offsets
                .iter()
                .map(|offset| unsafe { buffer.offset(*offset as isize) })
                .collect(),
            xmin: unsafe { buffer.offset(xmin_offset as isize) as *mut AtomicU64 },
            xmax: unsafe { buffer.offset(xmax_offset as isize) as *mut AtomicU64 },
            stats: unsafe { buffer.offset(stats_offset as isize) },
            buffer,
            capacity,
        }
    }

    pub fn select(&self) -> Reference {
        let num_rows = self.num_rows().load(Ordering::Relaxed);
        // View data columns as arrow format.
        let mut columns: Vec<Arc<dyn Array>> = (0..self.columns.len())
            .map(|i| head(self.schema.field(i), self.columns[i], num_rows))
            .collect();
        // Add system columns.
        let xmin = xcolumn("$xmin");
        columns.push(head(&xmin, self.xmin as *mut u8, num_rows));
        let xmax = xcolumn("$xmax");
        columns.push(head(&xmax, self.xmax as *mut u8, num_rows));
        // Add system columns to schema.
        let mut fields = self.schema.fields().clone();
        fields.push(xmin);
        fields.push(xmax);
        // Wrap RecordBatch in a reference that ensures the underlying buffer doesn't get destroyed.
        Reference {
            value: RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
            _page: self,
        }
    }

    pub fn insert(&self, records: &RecordBatch, txn: u64) -> usize {
        let (start, end) = self.reserve(records.num_rows());
        // Write the new row in the reserved slot.
        for i in 0..records.num_columns() {
            extend(self.columns[i], start, end, records.column(i).deref())
        }
        // Update the stats.
        let (min, max) = min_max(records.column(0).deref());
        self.min_cluster_by().fetch_min(min, Ordering::Relaxed);
        self.max_cluster_by().fetch_max(max, Ordering::Relaxed);
        // Make the rows visible to subsequent transactions.
        unsafe {
            for i in 0..records.num_rows() {
                let xmin = &*self.xmin.offset(i as isize);
                let xmax = &*self.xmax.offset(i as isize);
                xmin.store(txn, Ordering::Relaxed);
                xmax.store(u64::MAX, Ordering::Relaxed);
            }
        }
        end
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

    fn reserve(&self, request: usize) -> (usize, usize) {
        let start = self.num_rows().load(Ordering::Relaxed);
        let end = start + request;
        // If there's not enough space for request, take whatever is available.
        if end > PAGE_SIZE {
            let start = self.num_rows().swap(PAGE_SIZE, Ordering::Relaxed);
            return (start, PAGE_SIZE);
        }
        // If someone else concurrently reserves rows, try again.
        if self
            .num_rows()
            .compare_and_swap(start, end, Ordering::Relaxed)
            != start
        {
            return self.reserve(request);
        }
        // Otherwise, we have successfully reserved a segment of the page.
        (start, end)
    }

    fn num_rows(&self) -> &mut AtomicUsize {
        unsafe { &mut *(self.stats.offset(OFFSET_NUM_ROWS) as *mut AtomicUsize) }
    }

    fn min_cluster_by(&self) -> &mut AtomicU64 {
        unsafe { &mut *(self.stats.offset(OFFSET_MIN_CLUSTER_BY) as *mut AtomicU64) }
    }

    fn max_cluster_by(&self) -> &mut AtomicU64 {
        unsafe { &mut *(self.stats.offset(OFFSET_MAX_CLUSTER_BY) as *mut AtomicU64) }
    }

    pub fn range(&self) -> RangeInclusive<[u8; 8]> {
        let min = self.min_cluster_by().load(Ordering::Relaxed).to_be_bytes();
        let max = self.max_cluster_by().load(Ordering::Relaxed).to_be_bytes();
        min..=max
    }
}

impl Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let schema = self.schema.to_string();
        let num_rows = self.num_rows().load(Ordering::Relaxed);
        write!(f, "Page {{ [{}] #{} }}", schema, num_rows)
    }
}

impl Display for Page {
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

impl Drop for Page {
    fn drop(&mut self) {
        unsafe {
            dealloc(
                self.buffer,
                Layout::from_size_align(self.capacity, 1).unwrap(),
            );
        }
    }
}

impl<'a> Deref for Reference<'a> {
    type Target = RecordBatch;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

fn xcolumn(name: &str) -> Field {
    Field::new(name, DataType::UInt64, false)
}

fn head(field: &Field, column: *mut u8, num_rows: usize) -> Arc<dyn Array> {
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

fn min_max(cluster_by: &dyn Array) -> (u64, u64) {
    match cluster_by.data_type() {
        DataType::Boolean => min_max_primitive::<BooleanType>(cluster_by),
        DataType::Int64 => min_max_primitive::<Int64Type>(cluster_by),
        DataType::Float64 => min_max_primitive::<Float64Type>(cluster_by),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            min_max_primitive::<TimestampMicrosecondType>(cluster_by)
        }
        DataType::Date32(DateUnit::Day) => min_max_primitive::<Date32Type>(cluster_by),
        DataType::Utf8 => todo!(),
        other => panic!("{:?}", other),
    }
}

fn min_max_primitive<T>(cluster_by: &dyn Array) -> (u64, u64)
where
    T: ArrowPrimitiveType,
    T::Native: ByteKey,
{
    let cluster_by = cluster_by
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap();
    let mut min = [0; 8];
    let mut max = [u8::MAX; 8];
    for i in 1..cluster_by.len() {
        let key = ByteKey::key(cluster_by.value(i));
        min = min.min(key);
        max = max.max(key);
    }
    (u64::from_be_bytes(min), u64::from_be_bytes(max))
}

fn extend(column: *mut u8, start: usize, end: usize, values: &dyn Array) {
    match values.data_type() {
        DataType::Boolean => extend_boolean(column, start, end, values),
        DataType::Int64 => extend_primitive::<Int64Type>(column, start, end, values),
        DataType::Float64 => extend_primitive::<Float64Type>(column, start, end, values),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            extend_primitive::<TimestampMicrosecondType>(column, start, end, values)
        }
        DataType::Date32(DateUnit::Day) => {
            extend_primitive::<Date32Type>(column, start, end, values)
        }
        DataType::Utf8 => todo!(),
        other => panic!("{:?}", other),
    }
}

fn extend_primitive<T: ArrowNumericType>(dst: *mut u8, start: usize, end: usize, src: &dyn Array) {
    let src = src
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap()
        .raw_values();
    let dst = dst as *mut <T as ArrowPrimitiveType>::Native;
    unsafe {
        std::ptr::copy_nonoverlapping(src, dst.offset(start as isize), end - start);
    }
}

fn extend_boolean(dst: *mut u8, start: usize, end: usize, src: &dyn Array) {
    let src: &BooleanArray = src.as_any().downcast_ref::<BooleanArray>().unwrap();
    for i in 0..(end - start) {
        set_bit(dst, start + i, src.value(i))
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
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            TimestampMicrosecondType::get_bit_width() / 8 * num_rows
        }
        DataType::Date32(DateUnit::Day) => Date32Type::get_bit_width() / 8 * num_rows,
        DataType::Utf8 => 4,   // offset width
        DataType::Binary => 4, // offset width
        other => panic!("{:?}", other),
    }
}

fn get_bit(data: *const u8, i: usize) -> bool {
    unsafe { (*data.add(i >> 3) & BIT_MASK[i & 7]) != 0 }
}

fn set_bit(data: *mut u8, i: usize, value: bool) {
    unsafe {
        if value {
            *data.add(i >> 3) |= BIT_MASK[i & 7];
        } else {
            *data.add(i >> 3) ^= BIT_MASK[i & 7];
        }
    }
}

static BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
