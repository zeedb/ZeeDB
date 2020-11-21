use crate::byte_key::*;
use arrow::array::*;
use arrow::buffer::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use ast::Column;
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
    columns: Vec<usize>,
    xmin: usize,
    xmax: usize,
    stats: usize,
    columns_buffer: Buffer,
    string_buffers: Vec<Option<Buffer>>,
}

const OFFSET_NUM_ROWS: usize = 0;
const OFFSET_MIN_CLUSTER_BY: usize = size_of::<usize>();
const OFFSET_MAX_CLUSTER_BY: usize = OFFSET_MIN_CLUSTER_BY + size_of::<u64>();
const STATS_LENGTH: usize = OFFSET_MAX_CLUSTER_BY as usize + size_of::<u64>();
const INITIAL_STRING_CAPACITY: usize = PAGE_SIZE * 10;

impl Page {
    // Allocate a mutable page that can hold PAGE_SIZE tuples.
    pub fn empty(schema: Arc<Schema>) -> Self {
        let mut capacity = 0;
        let mut columns = vec![];
        for field in schema.fields() {
            columns.push(capacity);
            // If column is nullable, allocate a null bitmap.
            if field.is_nullable() {
                capacity = align(capacity + PAGE_SIZE / 8);
            }
            // Allocate space for the values in their pax representation.
            capacity = align(capacity + pax_length(field.data_type(), PAGE_SIZE));
        }
        let xmin = capacity;
        capacity = align(capacity + PAGE_SIZE * size_of::<u64>());
        let xmax = capacity;
        capacity = align(capacity + PAGE_SIZE * size_of::<u64>());
        let stats = capacity;
        capacity = capacity + STATS_LENGTH;
        // Allocate memory.
        let columns_buffer = zeros(capacity);
        let string_buffers: Vec<Option<Buffer>> = schema
            .fields()
            .iter()
            .map(|field| string_buffer(field.data_type()))
            .collect();
        Self {
            schema,
            columns,
            string_buffers,
            xmin,
            xmax,
            stats,
            columns_buffer,
        }
    }

    pub fn select(&self, projects: &Vec<Column>) -> RecordBatch {
        let num_rows = self.num_rows().load(Ordering::Relaxed);
        // View data columns as arrow format.
        let mut columns = vec![];
        let mut fields = vec![];
        for column in projects {
            let (i, field) = self
                .schema
                .fields()
                .iter()
                .enumerate()
                .find(|(_, field)| field.name() == &column.name)
                .unwrap();
            let field = Field::new(
                column.canonical_name().as_str(),
                field.data_type().clone(),
                field.is_nullable(),
            );
            let column = head(
                &field,
                self.columns[i],
                &self.columns_buffer,
                &self.string_buffers[i],
                num_rows,
            );
            columns.push(column);
            fields.push(field);
        }
        // Add system columns.
        let xmin = xcolumn("$xmin");
        columns.push(head(
            &xmin,
            self.xmin,
            &self.columns_buffer,
            &None,
            num_rows,
        ));
        fields.push(xmin);
        let xmax = xcolumn("$xmax");
        columns.push(head(
            &xmax,
            self.xmax,
            &self.columns_buffer,
            &None,
            num_rows,
        ));
        fields.push(xmax);
        // Wrap RecordBatch in a reference that ensures the underlying buffer doesn't get destroyed.
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    pub fn insert(&self, records: &RecordBatch, txn: u64) -> usize {
        let (start, end) = self.reserve(records.num_rows());
        // Write the new row in the reserved slot.
        for i in 0..records.num_columns() {
            extend(
                self.columns[i],
                &self.columns_buffer,
                &self.string_buffers[i],
                start,
                end,
                records.column(i).deref(),
            )
        }
        // Update the stats.
        let (min, max) = min_max(records.column(0).deref());
        self.min_cluster_by().fetch_min(min, Ordering::Relaxed);
        self.max_cluster_by().fetch_max(max, Ordering::Relaxed);
        // Make the rows visible to subsequent transactions.
        unsafe {
            let xmin = self.columns_buffer.slice(self.xmin).raw_data() as *const AtomicU64;
            let xmax = self.columns_buffer.slice(self.xmax).raw_data() as *const AtomicU64;
            for i in start..end {
                xmin.offset(i as isize)
                    .as_ref()
                    .unwrap()
                    .store(txn, Ordering::Relaxed);
                xmax.offset(i as isize)
                    .as_ref()
                    .unwrap()
                    .store(u64::MAX, Ordering::Relaxed);
            }
        }
        end - start
    }

    pub fn delete(&self, row: usize, txn: u64) -> bool {
        let xmax = self.xmax(row);
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

    fn num_rows(&self) -> &AtomicUsize {
        self.stat::<AtomicUsize>(self.stats + OFFSET_NUM_ROWS)
    }

    fn min_cluster_by(&self) -> &AtomicU64 {
        self.stat::<AtomicU64>(self.stats + OFFSET_MIN_CLUSTER_BY)
    }

    fn max_cluster_by(&self) -> &AtomicU64 {
        self.stat::<AtomicU64>(self.stats + OFFSET_MAX_CLUSTER_BY)
    }

    fn xmin(&self, row: usize) -> &AtomicU64 {
        self.column::<AtomicU64>(self.xmin, row)
    }

    fn xmax(&self, row: usize) -> &AtomicU64 {
        self.column::<AtomicU64>(self.xmax, row)
    }

    fn stat<T>(&self, offset: usize) -> &T {
        unsafe {
            let ptr = self.columns_buffer.raw_data().offset(offset as isize) as *const T;
            ptr.as_ref().unwrap()
        }
    }

    fn column<T>(&self, offset: usize, row: usize) -> &T {
        unsafe {
            let ptr = self.columns_buffer.raw_data().offset(offset as isize) as *const T;
            ptr.offset(row as isize).as_ref().unwrap()
        }
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
        let record_batch = self.select(&all_columns(self.schema.as_ref()));
        let mut csv_bytes = vec![];
        arrow::csv::Writer::new(&mut csv_bytes)
            .write(&record_batch)
            .unwrap();
        let csv = String::from_utf8(csv_bytes).unwrap();
        f.write_str(csv.as_str())
    }
}

fn all_columns(schema: &Schema) -> Vec<Column> {
    schema
        .fields()
        .iter()
        .map(|field| Column {
            created: ast::Phase::Plan,
            id: 0,
            name: field.name().clone(),
            table: None,
            data: field.data_type().clone(),
        })
        .collect()
}

fn string_buffer(data: &DataType) -> Option<Buffer> {
    match data {
        DataType::Utf8 => Some(zeros(INITIAL_STRING_CAPACITY)),
        _ => None,
    }
}

fn zeros(capacity: usize) -> Buffer {
    let mut buffer = MutableBuffer::new(capacity);
    buffer.resize(capacity).unwrap();
    buffer.freeze()
}

fn xcolumn(name: &str) -> Field {
    Field::new(name, DataType::UInt64, false)
}

fn head(
    field: &Field,
    mut column_offset: usize,
    columns_buffer: &Buffer,
    string_buffer: &Option<Buffer>,
    len: usize,
) -> Arc<dyn Array> {
    let mut array = ArrayDataBuilder::new(field.data_type().clone()).len(len);
    // If column is nullable, add a null buffer and offset the values buffer.
    if field.is_nullable() {
        let null_buffer = columns_buffer.slice(column_offset);
        array = array.null_bit_buffer(null_buffer);
        column_offset = align(column_offset + pax_length(&DataType::Boolean, PAGE_SIZE))
    }
    // Add the values buffer.
    let values_buffer = columns_buffer.slice(column_offset);
    array = array.add_buffer(values_buffer);
    // If column is a string, add a string buffer.
    if let Some(string_buffer) = string_buffer.as_ref() {
        array = array.add_buffer(string_buffer.clone());
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
        DataType::Utf8 => panic!("STRING is not supported as a CLUSTER BY key"),
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

fn extend(
    column_offset: usize,
    columns_buffer: &Buffer,
    string_buffer: &Option<Buffer>,
    start: usize,
    end: usize,
    values: &dyn Array,
) {
    match values.data_type() {
        DataType::Boolean => extend_boolean(column_offset, columns_buffer, start, end, values),
        DataType::Int64 => {
            extend_primitive::<Int64Type>(column_offset, columns_buffer, start, end, values)
        }
        DataType::Float64 => {
            extend_primitive::<Float64Type>(column_offset, columns_buffer, start, end, values)
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            extend_primitive::<TimestampMicrosecondType>(
                column_offset,
                columns_buffer,
                start,
                end,
                values,
            )
        }
        DataType::Date32(DateUnit::Day) => {
            extend_primitive::<Date32Type>(column_offset, columns_buffer, start, end, values)
        }
        DataType::Utf8 => extend_string(
            column_offset,
            columns_buffer,
            string_buffer.as_ref().unwrap(),
            start,
            end,
            values,
        ),
        other => panic!("{:?}", other),
    }
}

fn extend_primitive<T: ArrowNumericType>(
    column_offset: usize,
    columns_buffer: &Buffer,
    start: usize,
    end: usize,
    values: &dyn Array,
) {
    let dst = columns_buffer.slice(column_offset).raw_data() as *mut T::Native;
    let src = values
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap()
        .raw_values();
    let dst = dst as *mut <T as ArrowPrimitiveType>::Native;
    unsafe {
        std::ptr::copy_nonoverlapping(src, dst.offset(start as isize), end - start);
    }
}

fn extend_boolean(
    column_offset: usize,
    columns_buffer: &Buffer,
    start: usize,
    end: usize,
    src: &dyn Array,
) {
    let dst = columns_buffer.slice(column_offset).raw_data() as *mut u8;
    let src: &BooleanArray = src.as_any().downcast_ref::<BooleanArray>().unwrap();
    for i in 0..(end - start) {
        set_bit(dst, start + i, src.value(i))
    }
}

fn extend_string(
    column_offset: usize,
    columns_buffer: &Buffer,
    string_buffer: &Buffer,
    start: usize,
    end: usize,
    src: &dyn Array,
) {
    let dst_offsets = columns_buffer.slice(column_offset).raw_data() as *mut i32;
    let src: &StringArray = src.as_any().downcast_ref::<StringArray>().unwrap();
    let src_data = src.value_data().raw_data();
    // Copy offsets.
    for i in start..end {
        unsafe {
            *dst_offsets.offset((i + 1) as isize) =
                *dst_offsets.offset(i as isize) + src.value_length(i - start);
        }
    }
    // Copy data.
    let dst_data = string_buffer.raw_data() as *mut u8;
    // Start position in dst_data is given by the offset of the end of the last string, dst_offsets[start].
    let dst_data_start = if start == 0 {
        0
    } else {
        unsafe { *dst_offsets.offset(start as isize) }
    };
    // Length in dst_data is given by the total length of all strings in src.
    let src_data_len = src.value_offset(src.len());
    // Copy value bytes from src to dest.
    unsafe {
        std::ptr::copy_nonoverlapping(
            src_data,
            dst_data.offset(dst_data_start as isize),
            src_data_len as usize,
        );
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
        DataType::Utf8 => 4 * (num_rows + 1), // offset width
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
