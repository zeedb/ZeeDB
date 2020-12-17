use arrow::array::*;
use arrow::buffer::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::fmt;
use std::mem::size_of;
use std::ops::Deref;
use std::sync::atomic::*;
use std::sync::{Arc, RwLock};

pub const PAGE_SIZE: usize = 1024;

pub const INITIAL_STRING_CAPACITY: usize = PAGE_SIZE * 10;

#[derive(Clone)]
pub struct Page {
    inner: Arc<Inner>,
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
//
// Stats are stored at the end of the page.
struct Inner {
    pid: usize,
    schema: Arc<Schema>,
    columns: Vec<usize>,
    xmin: usize,
    xmax: usize,
    num_rows: usize,
    columns_buffer: Buffer,
    string_buffers: Vec<Option<RwLock<Buffer>>>,
}

impl Page {
    // Allocate a mutable page that can hold PAGE_SIZE tuples.
    pub fn empty(pid: usize, schema: Arc<Schema>) -> Self {
        let schema = Schema::new(schema.fields().iter().map(with_base_name).collect());
        let mut capacity = 0;
        let mut columns = vec![];
        for field in schema.fields() {
            columns.push(capacity);
            // Allocate a null bitmap.
            if field.is_nullable() {
                capacity = align(capacity + PAGE_SIZE / 8);
            }
            // Allocate space for the values in their pax representation.
            capacity = align(capacity + pax_length(field.data_type(), PAGE_SIZE));
        }
        let xmin = capacity;
        capacity = align(capacity + PAGE_SIZE * size_of::<i64>());
        let xmax = capacity;
        capacity = align(capacity + PAGE_SIZE * size_of::<i64>());
        let num_rows = capacity;
        capacity = align(capacity + size_of::<i64>());
        // Allocate memory.
        let columns_buffer = zeros(capacity);
        let string_buffers: Vec<Option<RwLock<Buffer>>> = schema
            .fields()
            .iter()
            .map(|field| string_buffer(field.data_type()))
            .collect();
        let inner = Inner {
            pid,
            schema: Arc::new(schema),
            columns,
            string_buffers,
            xmin,
            xmax,
            num_rows,
            columns_buffer,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.inner.schema.clone()
    }

    pub fn select(&self, projects: &Vec<String>) -> RecordBatch {
        let num_rows = self.num_rows().load(Ordering::Relaxed);
        // View data columns as arrow format.
        let mut columns = vec![];
        let mut fields = vec![];
        for project in projects {
            match base_name(project) {
                "$xmin" => {
                    columns.push(head(
                        self.inner.xmin,
                        &self.inner.columns_buffer,
                        &None,
                        num_rows,
                        DataType::Int64,
                        false,
                    ));
                    fields.push(Field::new(project, DataType::Int64, false));
                }
                "$xmax" => {
                    columns.push(head(
                        self.inner.xmax,
                        &self.inner.columns_buffer,
                        &None,
                        num_rows,
                        DataType::Int64,
                        false,
                    ));
                    fields.push(Field::new(project, DataType::Int64, false));
                }
                "$tid" => {
                    let start = self.inner.pid * PAGE_SIZE;
                    let end = start + num_rows;
                    let tids: Vec<i64> = (start as i64..end as i64).collect();
                    columns.push(Arc::new(Int64Array::from(tids)));
                    fields.push(Field::new(project, DataType::Int64, false));
                }
                name => {
                    if let Some(i) = self
                        .inner
                        .schema
                        .fields()
                        .iter()
                        .position(|f| f.name() == name)
                    {
                        let field = self.inner.schema.field(i);
                        let column = head(
                            self.inner.columns[i],
                            &self.inner.columns_buffer,
                            &self.inner.string_buffers[i],
                            num_rows,
                            field.data_type().clone(),
                            field.is_nullable(),
                        );
                        columns.push(column);
                        fields.push(Field::new(
                            project,
                            field.data_type().clone(),
                            field.is_nullable(),
                        ));
                    } else {
                        panic!("{} is not in {:?}", name, self.inner.schema.fields())
                    }
                }
            }
        }
        // Wrap RecordBatch in a reference that ensures the underlying buffer doesn't get destroyed.
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    pub fn star(&self) -> Vec<String> {
        self.inner
            .schema
            .fields()
            .iter()
            .map(|field| base_name(field.name()).to_string())
            .collect()
    }

    pub fn insert(
        &self,
        records: &RecordBatch,
        txn: i64,
        tids: &mut Int64Builder,
        offset: &mut usize,
    ) {
        let (start, end) = self.reserve(records.num_rows());
        // Write the new rows in the reserved slots.
        for (dst, dst_field) in self.inner.schema.fields().iter().enumerate() {
            for (src, src_field) in records.schema().fields().iter().enumerate() {
                if base_name(src_field.name()) == base_name(dst_field.name()) {
                    let mut nulls_offset = None;
                    let mut columns_offset = self.inner.columns[dst];
                    if self.inner.schema.field(dst).is_nullable() {
                        nulls_offset = Some(columns_offset);
                        columns_offset = align(columns_offset + PAGE_SIZE / 8)
                    }
                    extend(
                        nulls_offset,
                        columns_offset,
                        &self.inner.columns_buffer,
                        &self.inner.string_buffers[dst],
                        start,
                        end,
                        records.column(src).deref(),
                    );
                }
            }
        }
        for rid in start..end {
            // Make the rows visible to subsequent transactions.
            self.xmin(rid).store(txn, Ordering::Relaxed);
            self.xmax(rid).store(i64::MAX, Ordering::Relaxed);
            // Write new tids.
            let tid = self.inner.pid * PAGE_SIZE + rid;
            tids.append_value(tid as i64).unwrap();
        }
        *offset += end - start;
    }

    pub fn delete(&self, row: usize, txn: i64) -> bool {
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

    pub fn approx_num_rows(&self) -> usize {
        self.num_rows().load(Ordering::Relaxed)
    }

    fn num_rows(&self) -> &AtomicUsize {
        self.atomic::<AtomicUsize>(self.inner.num_rows, 0)
    }

    fn xmin(&self, row: usize) -> &AtomicI64 {
        self.atomic::<AtomicI64>(self.inner.xmin, row)
    }

    fn xmax(&self, row: usize) -> &AtomicI64 {
        self.atomic::<AtomicI64>(self.inner.xmax, row)
    }

    fn atomic<T>(&self, column_offset: usize, row: usize) -> &T {
        assert!(column_offset + (row + 1) * size_of::<T>() < self.inner.columns_buffer.capacity());
        unsafe {
            let row_offset = column_offset + row * size_of::<T>();
            let ptr = self.inner.columns_buffer.slice(row_offset).raw_data() as *const T;
            ptr.as_ref().unwrap()
        }
    }

    pub(crate) fn print(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        let mut star: Vec<String> = self.star();
        star.push("$xmin".to_string());
        star.push("$xmax".to_string());
        let record_batch = self.select(&star);
        let header: Vec<String> = record_batch
            .schema()
            .fields()
            .iter()
            .map(|field| base_name(field.name()).to_string())
            .collect();
        let mut csv_bytes = vec![];
        csv_bytes.extend_from_slice(header.join(",").as_bytes());
        csv_bytes.extend_from_slice("\n".as_bytes());
        arrow::csv::WriterBuilder::new()
            .has_headers(false)
            .build(&mut csv_bytes)
            .write(&record_batch)
            .unwrap();
        let csv = String::from_utf8(csv_bytes).unwrap();
        for line in csv.lines().take(10) {
            for _ in 0..indent {
                write!(f, "\t")?;
            }
            writeln!(f, "{}", line.trim())?;
        }
        if record_batch.num_rows() > 10 {
            for _ in 0..indent {
                write!(f, "\t")?;
            }
            writeln!(f, "...{} additional rows", record_batch.num_rows() - 10)?;
        }
        Ok(())
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.print(f, 0)
    }
}

fn string_buffer(data_type: &DataType) -> Option<RwLock<Buffer>> {
    match data_type {
        DataType::Utf8 => Some(RwLock::new(zeros(INITIAL_STRING_CAPACITY))),
        _ => None,
    }
}

fn zeros(capacity: usize) -> Buffer {
    let mut buffer = MutableBuffer::new(capacity);
    buffer.resize(capacity).unwrap();
    buffer.freeze()
}

fn head(
    mut column_offset: usize,
    columns_buffer: &Buffer,
    string_buffer: &Option<RwLock<Buffer>>,
    len: usize,
    data_type: DataType,
    nullable: bool,
) -> Arc<dyn Array> {
    let mut array = ArrayDataBuilder::new(data_type.clone()).len(len);
    // If column is nullable, add a null buffer and offset the values buffer.
    if nullable {
        let nulls_buffer = columns_buffer.slice(column_offset);
        array = array.null_bit_buffer(nulls_buffer);
        column_offset = align(column_offset + pax_length(&DataType::Boolean, PAGE_SIZE))
    }
    // Add the values buffer.
    let values_buffer = columns_buffer.slice(column_offset);
    array = array.add_buffer(values_buffer);
    // If column is a string, add a string buffer.
    if let Some(string_buffer) = string_buffer {
        array = array.add_buffer(string_buffer.read().unwrap().clone());
    }
    make_array(array.build())
}

fn extend(
    nulls_offset: Option<usize>,
    column_offset: usize,
    columns_buffer: &Buffer,
    string_buffer: &Option<RwLock<Buffer>>,
    start: usize,
    end: usize,
    values: &dyn Array,
) {
    if let Some(nulls_offset) = nulls_offset {
        extend_nulls(nulls_offset, columns_buffer, start, end, values);
    }
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

fn extend_nulls(
    nulls_offset: usize,
    columns_buffer: &Buffer,
    start: usize,
    end: usize,
    values: &dyn Array,
) {
    assert!(nulls_offset + (end + 7) / 8 < columns_buffer.capacity());

    let dst = columns_buffer.slice(nulls_offset).raw_data() as *mut u8;
    if let Some(null_bitmap) = values.data_ref().null_bitmap() {
        for i in 0..(end - start) {
            if null_bitmap.is_set(i) {
                unsafe { kernel::set_bit_raw(dst, start + i) }
            }
        }
    } else {
        unsafe {
            arrow::util::bit_util::set_bits_raw(dst, start, end);
        }
    }
}

fn extend_primitive<T: ArrowNumericType>(
    column_offset: usize,
    columns_buffer: &Buffer,
    start: usize,
    end: usize,
    values: &dyn Array,
) {
    assert!(column_offset + end * size_of::<T::Native>() < columns_buffer.capacity());

    let row_offset = column_offset + start * size_of::<T::Native>();
    let dst = columns_buffer.slice(row_offset).raw_data() as *mut T::Native;
    let src = values
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap()
        .raw_values();
    unsafe {
        std::ptr::copy_nonoverlapping(src, dst, end - start);
    }
}

fn extend_boolean(
    column_offset: usize,
    columns_buffer: &Buffer,
    start: usize,
    end: usize,
    src: &dyn Array,
) {
    assert!(column_offset + (end + 7) / 8 < columns_buffer.capacity());

    let dst = columns_buffer.slice(column_offset).raw_data() as *mut u8;
    let src = src
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .values()
        .raw_data();
    for i in 0..(end - start) {
        unsafe {
            if kernel::get_bit_raw(src, i) {
                kernel::set_bit_raw(dst, start + i)
            }
        }
    }
}

fn extend_string(
    column_offset: usize,
    columns_buffer: &Buffer,
    string_buffer: &RwLock<Buffer>,
    start: usize,
    end: usize,
    src: &dyn Array,
) {
    let src: &StringArray = src.as_any().downcast_ref::<StringArray>().unwrap();
    let dst_data_start = extend_string_offsets(column_offset, columns_buffer, start, end, src);
    extend_string_values(string_buffer, dst_data_start, src);
}

fn extend_string_offsets(
    column_offset: usize,
    columns_buffer: &Buffer,
    start: usize,
    end: usize,
    src: &StringArray,
) -> i32 {
    assert!(column_offset + end * size_of::<i32>() < columns_buffer.capacity());

    let dst_offsets = columns_buffer.slice(column_offset).raw_data() as *mut i32;
    for i in start..end {
        unsafe {
            *dst_offsets.offset((i + 1) as isize) =
                *dst_offsets.offset(i as isize) + src.value_length(i - start);
        }
    }
    // Start position in dst_data is given by the offset of the end of the last string, dst_offsets[start].
    unsafe { *dst_offsets.offset(start as isize) }
}

fn extend_string_values(string_buffer: &RwLock<Buffer>, dst_data_start: i32, src: &StringArray) {
    let src_data_len = src.value_offset(src.len());
    let needs_capacity = (dst_data_start + src_data_len) as usize;

    if needs_capacity > string_buffer.read().unwrap().capacity() {
        resize_string_buffer(string_buffer, needs_capacity.next_power_of_two());
    }

    assert!(needs_capacity <= string_buffer.read().unwrap().capacity());

    let src_data = src.value_data().raw_data();
    let dst_data = string_buffer.read().unwrap().raw_data() as *mut u8;
    unsafe {
        std::ptr::copy_nonoverlapping(
            src_data,
            dst_data.offset(dst_data_start as isize),
            src_data_len as usize,
        );
    }
}

fn resize_string_buffer(string_buffer: &RwLock<Buffer>, new_capacity: usize) {
    let mut lock = string_buffer.write().unwrap();
    let mut bigger = MutableBuffer::new(new_capacity);
    bigger.write_bytes(lock.data(), lock.len()).unwrap();
    *lock = bigger.freeze();
}

fn align(offset: usize) -> usize {
    ((offset + arrow::memory::ALIGNMENT - 1) / arrow::memory::ALIGNMENT) * arrow::memory::ALIGNMENT
}

fn pax_length(data_type: &DataType, num_rows: usize) -> usize {
    match data_type {
        DataType::Boolean => (num_rows + 7) / 8,
        DataType::Int64 => Int64Type::get_bit_width() / 8 * num_rows,
        DataType::Float64 => Float64Type::get_bit_width() / 8 * num_rows,
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            TimestampMicrosecondType::get_bit_width() / 8 * num_rows
        }
        DataType::Date32(DateUnit::Day) => Date32Type::get_bit_width() / 8 * num_rows,
        DataType::Utf8 => 4 * (num_rows + 1), // offset width
        other => panic!("{:?}", other),
    }
}

pub fn base_name(field: &str) -> &str {
    for (n, c) in field.char_indices().rev() {
        if c == '#' {
            return &field[0..n];
        } else if ('0'..='9').contains(&c) {
            continue;
        } else {
            break;
        }
    }
    field
}

fn with_base_name(field: &Field) -> Field {
    Field::new(
        base_name(field.name()),
        field.data_type().clone(),
        field.is_nullable(),
    )
}
