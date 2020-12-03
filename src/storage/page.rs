use arrow::array::*;
use arrow::buffer::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use arrow::util::bit_util::*;
use ast::*;
use regex::Regex;
use std::fmt;
use std::mem::size_of;
use std::ops::Deref;
use std::sync::atomic::*;
use std::sync::Arc;

pub const PAGE_SIZE: usize = 1024;
const INITIAL_STRING_CAPACITY: usize = PAGE_SIZE * 10;

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
    schema: Arc<Schema>,
    columns: Vec<usize>,
    xmin: usize,
    xmax: usize,
    num_rows: usize,
    columns_buffer: Buffer,
    string_buffers: Vec<Option<Buffer>>,
}

impl Page {
    // Allocate a mutable page that can hold PAGE_SIZE tuples.
    pub fn empty(schema: Arc<Schema>) -> Self {
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
        capacity = align(capacity + PAGE_SIZE * size_of::<u64>());
        let xmax = capacity;
        capacity = align(capacity + PAGE_SIZE * size_of::<u64>());
        let num_rows = capacity;
        capacity = capacity + size_of::<u64>();
        // Allocate memory.
        let columns_buffer = zeros(capacity);
        let string_buffers: Vec<Option<Buffer>> = schema
            .fields()
            .iter()
            .map(|field| string_buffer(field.data_type()))
            .collect();
        let inner = Inner {
            schema,
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

    pub unsafe fn read(pid: u64) -> Self {
        Page {
            inner: crate::arc::read(pid as *const Inner),
        }
    }

    fn pid(&self) -> u64 {
        crate::arc::leak(&self.inner) as u64
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.inner.schema.clone()
    }

    pub fn select(&self, projects: &Vec<Column>) -> RecordBatch {
        let num_rows = self.num_rows().load(Ordering::Relaxed);
        // View data columns as arrow format.
        let mut columns = vec![];
        let mut fields = vec![];
        for column in projects {
            for (i, field) in self.inner.schema.fields().iter().enumerate() {
                if base_name(field.name()) == base_name(&column.name) {
                    let field = Field::new(
                        column.canonical_name().as_str(),
                        field.data_type().clone(),
                        field.is_nullable(),
                    );
                    let column = head(
                        &field,
                        self.inner.columns[i],
                        &self.inner.columns_buffer,
                        self.inner.string_buffers[i].as_ref(),
                        num_rows,
                    );
                    columns.push(column);
                    fields.push(field);
                }
            }
        }
        // Add system columns.
        if let Some(xmin) = projects.iter().find(|c| c.name == "$xmin") {
            columns.push(head(
                &Field::new("$xmin", DataType::UInt64, false),
                self.inner.xmin,
                &self.inner.columns_buffer,
                None,
                num_rows,
            ));
            fields.push(xmin.into_query_field());
        }
        if let Some(xmax) = projects.iter().find(|c| c.name == "$xmax") {
            columns.push(head(
                &Field::new("$xmax", DataType::UInt64, false),
                self.inner.xmax,
                &self.inner.columns_buffer,
                None,
                num_rows,
            ));
            fields.push(xmax.into_query_field());
        }
        if let Some(pid) = projects.iter().find(|c| c.name == "$pid") {
            columns.push(Arc::new(UInt64Array::from(
                vec![self.pid()].repeat(num_rows),
            )));
            fields.push(pid.into_query_field());
        }
        if let Some(tid) = projects.iter().find(|c| c.name == "$tid") {
            columns.push(Arc::new(UInt32Array::from(
                (0u32..num_rows as u32).collect::<Vec<u32>>(),
            )));
            fields.push(tid.into_query_field());
        }
        // Wrap RecordBatch in a reference that ensures the underlying buffer doesn't get destroyed.
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    pub fn insert(
        &self,
        records: &RecordBatch,
        txn: u64,
        pids: &mut UInt64Builder,
        tids: &mut UInt32Builder,
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
                        self.inner.string_buffers[dst].as_ref(),
                        start,
                        end,
                        records.column(src).deref(),
                    );
                }
            }
        }
        for i in start..end {
            // Make the rows visible to subsequent transactions.
            self.xmin(i).store(txn, Ordering::Relaxed);
            self.xmax(i).store(u64::MAX, Ordering::Relaxed);
            // Write locations of new pids and tids.
            pids.append_value(self.pid()).unwrap();
            tids.append_value(i as u32).unwrap();
        }
        *offset += end - start;
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
        self.stat::<AtomicUsize>(self.inner.num_rows)
    }

    fn xmin(&self, row: usize) -> &AtomicU64 {
        self.column::<AtomicU64>(self.inner.xmin, row)
    }

    fn xmax(&self, row: usize) -> &AtomicU64 {
        self.column::<AtomicU64>(self.inner.xmax, row)
    }

    fn stat<T>(&self, offset: usize) -> &T {
        unsafe {
            let ptr = self.inner.columns_buffer.raw_data().offset(offset as isize) as *const T;
            ptr.as_ref().unwrap()
        }
    }

    fn column<T>(&self, offset: usize, row: usize) -> &T {
        unsafe {
            let ptr = self.inner.columns_buffer.raw_data().offset(offset as isize) as *const T;
            ptr.offset(row as isize).as_ref().unwrap()
        }
    }

    pub(crate) fn print(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        let mut star: Vec<Column> = self
            .inner
            .schema
            .fields()
            .iter()
            .map(|field| Column {
                created: ast::Phase::Plan,
                id: 0,
                name: field.name().clone(),
                table: None,
                data: field.data_type().clone(),
            })
            .collect();
        star.push(Column {
            created: Phase::Plan,
            id: 0,
            name: "$xmin".to_string(),
            table: None,
            data: DataType::UInt64,
        });
        star.push(Column {
            created: Phase::Plan,
            id: 0,
            name: "$xmax".to_string(),
            table: None,
            data: DataType::UInt64,
        });
        let record_batch = self.select(&star);
        let trim = |field: &String| {
            if let Some(captures) = Regex::new(r"(.*)#\d+").unwrap().captures(field) {
                captures.get(1).unwrap().as_str().to_string()
            } else {
                field.clone()
            }
        };
        let header: Vec<String> = record_batch
            .schema()
            .fields()
            .iter()
            .map(|field| trim(field.name()))
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
        for line in csv.lines() {
            for _ in 0..indent {
                write!(f, "\t")?;
            }
            writeln!(f, "{}", line.trim())?;
        }
        Ok(())
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.print(f, 0)
    }
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

fn head(
    field: &Field,
    mut column_offset: usize,
    columns_buffer: &Buffer,
    string_buffer: Option<&Buffer>,
    len: usize,
) -> Arc<dyn Array> {
    let mut array = ArrayDataBuilder::new(field.data_type().clone()).len(len);
    // If column is nullable, add a null buffer and offset the values buffer.
    if field.is_nullable() {
        let nulls_buffer = columns_buffer.slice(column_offset);
        array = array.null_bit_buffer(nulls_buffer);
        column_offset = align(column_offset + pax_length(&DataType::Boolean, PAGE_SIZE))
    }
    // Add the values buffer.
    let values_buffer = columns_buffer.slice(column_offset);
    array = array.add_buffer(values_buffer);
    // If column is a string, add a string buffer.
    if let Some(string_buffer) = string_buffer {
        array = array.add_buffer(string_buffer.clone());
    }
    make_array(array.build())
}

fn extend(
    nulls_offset: Option<usize>,
    column_offset: usize,
    columns_buffer: &Buffer,
    string_buffer: Option<&Buffer>,
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
            string_buffer.unwrap(),
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
    let dst = columns_buffer.slice(nulls_offset).raw_data() as *mut u8;
    if let Some(nulls_buffer) = values.data_ref().null_buffer() {
        let src = nulls_buffer.raw_data();
        unsafe {
            for i in 0..(end - start) {
                if get_bit_raw(src, i) {
                    set_bit_raw(dst, start + i)
                }
            }
        }
    } else {
        unsafe {
            set_bits_raw(dst, start, end);
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
    let src = src
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .values()
        .raw_data();
    for i in 0..(end - start) {
        unsafe {
            if get_bit_raw(src, i) {
                set_bit_raw(dst, start + i)
            }
        }
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

pub fn base_name(field: &String) -> &str {
    if let Some(captures) = Regex::new(r"(.*)#\d+").unwrap().captures(field) {
        captures.get(1).unwrap().as_str()
    } else {
        field
    }
}
