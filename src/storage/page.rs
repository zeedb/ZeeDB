use kernel::*;
use regex::Regex;
use std::sync::atomic::*;
use std::{fmt, sync::Arc};

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
#[derive(Clone)]
pub struct Page {
    inner: Arc<Inner>,
}

struct Inner {
    pid: usize,
    columns: Vec<(String, Data)>,
    xmin: [i64; PAGE_SIZE],
    xmax: [i64; PAGE_SIZE],
    len: AtomicUsize,
}

#[derive(Clone)]
enum Data {
    Bool {
        values: [u8; PAGE_SIZE / 8],
        is_valid: [u8; PAGE_SIZE / 8],
    },
    I64 {
        values: [i64; PAGE_SIZE],
        is_valid: [u8; PAGE_SIZE / 8],
    },
    F64 {
        values: [f64; PAGE_SIZE],
        is_valid: [u8; PAGE_SIZE / 8],
    },
    Date {
        values: [i32; PAGE_SIZE],
        is_valid: [u8; PAGE_SIZE / 8],
    },
    Timestamp {
        values: [i64; PAGE_SIZE],
        is_valid: [u8; PAGE_SIZE / 8],
    },
    String {
        buffer: String,
        offsets: [i32; PAGE_SIZE + 1],
        is_valid: [u8; PAGE_SIZE / 8],
    },
}

impl Data {
    fn new(data_type: DataType) -> Self {
        match data_type {
            DataType::Bool => Data::Bool {
                values: [0; PAGE_SIZE / 8],
                is_valid: [0; PAGE_SIZE / 8],
            },
            DataType::I64 => Data::I64 {
                values: [0; PAGE_SIZE],
                is_valid: [0; PAGE_SIZE / 8],
            },
            DataType::F64 => Data::F64 {
                values: [0.0; PAGE_SIZE],
                is_valid: [0; PAGE_SIZE / 8],
            },
            DataType::Date => Data::Date {
                values: [0; PAGE_SIZE],
                is_valid: [0; PAGE_SIZE / 8],
            },
            DataType::Timestamp => Data::Timestamp {
                values: [0; PAGE_SIZE],
                is_valid: [0; PAGE_SIZE / 8],
            },
            DataType::String => Data::String {
                buffer: String::with_capacity(PAGE_SIZE),
                offsets: [0; PAGE_SIZE + 1],
                is_valid: [0; PAGE_SIZE / 8],
            },
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            Data::Bool { .. } => DataType::Bool,
            Data::I64 { .. } => DataType::I64,
            Data::F64 { .. } => DataType::F64,
            Data::Date { .. } => DataType::Date,
            Data::Timestamp { .. } => DataType::Timestamp,
            Data::String { .. } => DataType::String,
        }
    }

    fn slice(&self, len: usize) -> Array {
        match self {
            Data::Bool { values, is_valid } => Array::Bool(BoolArray::from_slice(
                BitSlice::from_slice(values, 0..len),
                BitSlice::from_slice(is_valid, 0..len),
            )),
            Data::I64 { values, is_valid } => Array::I64(I64Array::from_slice(
                &values[..len],
                BitSlice::from_slice(is_valid, 0..len),
            )),
            Data::F64 { values, is_valid } => Array::F64(F64Array::from_slice(
                &values[..len],
                BitSlice::from_slice(is_valid, 0..len),
            )),
            Data::Date { values, is_valid } => Array::Date(DateArray::from_slice(
                &values[..len],
                BitSlice::from_slice(is_valid, 0..len),
            )),
            Data::Timestamp { values, is_valid } => Array::Timestamp(TimestampArray::from_slice(
                &values[..len],
                BitSlice::from_slice(is_valid, 0..len),
            )),
            Data::String {
                buffer,
                offsets,
                is_valid,
            } => Array::String(StringArray::from_slice(
                &buffer[..offsets[len] as usize],
                &offsets[..len + 1],
                BitSlice::from_slice(is_valid, 0..len),
            )),
        }
    }

    fn extend(&mut self, start: usize, end: usize, from: &Array, offset: usize) {
        match (self, from) {
            (Data::Bool { values, is_valid }, Array::Bool(from)) => {
                for i in 0..(end - start) {
                    let src = offset + i;
                    let dst = start + i;
                    if let Some(value) = from.get(src) {
                        if value {
                            set_bit(values, dst);
                        } else {
                            unset_bit(values, dst);
                        }
                        set_bit(is_valid, dst);
                    } else {
                        unset_bit(is_valid, dst);
                    }
                }
            }
            (Data::I64 { values, is_valid }, Array::I64(from)) => {
                for i in 0..(end - start) {
                    let src = offset + i;
                    let dst = start + i;
                    if let Some(value) = from.get(src) {
                        values[dst] = value;
                        set_bit(is_valid, dst);
                    } else {
                        unset_bit(is_valid, dst);
                    }
                }
            }
            (Data::F64 { values, is_valid }, Array::F64(from)) => {
                for i in 0..(end - start) {
                    let src = offset + i;
                    let dst = start + i;
                    if let Some(value) = from.get(src) {
                        values[dst] = value;
                        set_bit(is_valid, dst);
                    } else {
                        unset_bit(is_valid, dst);
                    }
                }
            }
            (Data::Date { values, is_valid }, Array::Date(from)) => {
                for i in 0..(end - start) {
                    let src = offset + i;
                    let dst = start + i;
                    if let Some(value) = from.get(src) {
                        values[dst] = value;
                        set_bit(is_valid, dst);
                    } else {
                        unset_bit(is_valid, dst);
                    }
                }
            }
            (Data::Timestamp { values, is_valid }, Array::Timestamp(from)) => {
                for i in 0..(end - start) {
                    let src = offset + i;
                    let dst = start + i;
                    if let Some(value) = from.get(src) {
                        values[dst] = value;
                        set_bit(is_valid, dst);
                    } else {
                        unset_bit(is_valid, dst);
                    }
                }
            }
            (
                Data::String {
                    buffer,
                    offsets,
                    is_valid,
                },
                Array::String(from),
            ) => {
                for i in 0..(end - start) {
                    let src = offset + i;
                    let dst = start + i;
                    if let Some(value) = from.get(src) {
                        buffer.push_str(value);
                        offsets[dst + 1] = buffer.len() as i32;
                        set_bit(is_valid, dst);
                    } else {
                        offsets[dst + 1] = buffer.len() as i32;
                        unset_bit(is_valid, dst);
                    }
                }
            }
            (left, right) => panic!("{} does not match {}", left.data_type(), right.data_type()),
        }
    }
}

impl Page {
    // Allocate a mutable page that can hold PAGE_SIZE tuples.
    pub fn empty(pid: usize, mut schema: Vec<(String, DataType)>) -> Self {
        Self {
            inner: Arc::new(Inner {
                pid,
                columns: schema
                    .drain(..)
                    .map(|(name, data_type)| (name, Data::new(data_type)))
                    .collect(),
                xmin: [0; PAGE_SIZE],
                xmax: [0; PAGE_SIZE],
                len: AtomicUsize::new(0),
            }),
        }
    }

    pub fn schema(&self, projects: &Vec<String>) -> Vec<(String, DataType)> {
        projects
            .iter()
            .map(|find| {
                let data_type = match find.as_str() {
                    "$xmin" | "$xmax" | "$tid" => DataType::I64,
                    find => self
                        .inner
                        .columns
                        .iter()
                        .find(|(name, _)| find == name)
                        .map(|(_, data)| data.data_type())
                        .expect(find),
                };
                (find.clone(), data_type)
            })
            .collect()
    }

    pub fn select(&self, projects: &Vec<String>) -> RecordBatch {
        let len = self.inner.len.load(Ordering::Relaxed);
        let columns: Vec<(String, Array)> = projects
            .iter()
            .map(|find| {
                let array = match find.as_str() {
                    "$xmin" => Self::xcolumn(&self.inner.xmin[..len]),
                    "$xmax" => Self::xcolumn(&self.inner.xmax[..len]),
                    "$tid" => {
                        let start = (self.inner.pid * PAGE_SIZE) as i64;
                        let end = start + len as i64;
                        let tids: Vec<i64> = (start..end).collect();
                        Array::I64(I64Array::from(tids))
                    }
                    find => self
                        .inner
                        .columns
                        .iter()
                        .find(|(name, _)| find == name)
                        .map(|(_, data)| data.slice(len))
                        .expect(find),
                };
                (find.clone(), array)
            })
            .collect();
        RecordBatch::new(columns)
    }

    fn xcolumn(values: &[i64]) -> Array {
        let is_valid = vec![u8::MAX].repeat((values.len() + 7) / 8);
        Array::I64(I64Array::from_slice(
            values,
            BitSlice::from_slice(&is_valid, 0..values.len()),
        ))
    }

    pub fn star(&self) -> Vec<String> {
        self.inner
            .columns
            .iter()
            .map(|(n, _)| format!("{}", n))
            .collect()
    }

    pub fn insert(&self, records: &RecordBatch, txn: i64, tids: &mut I64Array, offset: &mut usize) {
        let (start, end) = self.reserve(records.len() - *offset);
        // Write the new rows in the reserved slots.
        for (dst_name, dst_array) in &mut self.inner_mut().columns {
            for (src_name, src_array) in &records.columns {
                if dst_name == src_name {
                    dst_array.extend(start, end, src_array, *offset);
                }
            }
        }
        for rid in start..end {
            // Make the rows visible to subsequent transactions.
            self.inner_mut().xmin[rid] = txn;
            self.inner_mut().xmax[rid] = i64::MAX;
            // Write new tids.
            let tid = self.inner.pid * PAGE_SIZE + rid;
            tids.push(Some(tid as i64));
        }
        *offset += end - start;
    }

    fn inner_mut(&self) -> &mut Inner {
        unsafe { (Arc::as_ptr(&self.inner) as *mut Inner).as_mut().unwrap() }
    }

    pub fn delete(&self, row: usize, txn: i64) -> bool {
        if self.inner.xmax[row] > txn {
            self.inner_mut().xmax[row] = txn;
            true
        } else {
            false
        }
    }

    fn reserve(&self, request: usize) -> (usize, usize) {
        let start = self.inner.len.load(Ordering::Relaxed);
        let end = start + request;
        // If there's not enough space for request, take whatever is available.
        if end > PAGE_SIZE {
            let start = self.inner.len.swap(PAGE_SIZE, Ordering::Relaxed);
            return (start, PAGE_SIZE);
        }
        // If someone else concurrently reserves rows, try again.
        if self
            .inner
            .len
            .compare_and_swap(start, end, Ordering::Relaxed)
            != start
        {
            return self.reserve(request);
        }
        // Otherwise, we have successfully reserved a segment of the page.
        (start, end)
    }

    pub fn approx_num_rows(&self) -> usize {
        self.inner.len.load(Ordering::Relaxed)
    }

    pub(crate) fn print(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        let mut star: Vec<String> = self.star();
        star.push("$xmin".to_string());
        star.push("$xmax".to_string());
        let record_batch = self.select(&star);
        for line in fixed_width(&vec![record_batch]).split_terminator('\n') {
            for _ in 0..indent {
                write!(f, "\t")?;
            }
            write!(f, "{}\n", line)?;
        }
        Ok(())
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.print(f, 0)
    }
}
