use std::{fmt, sync::atomic::*};

use kernel::*;

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
pub struct Page {
    pid: usize,
    columns: Vec<(String, Data)>,
    xmin: [AtomicI64; PAGE_SIZE],
    xmax: [AtomicI64; PAGE_SIZE],
    len: AtomicUsize,
}

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

    fn slice(&self, len: usize) -> AnyArray {
        match self {
            Data::Bool { values, is_valid } => AnyArray::Bool(BoolArray::from_slice(
                BitSlice::from_slice(values, 0..len),
                BitSlice::from_slice(is_valid, 0..len),
            )),
            Data::I64 { values, is_valid } => AnyArray::I64(I64Array::from_slice(
                &values[..len],
                BitSlice::from_slice(is_valid, 0..len),
            )),
            Data::F64 { values, is_valid } => AnyArray::F64(F64Array::from_slice(
                &values[..len],
                BitSlice::from_slice(is_valid, 0..len),
            )),
            Data::Date { values, is_valid } => AnyArray::Date(DateArray::from_slice(
                &values[..len],
                BitSlice::from_slice(is_valid, 0..len),
            )),
            Data::Timestamp { values, is_valid } => AnyArray::Timestamp(
                TimestampArray::from_slice(&values[..len], BitSlice::from_slice(is_valid, 0..len)),
            ),
            Data::String {
                buffer,
                offsets,
                is_valid,
            } => AnyArray::String(StringArray::from_slice(
                &buffer[..offsets[len] as usize],
                &offsets[..len + 1],
                BitSlice::from_slice(is_valid, 0..len),
            )),
        }
    }

    unsafe fn as_mut(&self) -> &mut Self {
        ((self as *const Self) as *mut Self).as_mut().unwrap()
    }

    fn extend(&mut self, start: usize, end: usize, from: &AnyArray, offset: usize) {
        match (self, from) {
            (Data::Bool { values, is_valid }, AnyArray::Bool(from)) => {
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
            (Data::I64 { values, is_valid }, AnyArray::I64(from)) => {
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
            (Data::F64 { values, is_valid }, AnyArray::F64(from)) => {
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
            (Data::Date { values, is_valid }, AnyArray::Date(from)) => {
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
            (Data::Timestamp { values, is_valid }, AnyArray::Timestamp(from)) => {
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
                AnyArray::String(from),
            ) => {
                for i in 0..(end - start) {
                    let src = offset + i;
                    let dst = start + i;
                    if let Some(value) = from.get_str(src) {
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
            pid,
            columns: schema
                .drain(..)
                .map(|(name, data_type)| (name, Data::new(data_type)))
                .collect(),
            xmin: zeros(),
            xmax: zeros(),
            len: AtomicUsize::new(0),
        }
    }

    pub fn schema(&self, projects: &Vec<String>) -> Vec<(String, DataType)> {
        projects
            .iter()
            .map(|find| {
                let data_type = match find.as_str() {
                    "$xmin" | "$xmax" | "$tid" => DataType::I64,
                    find => self
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
        let len = self.len.load(Ordering::Relaxed);
        let columns: Vec<(String, AnyArray)> = projects
            .iter()
            .map(|find| {
                let array = match find.as_str() {
                    "$xmin" => Self::xcolumn(&self.xmin[..len]),
                    "$xmax" => Self::xcolumn(&self.xmax[..len]),
                    "$tid" => {
                        let start = (self.pid * PAGE_SIZE) as i64;
                        let end = start + len as i64;
                        let tids: Vec<i64> = (start..end).collect();
                        AnyArray::I64(I64Array::from_values(tids))
                    }
                    find => match self.columns.iter().find(|(name, _)| find == name) {
                        Some((_, data)) => data.slice(len),
                        None => panic!(
                            "{} is not a column of {}",
                            find,
                            self.column_names().join(", ")
                        ),
                    },
                };
                (find.clone(), array)
            })
            .collect();
        RecordBatch::new(columns)
    }

    fn xcolumn(values: &[AtomicI64]) -> AnyArray {
        let mut array = I64Array::with_capacity(values.len());
        for value in values {
            array.push(Some(value.load(Ordering::Relaxed)));
        }
        AnyArray::I64(array)
    }

    fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|(name, _)| name.clone()).collect()
    }

    pub fn star(&self) -> Vec<String> {
        self.columns.iter().map(|(n, _)| format!("{}", n)).collect()
    }

    pub fn insert(&self, records: &RecordBatch, txn: i64, tids: &mut I64Array, offset: &mut usize) {
        let (start, end) = self.reserve(records.len() - *offset);
        // Write the new rows in the reserved slots.
        for (dst_name, dst_array) in &self.columns {
            for (src_name, src_array) in &records.columns {
                if dst_name == src_name {
                    unsafe {
                        dst_array.as_mut().extend(start, end, src_array, *offset);
                    }
                }
            }
        }
        for rid in start..end {
            // Make the rows visible to subsequent transactions.
            self.xmin[rid].store(txn, Ordering::Relaxed);
            self.xmax[rid].store(i64::MAX, Ordering::Relaxed);
            // Write new tids.
            let tid = self.pid * PAGE_SIZE + rid;
            tids.push(Some(tid as i64));
        }
        *offset += end - start;
    }

    pub fn delete(&self, row: usize, txn: i64) -> bool {
        self.xmax[row].compare_exchange(i64::MAX, txn, Ordering::Relaxed, Ordering::Relaxed)
            == Ok(i64::MAX)
    }

    fn reserve(&self, request: usize) -> (usize, usize) {
        let start = self.len.load(Ordering::Relaxed);
        let end = start + request;
        // If there's not enough space for request, take whatever is available.
        if end > PAGE_SIZE {
            let start = self.len.swap(PAGE_SIZE, Ordering::Relaxed);
            return (start, PAGE_SIZE);
        }
        // If someone else concurrently reserves rows, try again.
        if self
            .len
            .compare_exchange_weak(start, end, Ordering::Relaxed, Ordering::Relaxed)
            != Ok(start)
        {
            return self.reserve(request);
        }
        // Otherwise, we have successfully reserved a segment of the page.
        (start, end)
    }

    pub fn approx_num_rows(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    pub(crate) fn print(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        let mut star: Vec<String> = self.star();
        star.push("$xmin".to_string());
        star.push("$xmax".to_string());
        let batch = self.select(&star);
        for line in fixed_width(&batch).split_terminator('\n') {
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

fn zeros() -> [AtomicI64; PAGE_SIZE] {
    let mut data: [std::mem::MaybeUninit<AtomicI64>; PAGE_SIZE] =
        unsafe { std::mem::MaybeUninit::uninit().assume_init() };

    for elem in &mut data[..] {
        unsafe {
            std::ptr::write(elem.as_mut_ptr(), AtomicI64::default());
        }
    }

    unsafe { std::mem::transmute::<_, [AtomicI64; PAGE_SIZE]>(data) }
}
