use kernel::*;
use storage::*;

pub(crate) fn insert(index: &mut Art, columns: &Vec<String>, input: &RecordBatch, tids: &I64Array) {
    // Convert each row into bytes that match the lexicographic order of rows.
    let index_columns: Vec<&AnyArray> = columns
        .iter()
        .map(|name| input.find(name).unwrap())
        .collect();
    let keys = byte_keys(index_columns, tids);
    // Insert the keys into the index.
    for i in 0..keys.len() {
        index.insert(keys.get(i), tids.get(i).unwrap());
    }
}

pub(crate) fn upper_bound(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    let mut i = end.len();
    loop {
        if i == 0 {
            end.push(u8::MAX);
            return end;
        } else if end[i - 1] < u8::MAX {
            end[i - 1] += 1;
            return end;
        } else {
            end[i - 1] = 0;
            i -= 1;
            continue;
        }
    }
}

#[derive(Debug)]
pub(crate) struct PackedBytes {
    bytes: Vec<u8>,
    offsets: Vec<usize>,
}

impl PackedBytes {
    fn with_capacity(len: usize, byte_len: usize) -> Self {
        let mut offsets = Vec::with_capacity(len + 1);
        offsets.push(0);
        Self {
            bytes: Vec::with_capacity(byte_len),
            offsets,
        }
    }

    fn push(&mut self, next: &[u8]) {
        debug_assert!(self.bytes.len() + next.len() <= self.bytes.capacity());

        self.bytes.extend_from_slice(next);
    }

    fn next(&mut self) {
        debug_assert!(self.offsets.len() + 1 <= self.offsets.capacity());

        self.offsets.push(self.bytes.len());
    }

    pub(crate) fn get(&self, i: usize) -> &[u8] {
        &self.bytes[self.offsets[i]..self.offsets[i + 1]]
    }

    pub(crate) fn len(&self) -> usize {
        self.offsets.len() - 1
    }
}

pub(crate) fn byte_keys(columns: Vec<&AnyArray>, tids: &I64Array) -> PackedBytes {
    let byte_len = columns.iter().map(|c| byte_len(*c)).sum::<usize>()
        + tids.len() * std::mem::size_of::<i64>();
    let mut result = PackedBytes::with_capacity(columns[0].len(), byte_len);
    for i in 0..columns[0].len() {
        push_byte_key_prefix(&mut result, &columns, i);
        result.push(&byte_key_i64(tids.get(i).unwrap()));
        result.next();
    }
    result
}

pub(crate) fn byte_key_prefix(columns: Vec<&AnyArray>) -> PackedBytes {
    let byte_len = columns.iter().map(|c| byte_len(*c)).sum::<usize>();
    let mut result = PackedBytes::with_capacity(columns[0].len(), byte_len);
    for i in 0..columns[0].len() {
        push_byte_key_prefix(&mut result, &columns, i);
        result.next();
    }
    result
}

fn byte_len(column: &AnyArray) -> usize {
    match column {
        AnyArray::Bool(array) => array.len(), // Unpacked representation.
        AnyArray::I64(array) => array.len() * std::mem::size_of::<i64>(),
        AnyArray::F64(array) => array.len() * std::mem::size_of::<f64>(),
        AnyArray::Date(array) => array.len() * std::mem::size_of::<i32>(),
        AnyArray::Timestamp(array) => array.len() * std::mem::size_of::<i64>(),
        AnyArray::String(array) => array.byte_len(),
    }
}

fn push_byte_key_prefix(result: &mut PackedBytes, columns: &Vec<&AnyArray>, i: usize) {
    for j in 0..columns.len() {
        match columns[j] {
            AnyArray::Bool(column) => {
                let b = match column.get(i) {
                    None => 0,
                    Some(false) => 1,
                    Some(true) => 2,
                };
                result.push(&[b]);
            }
            AnyArray::I64(column) => {
                if let Some(value) = column.get(i) {
                    result.push(&byte_key_i64(value));
                    // Per https://db.in.tum.de/~leis/papers/ART.pdf section IV.B.e
                    if value == i64::MIN {
                        result.push(&[1]);
                    }
                } else {
                    result.push(&byte_key_i64(i64::MIN));
                }
            }
            AnyArray::F64(column) => {
                if let Some(value) = column.get(i) {
                    result.push(&byte_key_f64(value));
                    if value == f64::MIN {
                        result.push(&[1]);
                    }
                } else {
                    result.push(&byte_key_f64(f64::MIN));
                }
            }
            AnyArray::Date(column) => {
                if let Some(value) = column.get(i) {
                    result.push(&byte_key_i32(value));
                    if value == i32::MIN {
                        result.push(&[1]);
                    }
                } else {
                    result.push(&byte_key_i32(i32::MIN));
                }
            }
            AnyArray::Timestamp(column) => {
                if let Some(value) = column.get(i) {
                    result.push(&byte_key_i64(value));
                    if value == i64::MIN {
                        result.push(&[1]);
                    }
                } else {
                    result.push(&byte_key_i64(i64::MIN));
                }
            }
            AnyArray::String(column) => {
                // TODO UTF-8 order doesn't match the "Unicode Collation Algorithm" https://www.unicode.org/reports/tr10/.
                // An implementation of unicode => binary comparable representation is available in https://docs.rs/crate/rust_icu.
                if let Some(value) = column.get(i) {
                    result.push(value.as_bytes());
                    // Strings are null-terminated, so that a key cannot be a prefix of another key.
                    result.push(&[0]);
                } else {
                    // TODO this creates a collision between NULL and "".
                    result.push(&[0]);
                }
            }
        }
    }
}
