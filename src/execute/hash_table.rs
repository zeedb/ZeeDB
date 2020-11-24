use crate::error::Error;
use crate::eval::eval;
use arrow::array::*;
use arrow::buffer::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use ast::*;
use fnv::FnvHasher;
use std::hash::Hasher;
use std::sync::Arc;

// HashTable stores a large set of tuples in a fixed-size, dense hash table.
// tuples[buckets[i]..buckets[i + 1]] contains all tuples where hash(tuple) % (buckets.len() - 1) == i
#[derive(Debug)]
pub struct HashTable {
    offsets: Vec<usize>,
    tuples: RecordBatch,
}

impl HashTable {
    pub fn new(scalars: &Vec<Scalar>, input: &Vec<RecordBatch>) -> Result<Self, Error> {
        let n_rows = input.iter().map(|batch| batch.num_rows()).sum();
        let n_buckets = size_hash_table(n_rows);
        let buckets = hash_buckets(&scalars, &input, n_rows, n_buckets)?;
        let offsets = bucket_offsets(&buckets, n_buckets);
        let indices = buckets_to_indices(buckets, offsets.clone());
        let schema = input.first().unwrap().schema().clone();
        let columns = gather_all(indices, input);
        let tuples = RecordBatch::try_new(schema, columns)?;
        Ok(HashTable { offsets, tuples })
    }

    pub fn get(&self, bucket: usize) -> RecordBatch {
        let min = self.offsets[bucket];
        let max = self.offsets[bucket + 1];
        let columns = self
            .tuples
            .columns()
            .iter()
            .map(|column| column.slice(min, max - min))
            .collect();
        RecordBatch::try_new(self.tuples.schema().clone(), columns).unwrap()
    }

    pub fn hash(&self, scalars: &Vec<Scalar>, input: &RecordBatch) -> Result<Vec<usize>, Error> {
        hash_buckets(
            scalars,
            &vec![input.clone()],
            input.num_rows(),
            self.offsets.len() - 1,
        )
    }

    pub fn schema(&self) -> Schema {
        self.tuples.schema().as_ref().clone()
    }
}

fn hash_buckets(
    scalars: &Vec<Scalar>,
    input: &Vec<RecordBatch>,
    n_rows: usize,
    n_buckets: usize,
) -> Result<Vec<usize>, Error> {
    // Allocate an array of hashers.
    let mut acc = vec![];
    acc.resize_with(n_rows, FnvHasher::default);
    let mut offset = 0;
    // For each batch, write a section of acc.
    for batch in input {
        // For each scalar, compute a vector and hash it.
        for scalar in scalars {
            let next = eval(scalar, batch)?;
            hash_one(next.as_ref(), &mut acc[offset..]);
        }
        offset += batch.num_rows();
    }
    let buckets = hash_finish(&mut acc, n_buckets);
    Ok(buckets)
}

fn hash_one(column: &dyn Array, acc: &mut [FnvHasher]) {
    match column.data_type() {
        DataType::Boolean => hash_typed::<BooleanType>(column, acc),
        DataType::Int64 => hash_typed::<Int64Type>(column, acc),
        DataType::Float64 => hash_typed::<Float64Type>(column, acc),
        DataType::Date32(DateUnit::Day) => hash_typed::<Date32Type>(column, acc),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            hash_typed::<TimestampMicrosecondType>(column, acc)
        }
        DataType::FixedSizeBinary(16) => todo!(),
        DataType::Utf8 => todo!(),
        DataType::Struct(fields) => todo!(),
        DataType::List(element) => todo!(),
        other => panic!("{:?} not supported", other),
    }
}

fn hash_typed<T: ArrowPrimitiveType>(column: &dyn Array, acc: &mut [FnvHasher]) {
    let column: &PrimitiveArray<T> = column.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    for i in 0..column.len() {
        acc[i].write(column.value(i).to_byte_slice());
    }
}

fn hash_finish(acc: &mut [FnvHasher], n_buckets: usize) -> Vec<usize> {
    let n_buckets = n_buckets as u64;
    acc.iter()
        .map(|hasher| (hasher.finish() % n_buckets) as usize)
        .collect()
}

fn bucket_offsets(buckets: &Vec<usize>, n_buckets: usize) -> Vec<usize> {
    let mut counts = vec![];
    counts.resize_with(n_buckets + 1, usize::default);
    for bucket in buckets {
        counts[*bucket + 1] += 1;
    }
    for i in 1..counts.len() {
        counts[i] = counts[i - 1] + counts[i]
    }
    counts
}

fn buckets_to_indices(buckets: Vec<usize>, mut offsets: Vec<usize>) -> Vec<usize> {
    let mut indices = Vec::with_capacity(buckets.len());
    for bucket in buckets {
        indices.push(offsets[bucket]);
        offsets[bucket] += 1;
    }
    indices
}

fn size_hash_table(n_rows: usize) -> usize {
    n_rows.next_power_of_two()
}

fn gather_all(indices: Vec<usize>, input: &Vec<RecordBatch>) -> Vec<Arc<dyn Array>> {
    let n_columns = input.first().unwrap().num_columns();
    (0..n_columns)
        .map(|i| {
            gather_column(
                &indices,
                &input.iter().map(|batch| batch.column(i).as_ref()).collect(),
            )
        })
        .collect()
}

fn gather_column(indices: &Vec<usize>, input: &Vec<&dyn Array>) -> Arc<dyn Array> {
    match input.first().unwrap().data_type() {
        DataType::Boolean => gather_bools(indices, input),
        DataType::Int64 => gather_typed::<Int64Type>(indices, input),
        DataType::UInt64 => gather_typed::<UInt64Type>(indices, input),
        DataType::Float64 => gather_typed::<Float64Type>(indices, input),
        DataType::Date32(DateUnit::Day) => gather_typed::<Date32Type>(indices, input),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            gather_typed::<TimestampMicrosecondType>(indices, input)
        }
        DataType::FixedSizeBinary(16) => todo!(),
        DataType::Utf8 => gather_strings(indices, input),
        DataType::Struct(fields) => todo!(),
        DataType::List(element) => todo!(),
        other => panic!("{:?} not supported", other),
    }
}

fn gather_bools(indices: &Vec<usize>, input: &Vec<&dyn Array>) -> Arc<dyn Array> {
    todo!()
}

fn gather_strings(indices: &Vec<usize>, input: &Vec<&dyn Array>) -> Arc<dyn Array> {
    // Gather length of each string in the new order.
    let mut lengths = Vec::with_capacity(indices.len());
    for array in input {
        let array = string_array(*array);
        for i in 0..array.len() {
            lengths.push(array.value_length(i));
        }
    }
    // Compute offsets in the new order.
    let mut offsets_buffer = MutableBuffer::new(std::mem::size_of::<i32>() * (indices.len() + 1));
    offsets_buffer.resize(offsets_buffer.capacity()).unwrap();
    let offsets = offsets_buffer.typed_data_mut::<i32>();
    for i in 0..lengths.len() {
        offsets[i + 1] = offsets[i] + lengths[i];
    }
    // Gather values.
    let mut values_buffer = MutableBuffer::new(*offsets.last().unwrap() as usize);
    values_buffer.resize(values_buffer.capacity()).unwrap();
    let values = values_buffer.data_mut();
    let mut next_index = 0;
    for array in input {
        let array = string_array(*array);
        for i in 0..array.len() {
            let start = offsets[indices[next_index]] as usize;
            let end = offsets[indices[next_index] + 1] as usize;
            values[start..end].copy_from_slice(array.value(i).as_bytes());
            next_index += 1;
        }
    }
    // Construct array.
    let mut data = ArrayData::builder(DataType::Utf8)
        .len(indices.len())
        .add_buffer(offsets_buffer.freeze())
        .add_buffer(values_buffer.freeze());
    if let Some((nulls, null_count)) = gather_nulls(indices, input) {
        data = data.null_bit_buffer(nulls).null_count(null_count);
    } else {
        data = data.null_count(0);
    }
    make_array(data.build())
}

fn string_array(array: &dyn Array) -> &StringArray {
    array.as_any().downcast_ref::<StringArray>().unwrap()
}

fn gather_typed<T>(indices: &Vec<usize>, input: &Vec<&dyn Array>) -> Arc<dyn Array>
where
    T: ArrowPrimitiveType + ArrowNumericType,
    T::Native: num_traits::Num,
{
    let values = gather_values::<T>(indices, input);
    let mut data = ArrayData::builder(T::get_data_type())
        .len(indices.len())
        .add_buffer(values);
    if let Some((nulls, null_count)) = gather_nulls(indices, input) {
        data = data.null_bit_buffer(nulls).null_count(null_count);
    } else {
        data = data.null_count(0);
    }
    make_array(data.build())
}

fn gather_values<T>(indices: &Vec<usize>, input: &Vec<&dyn Array>) -> Buffer
where
    T: ArrowPrimitiveType + ArrowNumericType,
    T::Native: num_traits::Num,
{
    let mut buffer = MutableBuffer::new(std::mem::size_of::<T::Native>() * indices.len());
    buffer.resize(buffer.capacity()).unwrap();
    let dst: &mut [T::Native] = buffer.typed_data_mut();
    let mut next_index = 0;
    for array in input {
        let array: &PrimitiveArray<T> = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        for next_value in array.value_slice(0, array.len()) {
            dst[indices[next_index]] = *next_value;
            next_index += 1;
        }
    }
    buffer.freeze()
}

fn gather_nulls(indices: &Vec<usize>, input: &Vec<&dyn Array>) -> Option<(Buffer, usize)> {
    let count = input.iter().map(|array| array.null_count()).sum();
    if count == 0 {
        return None;
    }
    let mut buffer = MutableBuffer::new((indices.len() + 7) / 8);
    buffer.resize(buffer.capacity()).unwrap();
    let data = buffer.raw_data_mut();
    let mut offset = 0;
    for array in input {
        for i in 0..array.len() {
            let valid = array.is_valid(i);
            set_bit(data, indices[offset], valid);
            offset += 1;
        }
    }
    Some((buffer.freeze(), count))
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
