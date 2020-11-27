use crate::state::State;
use arrow::array::*;
use arrow::record_batch::*;
use ast::*;
use kernel::Error;
use std::sync::Arc;

/// HashTable stores a large set of tuples in a fixed-size, dense hash table.
/// tuples[buckets[i]..buckets[i + 1]] contains all tuples where hash(tuple) % (buckets.len() - 1) == i
#[derive(Debug)]
pub struct HashTable {
    offsets: Vec<usize>,
    tuples: RecordBatch,
}

impl HashTable {
    pub fn new(
        scalars: &Vec<Scalar>,
        state: &mut State,
        input: &RecordBatch,
    ) -> Result<Self, Error> {
        let n_rows = input.num_rows();
        let n_buckets = size_hash_table(n_rows);
        let buckets = crate::eval::hash(scalars, n_buckets, input, state)?;
        let indices = kernel::sort(&buckets);
        let tuples = kernel::gather(input, &indices);
        let offsets = bucket_offsets(&buckets);
        Ok(HashTable { offsets, tuples })
    }

    /// Identify matching rows from self (build side of join) and right (probe side of the join).
    pub fn probe(&self, right_buckets: &Arc<dyn Array>) -> (RecordBatch, Arc<dyn Array>) {
        let right_buckets = kernel::coerce::<UInt32Array>(right_buckets);
        let mut left_index = vec![];
        let mut right_index = vec![];
        // For each row from the probe side of the join, get the bucket...
        for i_right in 0..right_buckets.len() {
            let bucket = right_buckets.value(i_right) as usize;
            // ...for each row on the build side of the join with matching bucket...
            for i_left in self.offsets[bucket]..self.offsets[bucket + 1] {
                // ...add a row to the output, effectively performing a cross-join.
                left_index.push(i_left as u32);
                right_index.push(i_right as u32);
            }
        }
        let left_index = UInt32Array::from(left_index);
        let right_index = UInt32Array::from(right_index);
        let left_index: Arc<dyn Array> = Arc::new(left_index);
        let right_index: Arc<dyn Array> = Arc::new(right_index);
        let left = kernel::gather(&self.tuples, &left_index);
        (left, right_index)
    }

    pub fn n_buckets(&self) -> usize {
        self.offsets.len() - 1
    }
}

fn bucket_offsets(buckets: &Arc<dyn Array>) -> Vec<usize> {
    let buckets = kernel::coerce::<UInt32Array>(buckets);
    let n_buckets = arrow::compute::max(buckets).map(|n| n + 1).unwrap_or(0) as usize;
    // Compute a dense histogram of buckets in offsets[1..n_buckets+1].
    let mut offsets = Vec::with_capacity(n_buckets);
    offsets.resize_with(n_buckets + 1, usize::default);
    for i in 0..buckets.len() {
        offsets[buckets.value(i) as usize + 1] += 1;
    }
    // Convert histogram to offsets of each bucket, assuming records have been sorted by bucket.
    for i in 0..n_buckets {
        offsets[i + 1] += offsets[i]
    }
    offsets
}

fn size_hash_table(n_rows: usize) -> usize {
    n_rows.next_power_of_two()
}
