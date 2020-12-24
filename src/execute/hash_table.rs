use arrow::array::*;
use arrow::record_batch::*;
use kernel::Error;
use std::sync::Arc;

/// HashTable stores a large set of tuples in a fixed-size, dense hash table.
/// tuples[buckets[i]..buckets[i + 1]] contains all tuples where hash(tuple) % (buckets.len() - 1) == i
#[derive(Debug)]
pub struct HashTable {
    offsets: Vec<usize>,
    pub tuples: RecordBatch,
}

impl HashTable {
    pub fn new(input: &RecordBatch, partition_by: &Vec<Arc<dyn Array>>) -> Result<Self, Error> {
        let n_rows = input.num_rows();
        let n_buckets = size_hash_table(n_rows);
        let hashes = kernel::hash(partition_by);
        let buckets = bucketize(&hashes, n_buckets);
        let indexes = kernel::sort(&buckets);
        let tuples = kernel::gather(input, &indexes);
        let offsets = bucket_offsets(&buckets, n_buckets);
        Ok(HashTable { offsets, tuples })
    }

    /// Identify matching rows from self (build side of join) and right (probe side of the join).
    pub fn probe(&self, partition_by: &Vec<Arc<dyn Array>>) -> (UInt32Array, UInt32Array) {
        let n_buckets = self.offsets.len() - 1;
        let right_buckets = kernel::hash(partition_by);
        let mut left_index = vec![];
        let mut right_index = vec![];
        // For each row from the probe side of the join, get the bucket...
        for i_right in 0..right_buckets.len() {
            let bucket = right_buckets.value(i_right) as usize % n_buckets;
            // ...for each row on the build side of the join with matching bucket...
            for i_left in self.offsets[bucket]..self.offsets[bucket + 1] {
                // ...add a row to the output, effectively performing a cross-join.
                left_index.push(i_left as u32);
                right_index.push(i_right as u32);
            }
        }
        let left_index = UInt32Array::from(left_index);
        let right_index = UInt32Array::from(right_index);
        (left_index, right_index)
    }
}

fn bucketize(hashes: &UInt64Array, n_buckets: usize) -> UInt32Array {
    let mut builder = UInt32Array::builder(hashes.len());
    for i in 0..hashes.len() {
        let bucket = hashes.value(i) % n_buckets as u64;
        builder.append_value(bucket as u32).unwrap();
    }
    builder.finish()
}

fn bucket_offsets(buckets: &UInt32Array, n_buckets: usize) -> Vec<usize> {
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
