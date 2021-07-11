use kernel::*;

/// HashTable stores a large set of tuples in a fixed-size, dense hash table.
/// tuples[buckets[i]..buckets[i + 1]] contains all tuples where hash(tuple) % (buckets.len() - 1) == i
#[derive(Debug)]
pub struct HashTable {
    offsets: Vec<usize>,
    tuples: RecordBatch,
}

impl HashTable {
    pub fn new(input: &RecordBatch, partition_by: &I64Array) -> Self {
        let n_rows = input.len();
        let n_buckets = size_hash_table(n_rows);
        let buckets = partition_by.hash_buckets(n_buckets);
        let indexes = buckets.sort();
        let tuples = input.gather(&indexes);
        let offsets = bucket_offsets(&buckets, n_buckets);
        HashTable { offsets, tuples }
    }

    /// Identify matching rows from self (build side of join) and right (probe side of the join).
    pub fn probe(&self, partition_by: &I64Array) -> (I32Array, I32Array) {
        let n_buckets = self.offsets.len() - 1;
        let mut left_index = vec![];
        let mut right_index = vec![];
        // For each row from the probe side of the join, get the bucket...
        for i_right in 0..partition_by.len() {
            let bucket = partition_by.get(i_right).unwrap() as usize % n_buckets;
            // ...for each row on the build side of the join with matching bucket...
            for i_left in self.offsets[bucket]..self.offsets[bucket + 1] {
                // ...add a row to the output, effectively performing a cross-join.
                left_index.push(i_left as i32);
                right_index.push(i_right as i32);
            }
        }
        let left_index = I32Array::from_values(left_index);
        let right_index = I32Array::from_values(right_index);
        (left_index, right_index)
    }

    pub fn build(&self) -> &RecordBatch {
        &self.tuples
    }
}

fn bucket_offsets(buckets: &I32Array, n_buckets: usize) -> Vec<usize> {
    // Compute a dense histogram of buckets in offsets[1..n_buckets+1].
    let mut offsets = Vec::with_capacity(n_buckets);
    offsets.resize_with(n_buckets + 1, usize::default);
    for i in 0..buckets.len() {
        offsets[buckets.get(i).unwrap() as usize + 1] += 1;
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
