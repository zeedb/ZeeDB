use crate::error::Error;
use crate::hash_table::HashTable;
use crate::state::State;
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use ast::*;
use std::sync::Arc;

pub fn hash_join(
    left: &HashTable,
    right: RecordBatch,
    partition_right: &Vec<Scalar>,
    join: &Join,
    state: &mut State,
) -> Result<RecordBatch, Error> {
    let buckets = left.hash(partition_right, state, &right)?;
    let mut output = vec![];
    for (bucket, right) in partition(right, buckets) {
        output.push(nested_loop(&left.get(bucket), right, join, state)?);
    }
    Ok(crate::common::concat_batches(output))
}

fn partition(input: RecordBatch, buckets: Vec<usize>) -> Vec<(usize, RecordBatch)> {
    let max_bucket = buckets.iter().max().unwrap_or(&0);
    let mut indices: Vec<UInt32Builder> = Vec::with_capacity(max_bucket + 1);
    indices.resize_with(max_bucket + 1, || UInt32Builder::new(0));
    for (i, bucket) in buckets.iter().enumerate() {
        indices[*bucket].append_value(i as u32).unwrap();
    }
    let mut partitions = Vec::with_capacity(indices.len());
    for bucket in 0..indices.len() {
        let indices = &indices[bucket].finish();
        let batch = crate::common::take_batch(&input, indices);
        partitions.push((bucket, batch));
    }
    partitions
}

pub fn nested_loop(
    left: &RecordBatch,
    right: RecordBatch,
    join: &Join,
    state: &mut State,
) -> Result<RecordBatch, Error> {
    match join {
        Join::Inner(predicates) => {
            crate::filter::filter(predicates, cross_product(left, &right)?, state)
        }
        Join::Right(_) => todo!(),
        Join::Outer(_) => todo!(),
        Join::Semi(_) => todo!(),
        Join::Anti(_) => todo!(),
        Join::Single(_) => todo!(),
        Join::Mark(_, _) => todo!(),
    }
}

fn cross_product(left: &RecordBatch, right: &RecordBatch) -> Result<RecordBatch, Error> {
    let num_rows = left.num_rows() * right.num_rows();
    let mut index_left = UInt32Builder::new(num_rows);
    let mut index_right = UInt32Builder::new(num_rows);
    for i in 0..left.num_rows() {
        for j in 0..right.num_rows() {
            index_left.append_value(i as u32).unwrap();
            index_right.append_value(j as u32).unwrap();
        }
    }
    let left = crate::common::take_batch(left, &index_left.finish());
    let right = crate::common::take_batch(right, &index_right.finish());
    let mut columns = vec![];
    columns.extend_from_slice(left.columns());
    columns.extend_from_slice(right.columns());
    let mut fields = vec![];
    fields.extend_from_slice(left.schema().fields());
    fields.extend_from_slice(right.schema().fields());
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap())
}
