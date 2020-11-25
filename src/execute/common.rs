use arrow::array::*;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub fn concat_batches(batches: Vec<RecordBatch>) -> RecordBatch {
    let head = batches.first().unwrap();
    let schema = head.schema().clone();
    let n_columns = head.num_columns();
    let mut columns = Vec::with_capacity(n_columns);
    for i in 0..n_columns {
        let slice: Vec<Arc<dyn Array>> = batches
            .iter()
            .map(|batch| batch.column(i).clone())
            .collect();
        let column = arrow::compute::concat(&slice[..]).unwrap();
        columns.push(column);
    }
    RecordBatch::try_new(schema, columns).unwrap()
}

pub fn take_batch(batch: &RecordBatch, indices: &UInt32Array) -> RecordBatch {
    let columns = batch
        .columns()
        .iter()
        .map(|column| arrow::compute::take(column, indices, None).unwrap())
        .collect();
    RecordBatch::try_new(batch.schema().clone(), columns).unwrap()
}
