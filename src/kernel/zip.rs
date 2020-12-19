use arrow::datatypes::*;
use arrow::record_batch::*;
use std::sync::Arc;

pub fn zip(left: &RecordBatch, right: &RecordBatch) -> RecordBatch {
    assert_eq!(left.num_rows(), right.num_rows());
    let mut columns = vec![];
    columns.extend_from_slice(left.columns());
    columns.extend_from_slice(right.columns());
    let mut fields = vec![];
    fields.extend_from_slice(left.schema().fields());
    fields.extend_from_slice(right.schema().fields());
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}
