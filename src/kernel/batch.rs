use arrow::array::*;
use arrow::compute::SortColumn;
use arrow::datatypes::*;
use arrow::record_batch::*;
use ast::Column;
use std::sync::Arc;

pub fn zip(left: &RecordBatch, right: &RecordBatch) -> RecordBatch {
    assert!(left.num_rows() == right.num_rows());
    let mut columns = vec![];
    columns.extend_from_slice(left.columns());
    columns.extend_from_slice(right.columns());
    let mut fields = vec![];
    fields.extend_from_slice(left.schema().fields());
    fields.extend_from_slice(right.schema().fields());
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

pub fn update(any: &RecordBatch, updates: &Vec<(Column, Arc<dyn Array>)>) -> RecordBatch {
    todo!()
}

impl crate::Kernel for RecordBatch {
    fn cat(any: &Vec<Self>) -> Self {
        let head = any.first().unwrap();
        let schema = head.schema().clone();
        let n_columns = head.num_columns();
        let mut columns = Vec::with_capacity(n_columns);
        for i in 0..n_columns {
            let slice: Vec<Arc<dyn Array>> =
                any.iter().map(|batch| batch.column(i).clone()).collect();
            let column = arrow::compute::concat(&slice[..]).unwrap();
            columns.push(column);
        }
        RecordBatch::try_new(schema, columns).unwrap()
    }

    fn gather(any: &Self, uint32: &Arc<dyn Array>) -> Self {
        let columns = any
            .columns()
            .iter()
            .map(|column| crate::gather(column, uint32))
            .collect();
        RecordBatch::try_new(any.schema(), columns).unwrap()
    }

    fn gather_logical(any: &Self, boolean: &Arc<dyn Array>) -> Self {
        let columns = any
            .columns()
            .iter()
            .map(|column| crate::gather_logical(column, boolean))
            .collect();
        RecordBatch::try_new(any.schema(), columns).unwrap()
    }

    fn scatter(any: &Self, uint32: &Arc<dyn Array>) -> Self {
        let columns = any
            .columns()
            .iter()
            .map(|column| crate::scatter(column, uint32))
            .collect();
        RecordBatch::try_new(any.schema(), columns).unwrap()
    }

    fn repeat(any: &Self, len: usize) -> Self {
        let columns = any
            .columns()
            .iter()
            .map(|column| crate::repeat(column, len))
            .collect();
        RecordBatch::try_new(any.schema(), columns).unwrap()
    }

    fn sort(any: &Self) -> Arc<dyn Array> {
        let sort_columns: Vec<SortColumn> = any
            .columns()
            .iter()
            .map(|column| SortColumn {
                values: column.clone(),
                options: None,
            })
            .collect();
        let indexes = arrow::compute::lexsort_to_indices(&sort_columns[..]).unwrap();
        Arc::new(indexes)
    }
}
