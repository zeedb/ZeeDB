use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use ast::OrderBy;
use kernel::Error;
use std::sync::Arc;

pub fn sort(input: RecordBatch, order_by: &Vec<OrderBy>) -> Result<RecordBatch, Error> {
    let sort_options = |order_by: &OrderBy| arrow::compute::SortOptions {
        descending: order_by.descending,
        nulls_first: order_by.nulls_first,
    };
    let sort_column = |order_by: &OrderBy| arrow::compute::SortColumn {
        options: Some(sort_options(order_by)),
        values: kernel::find(&input, &order_by.column),
    };
    let order_by: Vec<arrow::compute::SortColumn> = order_by.iter().map(sort_column).collect();
    let indices = arrow::compute::lexsort_to_indices(order_by.as_slice())?;
    let indices: Arc<dyn Array> = Arc::new(indices);
    Ok(kernel::gather(&input, &indices))
}
