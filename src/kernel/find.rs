use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use ast::Column;
use std::sync::Arc;

pub fn find(input: &RecordBatch, column: &Column) -> Arc<dyn Array> {
    for i in 0..input.num_columns() {
        if input.schema().field(i).name() == &column.canonical_name() {
            return input.column(i).clone();
        }
    }
    panic!("{} is not in {}", column.name, input.schema())
}
