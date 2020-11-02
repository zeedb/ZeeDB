use arrow::record_batch::RecordBatch;

pub fn execute(expr: &ast::Expr, storage: &storage::Storage) -> Result<(RecordBatch, i32), String> {
    todo!()
}
