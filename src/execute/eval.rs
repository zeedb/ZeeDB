use arrow::array::*;
use arrow::compute::*;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use ast::*;
use std::sync::Arc;

pub trait Eval {
    fn eval(&self, input: &RecordBatch) -> Result<Arc<dyn Array>, ArrowError>;
}

impl Eval for Scalar {
    fn eval(&self, input: &RecordBatch) -> Result<Arc<dyn Array>, ArrowError> {
        todo!()
    }
}
