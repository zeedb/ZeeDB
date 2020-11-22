use crate::error::Error;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use ast::*;
use std::sync::Arc;

pub trait Eval {
    fn eval(&self, input: &RecordBatch) -> Result<Arc<dyn Array>, Error>;
}

impl Eval for Scalar {
    fn eval(&self, input: &RecordBatch) -> Result<Arc<dyn Array>, Error> {
        match self {
            Scalar::Literal(value, as_type) => todo!(),
            Scalar::Column(column) => {
                let i = (0..input.num_columns())
                    .find(|i| input.schema().field(*i).name() == &column.canonical_name())
                    .expect(
                        format!(
                            "no column {} in {}",
                            column.canonical_name(),
                            input.schema()
                        )
                        .as_str(),
                    );
                Ok(input.column(i).clone())
            }
            Scalar::Call(function) => todo!(),
            Scalar::Cast(scalar, as_type) => todo!(),
        }
    }
}
