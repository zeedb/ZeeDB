use crate::error::Error;
use crate::state::State;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use ast::*;
use std::sync::Arc;

pub fn filter(
    predicates: &Vec<Scalar>,
    input: RecordBatch,
    state: &mut State,
) -> Result<RecordBatch, Error> {
    if predicates.is_empty() {
        return Ok(input);
    }
    let mut mask = crate::eval::eval(&predicates[0], state, &input)?;
    for p in &predicates[1..] {
        let next = crate::eval::eval(p, state, &input)?;
        mask = Arc::new(arrow::compute::and(
            mask.as_any().downcast_ref::<BooleanArray>().unwrap(),
            next.as_any().downcast_ref::<BooleanArray>().unwrap(),
        )?);
    }
    let mut columns = vec![];
    for c in input.columns() {
        columns.push(arrow::compute::filter(
            c.as_ref(),
            mask.as_any().downcast_ref::<BooleanArray>().unwrap(),
        )?)
    }
    Ok(RecordBatch::try_new(input.schema().clone(), columns)?)
}
