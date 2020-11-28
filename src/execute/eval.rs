use crate::state::State;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use ast::*;
use kernel::Error;
use std::sync::Arc;

pub fn all(
    predicates: &Vec<Scalar>,
    input: &RecordBatch,
    state: &mut State,
) -> Result<Arc<dyn Array>, Error> {
    let mut mask = BooleanArray::from(vec![true].repeat(input.num_rows()));
    for p in predicates {
        let next = crate::eval::eval(p, &input, state)?;
        mask = arrow::compute::and(&mask, kernel::coerce(&next))?;
    }
    Ok(Arc::new(mask))
}

pub fn evals(
    values: &Vec<Scalar>,
    input: &RecordBatch,
    state: &mut State,
) -> Result<Arc<dyn Array>, Error> {
    let mut output = Vec::with_capacity(values.len());
    for value in values {
        let value = crate::eval::eval(value, input, state)?;
        if value.len() != 1 {
            return Err(Error::MultipleRows);
        }
        output.push(value)
    }
    Ok(arrow::compute::concat(&output[..])?)
}

pub fn eval(
    scalar: &Scalar,
    input: &RecordBatch,
    state: &mut State,
) -> Result<Arc<dyn Array>, Error> {
    match scalar {
        Scalar::Literal(value) => Ok(kernel::repeat(&value.inner, input.num_rows())),
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
        Scalar::Parameter(name, _) => {
            let value: &Arc<dyn Array> = state.variables.get(name).as_ref().expect(name);
            assert!(value.len() == 1);
            Ok(kernel::repeat(value, input.num_rows()))
        }
        Scalar::Call(function) => match function.as_ref() {
            Function::CurrentDate => todo!(),
            Function::CurrentTimestamp => todo!(),
            Function::Rand => todo!(),
            Function::Not(_) => todo!(),
            Function::UnaryMinus(_) => todo!(),
            Function::And(left, right) => Ok(kernel::and(
                &eval(left, input, state)?,
                &eval(right, input, state)?,
            )?),
            Function::Equal(left, right) => Ok(kernel::equal(
                &eval(left, input, state)?,
                &eval(right, input, state)?,
            )?),
            Function::Greater(_, _) => todo!(),
            Function::GreaterOrEqual(_, _) => todo!(),
            Function::Less(left, right) => Ok(kernel::less(
                &eval(left, input, state)?,
                &eval(right, input, state)?,
            )?),
            Function::LessOrEqual(left, right) => Ok(kernel::less_equal(
                &eval(left, input, state)?,
                &eval(right, input, state)?,
            )?),
            Function::Like(_, _) => todo!(),
            Function::NotEqual(_, _) => todo!(),
            Function::Or(_, _) => todo!(),
            Function::Add(_, _, _) => todo!(),
            Function::Divide(_, _, _) => todo!(),
            Function::Multiply(_, _, _) => todo!(),
            Function::Subtract(_, _, _) => todo!(),
            Function::NextVal(sequence) => {
                let input = eval(sequence, input, state)?;
                let input: &Int64Array = input.as_any().downcast_ref::<Int64Array>().unwrap();
                let mut output = Int64Builder::new(input.len());
                for i in 0..input.len() {
                    let next = state.storage.next_val(input.value(i));
                    output.append_value(next).unwrap();
                }
                Ok(Arc::new(output.finish()))
            }
            Function::Xid => Ok(Arc::new(UInt64Array::from(
                vec![state.txn].repeat(input.num_rows()),
            ))),
        },
        Scalar::Cast(scalar, as_type) => todo!(),
    }
}

pub fn hash(
    scalars: &Vec<Scalar>,
    n_buckets: usize,
    input: &RecordBatch,
    state: &mut State,
) -> Result<Arc<dyn Array>, Error> {
    let mut output = None;
    for scalar in scalars {
        let any = crate::eval::eval(scalar, input, state)?;
        let next = kernel::hash(&any, n_buckets);
        output = match output {
            None => Some(next),
            Some(prev) => Some(kernel::bit_or(&prev, &next)),
        }
    }
    Ok(output.unwrap())
}
