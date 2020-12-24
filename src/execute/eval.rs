use crate::execute::Session;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use ast::*;
use kernel::Error;
use std::sync::Arc;

pub fn all(
    predicates: &Vec<Scalar>,
    input: &RecordBatch,
    state: &mut Session,
) -> Result<BooleanArray, Error> {
    let mut mask = BooleanArray::from(vec![true].repeat(input.num_rows()));
    for p in predicates {
        let next = crate::eval::eval(p, &input, state)?;
        mask = arrow::compute::and(&mask, as_boolean_array(&next))?;
    }
    Ok(mask)
}

pub fn eval(
    scalar: &Scalar,
    input: &RecordBatch,
    state: &mut Session,
) -> Result<Arc<dyn Array>, Error> {
    match scalar {
        Scalar::Literal(value) => Ok(repeat(&value.array(), input.num_rows())),
        Scalar::Column(column) => {
            let i = (0..input.num_columns())
                .find(|i| input.schema().field(*i).name() == &column.canonical_name())
                .expect(format!("no column {:?} in {}", column, input.schema()).as_str());
            Ok(input.column(i).clone())
        }
        Scalar::Parameter(name, _) => {
            let value: &Arc<dyn Array> = state.variables.get(name).as_ref().expect(name);
            assert_eq!(value.len(), 1);
            Ok(repeat(value, input.num_rows()))
        }
        Scalar::Call(function) => match function.as_ref() {
            Function::CurrentDate => todo!(),
            Function::CurrentTimestamp => todo!(),
            Function::Rand => todo!(),
            Function::IsNull(argument) => Ok(Arc::new(arrow::compute::is_null(&eval(
                argument, input, state,
            )?)?)),
            Function::Not(argument) => Ok(Arc::new(arrow::compute::not(&as_boolean_array(
                &eval(argument, input, state)?,
            ))?)),
            Function::UnaryMinus(_) => todo!(),
            Function::And(left, right) => Ok(Arc::new(arrow::compute::and(
                &as_boolean_array(&eval(left, input, state)?),
                &as_boolean_array(&eval(right, input, state)?),
            )?)),
            Function::Is(left, right) => Ok(Arc::new(kernel::is(
                &eval(left, input, state)?,
                &eval(right, input, state)?,
            ))),
            Function::Equal(left, right) => Ok(Arc::new(kernel::equal(
                &eval(left, input, state)?,
                &eval(right, input, state)?,
            ))),
            Function::Greater(left, right) => Ok(Arc::new(kernel::greater(
                &eval(left, input, state)?,
                &eval(right, input, state)?,
            ))),
            Function::GreaterOrEqual(left, right) => Ok(Arc::new(kernel::greater_equal(
                &eval(left, input, state)?,
                &eval(right, input, state)?,
            ))),
            Function::Less(left, right) => Ok(Arc::new(kernel::less(
                &eval(left, input, state)?,
                &eval(right, input, state)?,
            ))),
            Function::LessOrEqual(left, right) => Ok(Arc::new(kernel::less_equal(
                &eval(left, input, state)?,
                &eval(right, input, state)?,
            ))),
            Function::Like(_, _) => todo!(),
            Function::NotEqual(left, right) => Ok(Arc::new(kernel::equal(
                &eval(left, input, state)?,
                &eval(right, input, state)?,
            ))),
            Function::Or(left, right) => Ok(Arc::new(arrow::compute::or(
                &as_boolean_array(&eval(left, input, state)?),
                &as_boolean_array(&eval(right, input, state)?),
            )?)),
            Function::Add(left, right, _) => Ok(kernel::add(
                &eval(left, input, state)?,
                &eval(right, input, state)?,
            )?),
            Function::Divide(left, right, _) => Ok(kernel::div(
                &eval(left, input, state)?,
                &eval(right, input, state)?,
            )?),
            Function::Multiply(left, right, _) => Ok(kernel::mul(
                &eval(left, input, state)?,
                &eval(right, input, state)?,
            )?),
            Function::Subtract(left, right, _) => Ok(kernel::sub(
                &eval(left, input, state)?,
                &eval(right, input, state)?,
            )?),
            Function::Coalesce(_, _, _) => todo!(),
            Function::CaseNoValue(test, if_true, if_false, _) => Ok(kernel::mask(
                &as_boolean_array(&eval(test, input, state)?),
                &eval(if_true, input, state)?,
                &eval(if_false, input, state)?,
            )),
            Function::NextVal(sequence) => {
                let input = eval(sequence, input, state)?;
                let input: &Int64Array = input.as_any().downcast_ref::<Int64Array>().unwrap();
                let mut output = Int64Builder::new(input.len());
                for i in 0..input.len() {
                    assert!(!input.is_null(i));
                    let next = state.storage.next_val(input.value(i));
                    output.append_value(next).unwrap();
                }
                Ok(Arc::new(output.finish()))
            }
            Function::Xid => Ok(Arc::new(Int64Array::from(
                vec![state.txn].repeat(input.num_rows()),
            ))),
        },
        Scalar::Cast(scalar, data_type) => Ok(arrow::compute::cast(
            &eval(scalar, input, state)?,
            data_type,
        )?),
    }
}

pub fn repeat(any: &Arc<dyn Array>, len: usize) -> Arc<dyn Array> {
    let mut alias = vec![];
    alias.resize_with(len, || any.clone());
    arrow::compute::concat(&alias[..]).unwrap()
}
