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
            Function::And(_, _) => todo!(),
            Function::Equal(left, right) => match left.data() {
                DataType::Boolean => binary(arrow::compute::eq_utf8, left, right, input, state),
                DataType::Int64 => {
                    binary(arrow::compute::eq::<Int64Type>, left, right, input, state)
                }
                DataType::Float64 => {
                    binary(arrow::compute::eq::<Float64Type>, left, right, input, state)
                }
                DataType::Date32(DateUnit::Day) => {
                    binary(arrow::compute::eq::<Date32Type>, left, right, input, state)
                }
                DataType::Timestamp(TimeUnit::Microsecond, None) => binary(
                    arrow::compute::eq::<TimestampMicrosecondType>,
                    left,
                    right,
                    input,
                    state,
                ),
                DataType::FixedSizeBinary(16) => todo!(),
                DataType::Utf8 => binary(arrow::compute::eq_utf8, left, right, input, state),
                DataType::Struct(fields) => todo!(),
                DataType::List(element) => todo!(),
                other => panic!("{:?} is not a supported type", other),
            },
            Function::Greater(_, _) => todo!(),
            Function::GreaterOrEqual(_, _) => todo!(),
            Function::Less(_, _) => todo!(),
            Function::LessOrEqual(_, _) => todo!(),
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
        },
        Scalar::Cast(scalar, as_type) => todo!(),
    }
}

fn binary<T: 'static, U: Array + 'static>(
    f: impl Fn(&T, &T) -> arrow::error::Result<U>,
    left: &Scalar,
    right: &Scalar,
    input: &RecordBatch,
    state: &mut State,
) -> Result<Arc<dyn Array>, Error> {
    let left = eval(left, input, state)?;
    let right = eval(right, input, state)?;
    let left = left.as_any().downcast_ref::<T>().unwrap();
    let right = right.as_any().downcast_ref::<T>().unwrap();
    let output = f(left, right)?;
    Ok(Arc::new(output))
}
