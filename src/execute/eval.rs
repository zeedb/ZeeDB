use crate::error::Error;
use crate::state::State;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use ast::*;
use std::sync::Arc;

pub(crate) fn eval(
    scalar: &Scalar,
    state: &mut State,
    input: &RecordBatch,
) -> Result<Arc<dyn Array>, Error> {
    match scalar {
        Scalar::Literal(value, as_type) => match value {
            Value::Int64(value) => Ok(repeat_primitive::<Int64Type>(*value, input.num_rows())),
            Value::Bool(value) => Ok(repeat_bool(*value, input.num_rows())),
            Value::Double(value) => Ok(repeat_primitive::<Float64Type>(
                value.parse::<f64>().unwrap(),
                input.num_rows(),
            )),
            Value::String(value) => Ok(repeat_string(value, input.num_rows())),
            Value::Date(value) => Ok(repeat_primitive::<Date32Type>(*value, input.num_rows())),
            Value::Timestamp(value) => Ok(repeat_timestamp(*value, input.num_rows())),
            Value::Numeric(value) => todo!(),
            Value::Array(value) => todo!(),
            Value::Struct(value) => todo!(),
        },
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
        Scalar::Parameter(name, as_type) => todo!(),
        Scalar::Call(function) => match function.as_ref() {
            Function::CurrentDate => todo!(),
            Function::CurrentTimestamp => todo!(),
            Function::Rand => todo!(),
            Function::Not(_) => todo!(),
            Function::UnaryMinus(_) => todo!(),
            Function::And(_, _) => todo!(),
            Function::Equal(_, _) => todo!(),
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
            Function::NextVal(_) => todo!(),
        },
        Scalar::Cast(scalar, as_type) => todo!(),
    }
}

fn repeat_bool(value: bool, num_rows: usize) -> Arc<dyn Array> {
    let array = BooleanArray::from(vec![value].repeat(num_rows));
    Arc::new(array)
}

fn repeat_primitive<T: ArrowNumericType>(value: T::Native, num_rows: usize) -> Arc<dyn Array> {
    let mut builder = PrimitiveArray::<T>::builder(num_rows * std::mem::size_of::<T::Native>());
    for _ in 0..num_rows {
        builder.append_value(value).unwrap();
    }
    let array = builder.finish();
    Arc::new(array)
}

fn repeat_timestamp(value: i64, num_rows: usize) -> Arc<dyn Array> {
    let array = TimestampMicrosecondArray::from(vec![value].repeat(num_rows));
    Arc::new(array)
}

fn repeat_string(value: &String, num_rows: usize) -> Arc<dyn Array> {
    let mut builder = StringBuilder::new(num_rows * value.bytes().len());
    for _ in 0..num_rows {
        builder.append_value(value).unwrap();
    }
    let array = builder.finish();
    Arc::new(array)
}
