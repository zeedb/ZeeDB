use crate::error::Error;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use ast::*;
use std::sync::Arc;

pub fn eval(scalar: &Scalar, input: &RecordBatch) -> Result<Arc<dyn Array>, Error> {
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
        Scalar::Call(function) => todo!(),
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
