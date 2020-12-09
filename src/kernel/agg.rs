use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;

pub fn avg(array: &Arc<dyn Array>) -> Arc<dyn Array> {
    todo!()
}

pub fn bit_and(array: &Arc<dyn Array>) -> Arc<dyn Array> {
    todo!()
}

pub fn bit_or(array: &Arc<dyn Array>) -> Arc<dyn Array> {
    todo!()
}

pub fn bit_xor(array: &Arc<dyn Array>) -> Arc<dyn Array> {
    todo!()
}

pub fn count(array: &Arc<dyn Array>) -> Arc<dyn Array> {
    todo!()
}

pub fn logical_and(array: &Arc<dyn Array>) -> Arc<dyn Array> {
    todo!()
}

pub fn logical_or(array: &Arc<dyn Array>) -> Arc<dyn Array> {
    todo!()
}

pub fn max(array: &Arc<dyn Array>) -> Arc<dyn Array> {
    todo!()
}

pub fn min(array: &Arc<dyn Array>) -> Arc<dyn Array> {
    todo!()
}

pub fn string_agg(array: &Arc<dyn Array>) -> Arc<dyn Array> {
    todo!()
}

pub fn sum(array: &Arc<dyn Array>) -> Arc<dyn Array> {
    match array.data_type() {
        DataType::Int64 => {
            let sum = arrow::compute::sum(as_primitive_array::<Int64Type>(array));
            let int64 = Int64Array::from(vec![sum]);
            Arc::new(int64)
        }
        DataType::Float64 => {
            let sum = arrow::compute::sum(as_primitive_array::<Float64Type>(array));
            let float64 = Float64Array::from(vec![sum]);
            Arc::new(float64)
        }
        DataType::FixedSizeBinary(16) => todo!(),
        other => panic!("sum({:?}) is not supported", other),
    }
}
