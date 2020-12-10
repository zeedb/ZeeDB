use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;

pub fn any_value(array: &Arc<dyn Array>) -> Arc<dyn Array> {
    array.slice(0, 1)
}

/*
    if array.is_empty() {
        None
    } else {
        match array.data_type() {
            DataType::Boolean => todo!(),
            DataType::Int64 => todo!(),
            DataType::Float64 => todo!(),
            DataType::FixedSizeBinary(16) => todo!(),
            DataType::Timestamp(TimeUnit::Microsecond, None) => todo!(),
            DataType::Date32(DateUnit::Day) => todo!(),
            DataType::Utf8 => todo!(),
            other => panic!("+({:?}) is not supported", other),
        }
    }
*/
pub fn count(array: &Arc<dyn Array>) -> usize {
    let mut count = 0;
    for i in 0..array.len() {
        if array.is_valid(i) {
            count += 1;
        }
    }
    count
}

pub fn logical_and(array: &Arc<dyn Array>) -> Option<bool> {
    if array.null_count() == array.len() {
        return None;
    }
    let array = as_boolean_array(array);
    for i in 0..array.len() {
        if array.is_valid(i) && !array.value(i) {
            return Some(false);
        }
    }
    Some(true)
}

pub fn logical_or(array: &Arc<dyn Array>) -> Option<bool> {
    if array.null_count() == array.len() {
        return None;
    }
    let array = as_boolean_array(array);
    for i in 0..array.len() {
        if array.is_valid(i) && array.value(i) {
            return Some(true);
        }
    }
    Some(false)
}

pub fn max(array: &Arc<dyn Array>) -> Arc<dyn Array> {
    match array.data_type() {
        DataType::Boolean => todo!(),
        DataType::Int64 => {
            let x = arrow::compute::max(as_primitive_array::<Int64Type>(array));
            Arc::new(Int64Array::from(vec![x]))
        }
        DataType::Float64 => {
            let x = arrow::compute::max(as_primitive_array::<Float64Type>(array));
            Arc::new(Float64Array::from(vec![x]))
        }
        DataType::FixedSizeBinary(16) => todo!(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let x = arrow::compute::max(as_primitive_array::<TimestampMicrosecondType>(array));
            Arc::new(TimestampMicrosecondArray::from(vec![x]))
        }
        DataType::Date32(DateUnit::Day) => {
            let x = arrow::compute::max(as_primitive_array::<Date32Type>(array));
            Arc::new(Date32Array::from(vec![x]))
        }
        DataType::Utf8 => {
            let x = arrow::compute::max_string(as_string_array(array));
            Arc::new(StringArray::from(vec![x]))
        }
        other => panic!("max({:?}) is not supported", other),
    }
}

pub fn min(array: &Arc<dyn Array>) -> Arc<dyn Array> {
    match array.data_type() {
        DataType::Boolean => todo!(),
        DataType::Int64 => {
            let x = arrow::compute::min(as_primitive_array::<Int64Type>(array));
            Arc::new(Int64Array::from(vec![x]))
        }
        DataType::Float64 => {
            let x = arrow::compute::min(as_primitive_array::<Float64Type>(array));
            Arc::new(Float64Array::from(vec![x]))
        }
        DataType::FixedSizeBinary(16) => todo!(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let x = arrow::compute::min(as_primitive_array::<TimestampMicrosecondType>(array));
            Arc::new(TimestampMicrosecondArray::from(vec![x]))
        }
        DataType::Date32(DateUnit::Day) => {
            let x = arrow::compute::min(as_primitive_array::<Date32Type>(array));
            Arc::new(Date32Array::from(vec![x]))
        }
        DataType::Utf8 => {
            let x = arrow::compute::max_string(as_string_array(array));
            Arc::new(StringArray::from(vec![x]))
        }
        other => panic!("min({:?}) is not supported", other),
    }
}

pub fn sum(array: &Arc<dyn Array>) -> Arc<dyn Array> {
    match array.data_type() {
        DataType::Int64 => {
            let x = arrow::compute::sum(as_primitive_array::<Int64Type>(array));
            Arc::new(Int64Array::from(vec![x]))
        }
        DataType::Float64 => {
            let x = arrow::compute::sum(as_primitive_array::<Float64Type>(array));
            Arc::new(Float64Array::from(vec![x]))
        }
        DataType::FixedSizeBinary(16) => todo!(),
        other => panic!("sum({:?}) is not supported", other),
    }
}
