use arrow::array::*;
use arrow::datatypes::*;
use arrow::error::ArrowError;
use std::sync::Arc;

pub trait MathProvider: Sized {
    fn div(left: &Self, right: &Self) -> Result<Self, ArrowError>;
    fn mul(left: &Self, right: &Self) -> Result<Self, ArrowError>;
    fn add(left: &Self, right: &Self) -> Result<Self, ArrowError>;
    fn sub(left: &Self, right: &Self) -> Result<Self, ArrowError>;
}

pub fn div<P: MathProvider>(left: &P, right: &P) -> Result<P, ArrowError> {
    P::div(left, right)
}

pub fn mul<P: MathProvider>(left: &P, right: &P) -> Result<P, ArrowError> {
    P::mul(left, right)
}

pub fn add<P: MathProvider>(left: &P, right: &P) -> Result<P, ArrowError> {
    P::add(left, right)
}

pub fn sub<P: MathProvider>(left: &P, right: &P) -> Result<P, ArrowError> {
    P::sub(left, right)
}

macro_rules! primitive {
    ($T:ty) => {
        impl MathProvider for $T {
            fn div(left: &Self, right: &Self) -> Result<Self, ArrowError> {
                arrow::compute::divide(left, right)
            }
            fn mul(left: &Self, right: &Self) -> Result<Self, ArrowError> {
                arrow::compute::multiply(left, right)
            }
            fn add(left: &Self, right: &Self) -> Result<Self, ArrowError> {
                arrow::compute::add(left, right)
            }
            fn sub(left: &Self, right: &Self) -> Result<Self, ArrowError> {
                arrow::compute::subtract(left, right)
            }
        }
    };
}

primitive!(Int64Array);
primitive!(Float64Array);

impl MathProvider for Arc<dyn Array> {
    fn div(left: &Self, right: &Self) -> Result<Self, ArrowError> {
        match left.data_type() {
            DataType::Int64 => Ok(Arc::new(div(
                as_primitive_array::<Int64Type>(left),
                as_primitive_array::<Int64Type>(right),
            )?)),
            DataType::Float64 => Ok(Arc::new(div(
                as_primitive_array::<Float64Type>(left),
                as_primitive_array::<Float64Type>(right),
            )?)),
            other => panic!("/({:?}) is not supported", other),
        }
    }
    fn mul(left: &Self, right: &Self) -> Result<Self, ArrowError> {
        match left.data_type() {
            DataType::Int64 => Ok(Arc::new(mul(
                as_primitive_array::<Int64Type>(left),
                as_primitive_array::<Int64Type>(right),
            )?)),
            DataType::Float64 => Ok(Arc::new(mul(
                as_primitive_array::<Float64Type>(left),
                as_primitive_array::<Float64Type>(right),
            )?)),
            other => panic!("*({:?}) is not supported", other),
        }
    }
    fn add(left: &Self, right: &Self) -> Result<Self, ArrowError> {
        match left.data_type() {
            DataType::Int64 => Ok(Arc::new(add(
                as_primitive_array::<Int64Type>(left),
                as_primitive_array::<Int64Type>(right),
            )?)),
            DataType::Float64 => Ok(Arc::new(add(
                as_primitive_array::<Float64Type>(left),
                as_primitive_array::<Float64Type>(right),
            )?)),
            other => panic!("+({:?}) is not supported", other),
        }
    }
    fn sub(left: &Self, right: &Self) -> Result<Self, ArrowError> {
        match left.data_type() {
            DataType::Int64 => Ok(Arc::new(sub(
                as_primitive_array::<Int64Type>(left),
                as_primitive_array::<Int64Type>(right),
            )?)),
            DataType::Float64 => Ok(Arc::new(sub(
                as_primitive_array::<Float64Type>(left),
                as_primitive_array::<Float64Type>(right),
            )?)),
            other => panic!("-({:?}) is not supported", other),
        }
    }
}
