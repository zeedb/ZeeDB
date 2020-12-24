use arrow::{array::*, datatypes::*};
use std::sync::Arc;

pub fn mask<P: MaskProvider>(test: &BooleanArray, if_true: &P, if_false: &P) -> P {
    P::mask(test, if_true, if_false)
}

pub trait MaskProvider {
    fn mask(test: &BooleanArray, if_true: &Self, if_false: &Self) -> Self;
}

macro_rules! impl_primitive {
    ($T:ty) => {
        impl MaskProvider for $T {
            fn mask(test: &BooleanArray, if_true: &Self, if_false: &Self) -> Self {
                assert_eq!(test.len(), if_true.len());
                assert_eq!(test.len(), if_false.len());

                let mut builder = Self::builder(test.len());
                for i in 0..test.len() {
                    if test.is_null(i) {
                        builder.append_null().unwrap();
                    } else if test.value(i) {
                        if if_true.is_null(i) {
                            builder.append_null().unwrap();
                        } else {
                            builder.append_value(if_true.value(i)).unwrap();
                        }
                    } else {
                        if if_false.is_null(i) {
                            builder.append_null().unwrap();
                        } else {
                            builder.append_value(if_false.value(i)).unwrap();
                        }
                    }
                }
                builder.finish()
            }
        }
    };
}

impl_primitive!(BooleanArray);
impl_primitive!(Int64Array);
impl_primitive!(Float64Array);
impl_primitive!(Date32Array);
impl_primitive!(TimestampMicrosecondArray);

impl MaskProvider for StringArray {
    fn mask(test: &BooleanArray, if_true: &Self, if_false: &Self) -> Self {
        assert_eq!(test.len(), if_true.len());
        assert_eq!(test.len(), if_false.len());

        let mut builder = StringBuilder::new(test.len());
        for i in 0..test.len() {
            if test.is_null(i) {
                builder.append_null().unwrap();
            } else if test.value(i) {
                if if_true.is_null(i) {
                    builder.append_null().unwrap();
                } else {
                    builder.append_value(if_true.value(i)).unwrap();
                }
            } else {
                if if_false.is_null(i) {
                    builder.append_null().unwrap();
                } else {
                    builder.append_value(if_false.value(i)).unwrap();
                }
            }
        }
        builder.finish()
    }
}

impl MaskProvider for Arc<dyn Array> {
    fn mask(test: &BooleanArray, if_true: &Self, if_false: &Self) -> Self {
        assert_eq!(if_true.data_type(), if_false.data_type());

        match if_true.data_type() {
            DataType::Boolean => Arc::new(mask(
                test,
                as_boolean_array(if_true),
                as_boolean_array(if_false),
            )),
            DataType::Int64 => Arc::new(mask(
                test,
                as_primitive_array::<Int64Type>(if_true),
                as_primitive_array::<Int64Type>(if_false),
            )),
            DataType::Float64 => Arc::new(mask(
                test,
                as_primitive_array::<Float64Type>(if_true),
                as_primitive_array::<Float64Type>(if_false),
            )),
            DataType::Date32(DateUnit::Day) => Arc::new(mask(
                test,
                as_primitive_array::<Date32Type>(if_true),
                as_primitive_array::<Date32Type>(if_false),
            )),
            DataType::Timestamp(TimeUnit::Microsecond, None) => Arc::new(mask(
                test,
                as_primitive_array::<TimestampMicrosecondType>(if_true),
                as_primitive_array::<TimestampMicrosecondType>(if_false),
            )),
            DataType::Utf8 => Arc::new(mask(
                test,
                as_string_array(if_true),
                as_string_array(if_false),
            )),
            other => panic!("mask({:?}) not supported", other),
        }
    }
}
