use arrow::array::*;
use arrow::datatypes::*;
use chrono::*;
use std::any::Any;
use std::convert::TryInto;
use std::fmt;
use std::hash;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Value {
    pub inner: Arc<dyn Array>,
}

impl Value {
    pub fn new(inner: Box<dyn Any>, as_type: DataType) -> Self {
        let inner: Arc<dyn Array> = match as_type {
            DataType::Boolean => Arc::new(BooleanArray::from(vec![*inner
                .downcast_ref::<bool>()
                .unwrap()])),
            DataType::Int64 => Arc::new(Int64Array::from(vec![*inner
                .downcast_ref::<i64>()
                .unwrap()])),
            DataType::Float64 => Arc::new(Float64Array::from(vec![*inner
                .downcast_ref::<f64>()
                .unwrap()])),
            DataType::Utf8 => Arc::new(StringArray::from(vec![inner
                .downcast_ref::<String>()
                .unwrap()
                .as_str()])),
            DataType::Date32(DateUnit::Day) => Arc::new(Date32Array::from(vec![*inner
                .downcast_ref::<i32>()
                .unwrap()])),
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                Arc::new(TimestampMicrosecondArray::from(vec![*inner
                    .downcast_ref::<i64>()
                    .unwrap()]))
            }
            DataType::FixedSizeBinary(16) => {
                let inner: i128 = *inner.downcast_ref::<i128>().unwrap();
                let bytes = inner.to_be_bytes();
                let mut array = FixedSizeBinaryBuilder::new(16, 16);
                array.append_value(&bytes[..]).unwrap();
                Arc::new(array.finish())
            }
            other => panic!("{:?} is not a supported type", other),
        };
        Self { inner }
    }

    pub fn data(&self) -> &DataType {
        &self.inner.data_type()
    }

    pub fn bool(&self) -> Option<bool> {
        if let DataType::Boolean = self.data() {
            Some(
                self.inner
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(0),
            )
        } else {
            None
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.data() {
            DataType::Boolean => write!(
                f,
                "{}",
                self.inner
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(0)
            ),
            DataType::Int64 => write!(
                f,
                "{}",
                self.inner
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            ),
            DataType::Float64 => write!(
                f,
                "{}",
                self.inner
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0)
            ),
            DataType::Utf8 => write!(
                f,
                "{:?}",
                self.inner
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0)
            ),
            DataType::Date32(DateUnit::Day) => write!(
                f,
                "{}",
                date_value(
                    self.inner
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .unwrap()
                        .value(0)
                )
            ),
            DataType::Timestamp(TimeUnit::Microsecond, None) => write!(
                f,
                "{}",
                timestamp_value(
                    self.inner
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .value(0)
                )
            ),
            DataType::FixedSizeBinary(16) => {
                let bytes: &[u8] = self
                    .inner
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap()
                    .value(0);
                let number = numeric_value(bytes);
                write!(f, "{}", number)
            }
            other => panic!("{:?} is not a supported type", other),
        }
    }
}

impl Eq for Value {}
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}
impl hash::Hash for Value {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        std::ptr::hash(Arc::as_ptr(&self.inner), state)
    }
}

fn date_value(date: i32) -> NaiveDate {
    NaiveDate::from_ymd(1970, 1, 1) + Duration::days(date as i64)
}

fn timestamp_value(time: i64) -> DateTime<Utc> {
    DateTime::from_utc(
        NaiveDateTime::from_timestamp(time / 1_000_000, ((time % 1_000_000) * 1_000_000) as u32),
        Utc,
    )
}

fn numeric_value(bytes: &[u8]) -> i128 {
    let array: [u8; 16] = bytes.try_into().unwrap();
    i128::from_be_bytes(array)
}
