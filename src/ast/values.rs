use arrow::array::*;
use arrow::datatypes::*;
use chrono::*;
use std::fmt;
use std::hash;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum Value {
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Timestamp(i64),
    Date(i32),
}

impl Value {
    pub fn from(any: &Arc<dyn Array>) -> Option<Self> {
        if any.is_null(0) {
            None
        } else {
            let value = match any.data_type() {
                DataType::Boolean => {
                    Value::Boolean(as_primitive_array::<BooleanType>(any).value(0))
                }
                DataType::Int64 => Value::Int64(as_primitive_array::<Int64Type>(any).value(0)),
                DataType::Float64 => {
                    Value::Float64(as_primitive_array::<Float64Type>(any).value(0))
                }
                DataType::Utf8 => Value::Utf8(as_string_array(any).value(0).to_string()),
                DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    Value::Timestamp(as_primitive_array::<TimestampMicrosecondType>(any).value(0))
                }
                DataType::Date32(DateUnit::Day) => {
                    Value::Date(as_primitive_array::<Date32Type>(any).value(0))
                }
                other => panic!("type {:?} is not supported", other),
            };
            Some(value)
        }
    }

    pub fn array(&self) -> Arc<dyn Array> {
        match self {
            Value::Boolean(value) => Arc::new(BooleanArray::from(vec![*value])),
            Value::Int64(value) => Arc::new(Int64Array::from(vec![*value])),
            Value::Float64(value) => Arc::new(Float64Array::from(vec![*value])),
            Value::Utf8(value) => Arc::new(StringArray::from(vec![value.as_str()])),
            Value::Timestamp(value) => Arc::new(TimestampMicrosecondArray::from(vec![*value])),
            Value::Date(value) => Arc::new(Date32Array::from(vec![*value])),
        }
    }

    pub fn data_type(&self) -> &DataType {
        match self {
            Value::Boolean(_) => &DataType::Boolean,
            Value::Int64(_) => &DataType::Int64,
            Value::Float64(_) => &DataType::Float64,
            Value::Utf8(_) => &DataType::Utf8,
            Value::Timestamp(_) => &DataType::Timestamp(TimeUnit::Microsecond, None),
            Value::Date(_) => &DataType::Date32(DateUnit::Day),
        }
    }

    pub fn bool(&self) -> Option<bool> {
        if let Value::Boolean(value) = self {
            Some(*value)
        } else {
            None
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Boolean(value) => write!(f, "{}", value),
            Value::Int64(value) => write!(f, "{}", value),
            Value::Float64(value) => write!(f, "{}", value),
            Value::Utf8(value) => write!(f, "{:?}", value),
            Value::Timestamp(value) => write!(f, "{}", timestamp_value(*value)),
            Value::Date(value) => write!(f, "{}", date_value(*value)),
        }
    }
}

impl Eq for Value {}
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Boolean(left), Value::Boolean(right)) => *left == *right,
            (Value::Int64(left), Value::Int64(right)) => *left == *right,
            (Value::Float64(left), Value::Float64(right)) => *left == *right,
            (Value::Utf8(left), Value::Utf8(right)) => *left == *right,
            (Value::Timestamp(left), Value::Timestamp(right)) => *left == *right,
            (Value::Date(left), Value::Date(right)) => *left == *right,
            (_, _) => false,
        }
    }
}
impl hash::Hash for Value {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Boolean(value) => value.hash(state),
            Value::Int64(value) => value.hash(state),
            Value::Float64(value) => value.to_ne_bytes().hash(state),
            Value::Utf8(value) => value.hash(state),
            Value::Timestamp(value) => value.hash(state),
            Value::Date(value) => value.hash(state),
        }
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
