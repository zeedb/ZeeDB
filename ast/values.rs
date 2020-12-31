use chrono::*;
use kernel::*;
use std::fmt;
use std::hash;

#[derive(Debug, Clone)]
pub enum Value {
    Bool(Option<bool>),
    I64(Option<i64>),
    F64(Option<f64>),
    Date(Option<i32>),
    Timestamp(Option<i64>),
    String(Option<String>),
}

impl Value {
    pub fn from(array: &Array) -> Self {
        match array {
            Array::Bool(array) => Value::Bool(array.get(0)),
            Array::I64(array) => Value::I64(array.get(0)),
            Array::F64(array) => Value::F64(array.get(0)),
            Array::Date(array) => Value::Date(array.get(0)),
            Array::Timestamp(array) => Value::Timestamp(array.get(0)),
            Array::String(array) => Value::String(array.get(0).map(|s| s.to_string())),
        }
    }

    pub fn null(data_type: DataType) -> Self {
        match data_type {
            DataType::Bool => Value::Bool(None),
            DataType::I64 => Value::I64(None),
            DataType::F64 => Value::F64(None),
            DataType::Date => Value::Date(None),
            DataType::Timestamp => Value::Timestamp(None),
            DataType::String => Value::String(None),
        }
    }

    pub fn repeat(&self, len: usize) -> Array {
        match self {
            Value::Bool(value) => Array::Bool(BoolArray::from(vec![*value].repeat(len))),
            Value::I64(value) => Array::I64(I64Array::from(vec![*value].repeat(len))),
            Value::F64(value) => Array::F64(F64Array::from(vec![*value].repeat(len))),
            Value::String(value) => Array::String(StringArray::from(
                vec![value.as_ref().map(|s| s.as_str())].repeat(len),
            )),
            Value::Timestamp(value) => {
                Array::Timestamp(TimestampArray::from(vec![*value].repeat(len)))
            }
            Value::Date(value) => Array::Date(DateArray::from(vec![*value].repeat(len))),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Value::Bool(_) => DataType::Bool,
            Value::I64(_) => DataType::I64,
            Value::F64(_) => DataType::F64,
            Value::Date(_) => DataType::Date,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::String(_) => DataType::String,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Bool(value) => {
                if let Some(value) = value {
                    write!(f, "{}", value)
                } else {
                    write!(f, "null")
                }
            }
            Value::I64(value) => {
                if let Some(value) = value {
                    write!(f, "{}", value)
                } else {
                    write!(f, "null")
                }
            }
            Value::F64(value) => {
                if let Some(value) = value {
                    write!(f, "{}", value)
                } else {
                    write!(f, "null")
                }
            }
            Value::String(value) => {
                if let Some(value) = value {
                    write!(f, "{:?}", value)
                } else {
                    write!(f, "null")
                }
            }
            Value::Timestamp(value) => {
                if let Some(value) = value {
                    write!(f, "{}", timestamp_value(*value))
                } else {
                    write!(f, "null")
                }
            }
            Value::Date(value) => {
                if let Some(value) = value {
                    write!(f, "{}", date_value(*value))
                } else {
                    write!(f, "null")
                }
            }
        }
    }
}

impl Eq for Value {}
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // Note this is Rust semantics, not SQL semantics.
            (Value::Bool(left), Value::Bool(right)) => *left == *right,
            (Value::I64(left), Value::I64(right)) => *left == *right,
            (Value::F64(left), Value::F64(right)) => *left == *right,
            (Value::String(left), Value::String(right)) => *left == *right,
            (Value::Timestamp(left), Value::Timestamp(right)) => *left == *right,
            (Value::Date(left), Value::Date(right)) => *left == *right,
            (_, _) => false,
        }
    }
}
impl hash::Hash for Value {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Bool(value) => value.hash(state),
            Value::I64(value) => value.hash(state),
            Value::F64(value) => {
                if let Some(value) = value {
                    value.to_ne_bytes().hash(state)
                }
            }
            Value::String(value) => value.hash(state),
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
