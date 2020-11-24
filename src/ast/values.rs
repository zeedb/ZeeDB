use chrono::*;
use std::fmt;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Value {
    Int64(i64),
    Bool(bool),
    Double(String),
    String(String),
    Date(i32),
    Timestamp(i64),
    Numeric(i128),
    Array(Vec<Value>),
    Struct(Vec<Value>),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int64(x) => write!(f, "{}", x),
            Value::Bool(x) => write!(f, "{}", x),
            Value::Double(x) => write!(f, "{}", x),
            Value::String(x) => write!(f, "{}", x),
            Value::Date(x) => write!(f, "{}", date_value(*x)),
            Value::Timestamp(x) => write!(f, "{}", timestamp_value(*x)),
            Value::Numeric(x) => write!(f, "{}", x),
            Value::Array(x) => write!(f, "{:?}", x),
            Value::Struct(x) => write!(f, "{:?}", x),
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
