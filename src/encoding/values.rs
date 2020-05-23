use serde::de::Visitor;
use serde::{Deserialize, Serialize};
use std::fmt;

// TODO write custom serializer that uses varints
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Value {
    Int64(i64),
    Bool(bool),
    Double(String),
    String(String),
    Bytes(Vec<u8>),
    Date(chrono::NaiveDate),
    Timestamp(chrono::DateTime<chrono::Utc>),
    Numeric(i128),
    Array(Vec<Value>),
    Struct(Vec<Value>),
}

struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = i32;
    fn expecting(&self, _: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        todo!()
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int64(x) => write!(f, "{}", x),
            Value::Bool(x) => write!(f, "{}", x),
            Value::Double(x) => write!(f, "{}", x),
            Value::String(x) => write!(f, "{}", x),
            Value::Bytes(x) => write!(f, "{:?}", x),
            Value::Date(x) => write!(f, "{}", x),
            Value::Timestamp(x) => write!(f, "{}", x),
            Value::Numeric(x) => write!(f, "{}", x),
            Value::Array(x) => write!(f, "{:?}", x),
            Value::Struct(x) => write!(f, "{:?}", x),
        }
    }
}
