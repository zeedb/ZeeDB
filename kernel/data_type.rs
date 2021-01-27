use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Bool,
    I64,
    F64,
    Date,
    Timestamp,
    String,
}

impl DataType {
    pub fn to_proto(&self) -> zetasql::TypeProto {
        match self {
            DataType::I64 => zetasql::TypeProto {
                type_kind: Some(2),
                ..Default::default()
            },
            DataType::Bool => zetasql::TypeProto {
                type_kind: Some(5),
                ..Default::default()
            },
            DataType::F64 => zetasql::TypeProto {
                type_kind: Some(7),
                ..Default::default()
            },
            DataType::String => zetasql::TypeProto {
                type_kind: Some(8),
                ..Default::default()
            },
            DataType::Date => zetasql::TypeProto {
                type_kind: Some(10),
                ..Default::default()
            },
            DataType::Timestamp => zetasql::TypeProto {
                type_kind: Some(19),
                ..Default::default()
            },
        }
    }
}

impl From<&zetasql::TypeProto> for DataType {
    fn from(column_type: &zetasql::TypeProto) -> Self {
        match column_type.type_kind.unwrap() {
            2 => DataType::I64,
            5 => DataType::Bool,
            7 => DataType::F64,
            8 => DataType::String,
            10 => DataType::Date,
            19 => DataType::Timestamp,
            other => panic!("type {:?} not supported", other),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Bool => write!(f, "BOOL"),
            DataType::I64 => write!(f, "INT64"),
            DataType::F64 => write!(f, "DOUBLE"),
            DataType::Date => write!(f, "DATE"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::String => write!(f, "STRING"),
        }
    }
}

impl From<&str> for DataType {
    fn from(string: &str) -> Self {
        match string {
            "BOOL" => DataType::Bool,
            "INT64" => DataType::I64,
            "DOUBLE" => DataType::F64,
            "DATE" => DataType::Date,
            "TIMESTAMP" => DataType::Timestamp,
            "STRING" => DataType::String,
            other => panic!("{:?}", other),
        }
    }
}
