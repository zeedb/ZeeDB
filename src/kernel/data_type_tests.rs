use crate::data_type::*;

#[test]
fn test_to_string() {
    assert_eq!("BOOL".to_string(), DataType::Bool.to_string());
    assert_eq!("INT64".to_string(), DataType::I64.to_string());
    assert_eq!("DOUBLE".to_string(), DataType::F64.to_string());
    assert_eq!("STRING".to_string(), DataType::String.to_string());
    assert_eq!("DATE".to_string(), DataType::Date.to_string());
    assert_eq!("TIMESTAMP".to_string(), DataType::Timestamp.to_string());
}

#[test]
fn test_to_from_string() {
    let examples = vec![
        DataType::Bool,
        DataType::I64,
        DataType::F64,
        DataType::String,
        DataType::Date,
        DataType::Timestamp,
    ];
    for data_type in examples {
        assert_eq!(data_type, DataType::from(data_type.to_string().as_str()));
    }
}

#[test]
fn test_to_from_proto() {
    let examples = vec![
        DataType::Bool,
        DataType::I64,
        DataType::F64,
        DataType::String,
        DataType::Date,
        DataType::Timestamp,
    ];
    for data_type in examples {
        let proto: zetasql::TypeProto = data_type.into();
        assert_eq!(data_type, DataType::from(&proto));
    }
}
