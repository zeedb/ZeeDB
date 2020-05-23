use super::Type;

#[test]
fn test_to_string() {
    assert_eq!("BOOL".to_string(), Type::Bool.to_string());
    assert_eq!("INT64".to_string(), Type::Int64.to_string());
    assert_eq!("DOUBLE".to_string(), Type::Double.to_string());
    assert_eq!("STRING".to_string(), Type::String.to_string());
    assert_eq!("BYTES".to_string(), Type::Bytes.to_string());
    assert_eq!("DATE".to_string(), Type::Date.to_string());
    assert_eq!("TIMESTAMP".to_string(), Type::Timestamp.to_string());
    assert_eq!("NUMERIC".to_string(), Type::Numeric.to_string());
    assert_eq!(
        "STRUCT<i INT64, s STRING>".to_string(),
        Type::Struct(vec!(
            ("i".to_string(), Type::Int64),
            ("s".to_string(), Type::String)
        ))
        .to_string()
    );
    assert_eq!(
        "ARRAY<INT64>".to_string(),
        Type::Array(Box::from(Type::Int64)).to_string()
    );
}
