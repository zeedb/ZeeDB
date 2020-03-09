use super::Type;

#[test]
fn test_to_string() {
    assert_eq!(String::from("BOOL"), Type::Bool.to_string());
    assert_eq!(String::from("INT64"), Type::Int64.to_string());
    assert_eq!(String::from("DOUBLE"), Type::Double.to_string());
    assert_eq!(String::from("STRING"), Type::String.to_string());
    assert_eq!(String::from("BYTES"), Type::Bytes.to_string());
    assert_eq!(String::from("DATE"), Type::Date.to_string());
    assert_eq!(String::from("TIMESTAMP"), Type::Timestamp.to_string());
    assert_eq!(String::from("NUMERIC"), Type::Numeric.to_string());
    assert_eq!(
        String::from("STRUCT<i INT64, s STRING>"),
        Type::Struct(vec!(
            (String::from("i"), Type::Int64),
            (String::from("s"), Type::String)
        ))
        .to_string()
    );
    assert_eq!(
        String::from("ARRAY<INT64>"),
        Type::Array(Box::from(Type::Int64)).to_string()
    );
}
