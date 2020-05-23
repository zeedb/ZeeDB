use crate::values::*;

#[test]
fn test_serde() {
    let examples = vec![
        Value::Int64(1),
        Value::Bool(true),
        Value::Double("1.0".to_string()),
        Value::String("foo".to_string()),
        Value::Bytes("foo".as_bytes().to_vec()),
        Value::Date(chrono::NaiveDate::from_ymd(2020, 1, 1)),
        Value::Timestamp(chrono::Utc::now()),
        Value::Numeric(1),
        Value::Array(vec![]),
        Value::Struct(vec![]),
    ];
    for input in examples {
        let encode = bincode::serialize(&input).unwrap();
        let decode: Value = bincode::deserialize(&encode).unwrap();
        assert_eq!(input, decode);
    }
}
