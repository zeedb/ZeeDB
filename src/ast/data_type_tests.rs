use crate::data_type;
use arrow::datatypes::*;

#[test]
fn test_to_string() {
    assert_eq!("BOOL".to_string(), data_type::to_string(&DataType::Boolean));
    assert_eq!("INT64".to_string(), data_type::to_string(&DataType::Int64));
    assert_eq!(
        "DOUBLE".to_string(),
        data_type::to_string(&DataType::Float64)
    );
    assert_eq!("STRING".to_string(), data_type::to_string(&DataType::Utf8));
    assert_eq!(
        "DATE".to_string(),
        data_type::to_string(&DataType::Date32(DateUnit::Day))
    );
    assert_eq!(
        "TIMESTAMP".to_string(),
        data_type::to_string(&DataType::Timestamp(TimeUnit::Microsecond, None))
    );
    assert_eq!(
        "NUMERIC".to_string(),
        data_type::to_string(&DataType::FixedSizeBinary(16))
    );
    assert_eq!(
        "STRUCT<i INT64, s STRING>".to_string(),
        data_type::to_string(&DataType::Struct(vec![
            Field::new("i", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
        ]))
    );
    assert_eq!(
        "ARRAY<INT64>".to_string(),
        data_type::to_string(&DataType::List(Box::from(DataType::Int64)))
    );
}

#[test]
fn test_to_from_string() {
    let examples = vec![
        DataType::Boolean,
        DataType::Int64,
        DataType::Float64,
        DataType::Utf8,
        DataType::Date32(DateUnit::Day),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::FixedSizeBinary(16),
        // DataType::Struct(vec![
        //     Field::new("i", DataType::Int64, false),
        //     Field::new("s", DataType::Utf8, false),
        // ]),
        // DataType::List(Box::from(DataType::Int64)),
    ];
    for data in examples {
        assert_eq!(
            data_type::to_string(&data),
            data_type::to_string(&data_type::from_string(
                data_type::to_string(&data).as_str()
            ))
        );
    }
}

#[test]
fn test_to_from_proto() {
    let examples = vec![
        DataType::Boolean,
        DataType::Int64,
        DataType::Float64,
        DataType::Utf8,
        DataType::Date32(DateUnit::Day),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::FixedSizeBinary(16),
        // DataType::Struct(vec![
        //     Field::new("i", DataType::Int64, false),
        //     Field::new("s", DataType::Utf8, false),
        // ]),
        // DataType::List(Box::from(DataType::Int64)),
    ];
    for data in examples {
        assert_eq!(
            data_type::to_proto(&data),
            data_type::to_proto(&data_type::from_proto(&data_type::to_proto(&data)))
        );
    }
}
