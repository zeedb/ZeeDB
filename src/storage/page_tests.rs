use crate::page::*;
use arrow::datatypes::*;
use std::sync::Arc;

#[test]
fn test_fixed_types() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("boolean", DataType::Boolean, false),
        Field::new("int64", DataType::Int64, false),
        Field::new("float64", DataType::Float64, false),
        Field::new("date32", DataType::Date32(DateUnit::Day), false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));
    let pax = Pax::empty(&schema);
    assert!(pax.insert(
        vec![
            Some(Box::new(true)),
            Some(Box::new(1i64)),
            Some(Box::new(1.1f64)),
            Some(Box::new(1i32)),
            Some(Box::new(1i64)),
        ],
        1000
    ));
    assert_eq!(
        "boolean,int64,float64,date32,timestamp,$xmin,$xmax\ntrue,1,1.1,1970-01-02,1970-01-01T00:00:00.000001000,1000,18446744073709551615\n",
        format!("{}", pax)
    );
}

#[test]
fn test_insert_delete() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let pax = Pax::empty(&schema);
    assert!(pax.insert(vec![Some(Box::new(1i64)), Some(Box::new(10i64))], 1000));
    assert!(pax.insert(vec![Some(Box::new(2i64)), Some(Box::new(20i64))], 1001));
    assert!(pax.delete(1, 2000));
    assert!(!pax.delete(1, 2001));
    assert_eq!(
        "a,b,$xmin,$xmax\n1,10,1000,18446744073709551615\n2,20,1001,2000\n",
        format!("{}", pax)
    );
}
