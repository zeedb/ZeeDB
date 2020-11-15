use crate::page::*;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
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
    let pax = Page::empty(schema.clone());
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(BooleanArray::from(vec![Some(true)])),
            Arc::new(Int64Array::from(vec![Some(1i64)])),
            Arc::new(Float64Array::from(vec![Some(1.1f64)])),
            Arc::new(Date32Array::from(vec![Some(1i32)])),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(1i64)])),
        ],
    )
    .unwrap();
    assert_eq!(1, pax.insert(&input, 1000));
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
    let pax = Page::empty(schema.clone());
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1i64), Some(2i64)])),
            Arc::new(Int64Array::from(vec![Some(10i64), Some(20i64)])),
        ],
    )
    .unwrap();
    assert_eq!(2, pax.insert(&input, 1000));
    assert!(pax.delete(1, 2000));
    assert!(!pax.delete(1, 2001));
    assert_eq!(
        "a,b,$xmin,$xmax\n1,10,1000,18446744073709551615\n2,20,1000,2000\n",
        format!("{}", pax)
    );
}
