use crate::page::*;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::sync::Arc;

#[test]
fn test_fixed_types() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("boolean", DataType::Boolean, true),
        Field::new("int64", DataType::Int64, true),
        Field::new("float64", DataType::Float64, true),
        Field::new("date32", DataType::Date32(DateUnit::Day), true),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));
    let page = Page::empty(schema.clone());
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
    let mut offset = 0;
    let mut pids = UInt64Builder::new(1);
    let mut tids = UInt32Builder::new(1);
    page.insert(&input, 1000, &mut pids, &mut tids, &mut offset);
    assert_eq!(1, offset);
    assert_eq!(
        "boolean,int64,float64,date32,timestamp,$xmin,$xmax\ntrue,1,1.1,1970-01-02,1970-01-01T00:00:00.000001000,1000,18446744073709551615\n",
        format!("{:?}", page)
    );
    let mut offset = 0;
    let mut pids = UInt64Builder::new(1);
    let mut tids = UInt32Builder::new(1);
    page.insert(&input, 2000, &mut pids, &mut tids, &mut offset);
    assert_eq!(1, offset);
    assert_eq!(
        "boolean,int64,float64,date32,timestamp,$xmin,$xmax\ntrue,1,1.1,1970-01-02,1970-01-01T00:00:00.000001000,1000,18446744073709551615\ntrue,1,1.1,1970-01-02,1970-01-01T00:00:00.000001000,2000,18446744073709551615\n",
        format!("{:?}", page)
    );
}

#[test]
fn test_var_types() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("int64", DataType::Int64, true),
        Field::new("string", DataType::Utf8, true),
    ]));
    let page = Page::empty(schema.clone());
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1i64), Some(2i64)])),
            Arc::new(StringArray::from(vec![Some("foo"), Some("bar")])),
        ],
    )
    .unwrap();
    let mut offset = 0;
    let mut pids = UInt64Builder::new(1);
    let mut tids = UInt32Builder::new(1);
    page.insert(&input, 1000, &mut pids, &mut tids, &mut offset);
    assert_eq!(2, offset);
    assert_eq!(
        "int64,string,$xmin,$xmax\n1,foo,1000,18446744073709551615\n2,bar,1000,18446744073709551615\n",
        format!("{:?}", page)
    );
    let mut offset = 0;
    let mut pids = UInt64Builder::new(1);
    let mut tids = UInt32Builder::new(1);
    page.insert(&input, 2000, &mut pids, &mut tids, &mut offset);
    assert_eq!(2, offset);
    assert_eq!(
        "int64,string,$xmin,$xmax\n1,foo,1000,18446744073709551615\n2,bar,1000,18446744073709551615\n1,foo,2000,18446744073709551615\n2,bar,2000,18446744073709551615\n",
        format!("{:?}", page)
    );
}

#[test]
fn test_insert_delete() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let page = Page::empty(schema.clone());
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1i64), Some(2i64)])),
            Arc::new(Int64Array::from(vec![Some(10i64), Some(20i64)])),
        ],
    )
    .unwrap();
    let mut offset = 0;
    let mut pids = UInt64Builder::new(1);
    let mut tids = UInt32Builder::new(1);
    page.insert(&input, 1000, &mut pids, &mut tids, &mut offset);
    assert_eq!(2, offset);
    assert!(page.delete(1, 2000));
    assert!(!page.delete(1, 2001));
    assert_eq!(
        "a,b,$xmin,$xmax\n1,10,1000,18446744073709551615\n2,20,1000,2000\n",
        format!("{:?}", page)
    );
}
