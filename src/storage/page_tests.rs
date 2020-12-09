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
    let page = Page::empty(0, schema.clone());
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
    let mut tids = Int64Builder::new(1);
    page.insert(&input, 1000, &mut tids, &mut offset);
    assert_eq!(1, offset);
    assert_eq!(
        "boolean,int64,float64,date32,timestamp,$xmin,$xmax\ntrue,1,1.1,1970-01-02,1970-01-01T00:00:00.000001000,1000,9223372036854775807\n",
        format!("{:?}", page)
    );
    let mut offset = 0;
    let mut tids = Int64Builder::new(1);
    page.insert(&input, 2000, &mut tids, &mut offset);
    assert_eq!(1, offset);
    assert_eq!(
        "boolean,int64,float64,date32,timestamp,$xmin,$xmax\ntrue,1,1.1,1970-01-02,1970-01-01T00:00:00.000001000,1000,9223372036854775807\ntrue,1,1.1,1970-01-02,1970-01-01T00:00:00.000001000,2000,9223372036854775807\n",
        format!("{:?}", page)
    );
}

#[test]
fn test_var_types() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("int64", DataType::Int64, true),
        Field::new("string", DataType::Utf8, true),
    ]));
    let page = Page::empty(0, schema.clone());
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1i64), Some(2i64)])),
            Arc::new(StringArray::from(vec![Some("foo"), Some("bar")])),
        ],
    )
    .unwrap();
    let mut offset = 0;
    let mut tids = Int64Builder::new(1);
    page.insert(&input, 1000, &mut tids, &mut offset);
    assert_eq!(2, offset);
    assert_eq!(
        "int64,string,$xmin,$xmax\n1,foo,1000,9223372036854775807\n2,bar,1000,9223372036854775807\n",
        format!("{:?}", page)
    );
    let mut offset = 0;
    let mut tids = Int64Builder::new(1);
    page.insert(&input, 2000, &mut tids, &mut offset);
    assert_eq!(2, offset);
    assert_eq!(
        "int64,string,$xmin,$xmax\n1,foo,1000,9223372036854775807\n2,bar,1000,9223372036854775807\n1,foo,2000,9223372036854775807\n2,bar,2000,9223372036854775807\n",
        format!("{:?}", page)
    );
}

#[test]
fn test_insert_delete() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let page = Page::empty(0, schema.clone());
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1i64), Some(2i64)])),
            Arc::new(Int64Array::from(vec![Some(10i64), Some(20i64)])),
        ],
    )
    .unwrap();
    let mut offset = 0;
    let mut tids = Int64Builder::new(1);
    page.insert(&input, 1000, &mut tids, &mut offset);
    assert_eq!(2, offset);
    assert!(page.delete(1, 2000));
    assert!(!page.delete(1, 2001));
    assert_eq!(
        "a,b,$xmin,$xmax\n1,10,1000,9223372036854775807\n2,20,1000,2000\n",
        format!("{:?}", page)
    );
}

#[test]
fn test_expand_string_pool() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
    let page = Page::empty(0, schema.clone());
    let strings: Vec<String> = (0..PAGE_SIZE)
        .map(|i| format!("1234567890-{}", i))
        .collect();
    let total: usize = strings.iter().map(|s| s.len()).sum();
    assert!(total > INITIAL_STRING_CAPACITY);
    let strs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
    let input =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(strs))]).unwrap();
    let mut offset = 0;
    let mut tids = Int64Builder::new(1);
    page.insert(&input, 1000, &mut tids, &mut offset);
    let batch = page.select(&page.star());
    let column: &StringArray = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..PAGE_SIZE {
        assert_eq!(&strings[i], column.value(i));
    }
}

#[test]
fn test_base_name() {
    assert_eq!("foo", base_name("foo"));
    assert_eq!("foo", base_name("foo#1"));
    assert_eq!("foo", base_name("foo#123"));
}
