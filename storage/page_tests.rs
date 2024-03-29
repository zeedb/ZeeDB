use kernel::*;

use crate::page::*;

#[test]
fn test_fixed_types() {
    let page = Page::empty(
        0,
        vec![
            ("boolean".to_string(), DataType::Bool),
            ("int64".to_string(), DataType::I64),
            ("float64".to_string(), DataType::F64),
            ("date".to_string(), DataType::Date),
            ("timestamp".to_string(), DataType::Timestamp),
        ],
    );
    let input = RecordBatch::new(vec![
        (
            "boolean".to_string(),
            AnyArray::Bool(BoolArray::from_options(vec![Some(true)])),
        ),
        (
            "int64".to_string(),
            AnyArray::I64(I64Array::from_options(vec![Some(1i64)])),
        ),
        (
            "float64".to_string(),
            AnyArray::F64(F64Array::from_options(vec![Some(1.1f64)])),
        ),
        (
            "date".to_string(),
            AnyArray::Date(DateArray::from_options(vec![Some(1i32)])),
        ),
        (
            "timestamp".to_string(),
            AnyArray::Timestamp(TimestampArray::from_options(vec![Some(1i64)])),
        ),
    ]);
    let mut offset = 0;
    let mut tids = I64Array::default();
    page.insert(&input, 1000, &mut tids, &mut offset);
    assert_eq!(1, offset);
    assert_eq!(
        r#"
boolean int64 float64 date       timestamp           $xmin $xmax              
true    1     1.100   1970-01-02 1970-01-01 00:00:00 1000  9223372036854775807
        "#
        .trim(),
        format!("{:?}", page).trim()
    );
    let mut offset = 0;
    let mut tids = I64Array::default();
    page.insert(&input, 2000, &mut tids, &mut offset);
    assert_eq!(1, offset);
    assert_eq!(
        r#"
boolean int64 float64 date       timestamp           $xmin $xmax              
true    1     1.100   1970-01-02 1970-01-01 00:00:00 1000  9223372036854775807
true    1     1.100   1970-01-02 1970-01-01 00:00:00 2000  9223372036854775807
        "#
        .trim(),
        format!("{:?}", page).trim()
    );
}

#[test]
fn test_var_types() {
    let page = Page::empty(
        0,
        vec![
            ("int64".to_string(), DataType::I64),
            ("string".to_string(), DataType::String),
        ],
    );
    let input = RecordBatch::new(vec![
        (
            "int64".to_string(),
            AnyArray::I64(I64Array::from_options(vec![Some(1i64), Some(2i64)])),
        ),
        (
            "string".to_string(),
            AnyArray::String(StringArray::from_str_options(vec![
                Some("foo"),
                Some("bar"),
            ])),
        ),
    ]);
    let mut offset = 0;
    let mut tids = I64Array::default();
    page.insert(&input, 1000, &mut tids, &mut offset);
    assert_eq!(2, offset);
    assert_eq!(
        r#"
int64 string $xmin $xmax              
1     foo    1000  9223372036854775807
2     bar    1000  9223372036854775807
        "#
        .trim(),
        format!("{:?}", page).trim()
    );
    let mut offset = 0;
    let mut tids = I64Array::default();
    page.insert(&input, 2000, &mut tids, &mut offset);
    assert_eq!(2, offset);
    assert_eq!(
        r#"
int64 string $xmin $xmax              
1     foo    1000  9223372036854775807
2     bar    1000  9223372036854775807
1     foo    2000  9223372036854775807
2     bar    2000  9223372036854775807
        "#
        .trim(),
        format!("{:?}", page).trim()
    );
}

#[test]
fn test_insert_delete() {
    let page = Page::empty(
        0,
        vec![
            ("a".to_string(), DataType::I64),
            ("b".to_string(), DataType::I64),
        ],
    );
    let input = RecordBatch::new(vec![
        (
            "a".to_string(),
            AnyArray::I64(I64Array::from_options(vec![Some(1i64), Some(2i64)])),
        ),
        (
            "b".to_string(),
            AnyArray::I64(I64Array::from_options(vec![Some(10i64), Some(20i64)])),
        ),
    ]);
    let mut offset = 0;
    let mut tids = I64Array::default();
    page.insert(&input, 1000, &mut tids, &mut offset);
    assert_eq!(2, offset);
    assert!(page.delete(1, 2000));
    assert!(!page.delete(1, 2001));
    assert_eq!(
        r#"
a b  $xmin $xmax              
1 10 1000  9223372036854775807
2 20 1000  2000               
        "#
        .trim(),
        format!("{:?}", page).trim()
    );
}

#[test]
fn test_expand_string_pool() {
    let page = Page::empty(0, vec![("a".to_string(), DataType::String)]);
    let strings: Vec<String> = (0..PAGE_SIZE)
        .map(|i| format!("1234567890-{}", i))
        .collect();
    let input = RecordBatch::new(vec![(
        "a".to_string(),
        AnyArray::String(StringArray::from_values(strings.clone())),
    )]);
    let mut offset = 0;
    let mut tids = I64Array::default();
    page.insert(&input, 1000, &mut tids, &mut offset);
    let batch = page.select(&page.star());
    if let (_, AnyArray::String(column)) = &batch.columns[0] {
        for i in 0..PAGE_SIZE {
            assert_eq!(Some(strings[i].as_str()), column.get_str(i));
        }
    } else {
        panic!("column 0 is not a string")
    }
}
