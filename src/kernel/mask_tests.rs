use arrow::array::*;

#[test]
fn test_mask() {
    let test = BooleanArray::from(vec![
        Some(true),
        Some(false),
        None,
        Some(true),
        Some(false),
        None,
    ]);
    // Int64
    let if_true = Int64Array::from(vec![Some(1), Some(2), Some(3), None, None, None]);
    let if_false = Int64Array::from(vec![Some(10), Some(20), Some(30), None, None, None]);
    let expect = Int64Array::from(vec![Some(1), Some(20), None, None, None, None]);
    assert_eq!(expect, crate::mask(&test, &if_true, &if_false));
    // String
    let if_true = StringArray::from(vec![Some("1"), Some("2"), Some("3"), None, None, None]);
    let if_false = StringArray::from(vec![Some("10"), Some("20"), Some("30"), None, None, None]);
    let expect = StringArray::from(vec![Some("1"), Some("20"), None, None, None, None]);
    assert_eq!(expect, crate::mask(&test, &if_true, &if_false));
}
