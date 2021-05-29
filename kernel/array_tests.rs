use crate::{Array, BoolArray, F64Array, I32Array, I64Array, StringArray};

#[test]
fn test_gather_scatter_bool() {
    let from = BoolArray::from_options(vec![Some(true), Some(false), None]);
    let mut into = BoolArray::nulls(10);
    let indexes = I32Array::from_values(vec![1, 4, 9]);
    from.scatter(&indexes, &mut into);
    let gather = into.gather(&indexes);
    assert_eq!(from, gather);
}

#[test]
fn test_gather_scatter_i64() {
    let from = I64Array::from_options(vec![Some(1), Some(2), None]);
    let mut into = I64Array::nulls(10);
    let indexes = I32Array::from_values(vec![1, 4, 9]);
    from.scatter(&indexes, &mut into);
    let gather = into.gather(&indexes);
    assert_eq!(from, gather);
}

#[test]
fn test_gather_scatter_f64() {
    let from = F64Array::from_options(vec![Some(1.0), Some(2.0), None]);
    let mut into = F64Array::nulls(10);
    let indexes = I32Array::from_values(vec![1, 4, 9]);
    from.scatter(&indexes, &mut into);
    let gather = into.gather(&indexes);
    assert_eq!(from, gather);
}

#[test]
fn test_gather_scatter_string() {
    let from = StringArray::from_str_options(vec![Some("true"), Some("false"), None]);
    let mut into = StringArray::nulls(10);
    let indexes = I32Array::from_values(vec![1, 4, 9]);
    from.scatter(&indexes, &mut into);
    let gather = into.gather(&indexes);
    assert_eq!(from, gather);
}

#[test]
fn test_conflict() {
    let indexes = I32Array::from_values(vec![10, 11, 12, 10]);
    let mask = BoolArray::from_values(vec![true, true, true, false]);
    assert!(!indexes.conflict(&mask, 13));
    let mask = BoolArray::from_values(vec![true, true, true, true]);
    assert!(indexes.conflict(&mask, 13));
}
