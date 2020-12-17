use crate::gather::*;
use arrow::array::*;

#[test]
fn test_gather_boolean() {
    let found = gather(
        &BooleanArray::from(vec![true, false, false, false, true]),
        &UInt32Array::from(vec![0, 2, 4]),
    );
    assert_eq!(
        (0..found.len()).map(|i| found.value(i)).collect::<Vec<_>>(),
        vec![true, false, true]
    );
}

#[test]
fn test_gather_int() {
    let found = gather(
        &Int64Array::from(vec![1, 2, 3, 4, 5]),
        &UInt32Array::from(vec![0, 2, 4]),
    );
    assert_eq!(
        (0..found.len()).map(|i| found.value(i)).collect::<Vec<_>>(),
        vec![1, 3, 5]
    );
}

#[test]
fn test_gather_string() {
    let found = gather(
        &StringArray::from(vec!["1", "2", "3", "4", "5"]),
        &UInt32Array::from(vec![0, 2, 4]),
    );
    assert_eq!(
        (0..found.len()).map(|i| found.value(i)).collect::<Vec<_>>(),
        vec!["1", "3", "5"]
    );
}

#[test]
fn test_gather_logical_boolean() {
    let found = gather_logical(
        &BooleanArray::from(vec![true, false, false, false, true]),
        &BooleanArray::from(vec![true, false, true, false, true]),
    );
    assert_eq!(
        (0..found.len()).map(|i| found.value(i)).collect::<Vec<_>>(),
        vec![true, false, true]
    );
}

#[test]
fn test_gather_logical_int() {
    let found = gather_logical(
        &Int64Array::from(vec![1, 2, 3, 4, 5]),
        &BooleanArray::from(vec![true, false, true, false, true]),
    );
    assert_eq!(
        (0..found.len()).map(|i| found.value(i)).collect::<Vec<_>>(),
        vec![1, 3, 5]
    );
}

#[test]
fn test_gather_logical_string() {
    let found = gather_logical(
        &StringArray::from(vec!["1", "2", "3", "4", "5"]),
        &BooleanArray::from(vec![true, false, true, false, true]),
    );
    assert_eq!(
        (0..found.len()).map(|i| found.value(i)).collect::<Vec<_>>(),
        vec!["1", "3", "5"]
    );
}
