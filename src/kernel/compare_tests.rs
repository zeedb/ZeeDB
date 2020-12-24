use arrow::array::*;

#[test]
fn test_equal() {
    // Int64
    let found = crate::equal(
        &Int64Array::from(vec![Some(1), Some(0), None]),
        &Int64Array::from(vec![Some(1), Some(1), Some(1)]),
    );
    let found: Vec<_> = (0..found.len())
        .map(|i| {
            if found.is_null(i) {
                None
            } else {
                Some(found.value(i))
            }
        })
        .collect();
    assert_eq!(vec![Some(true), Some(false), None], found);
    // Float64
    let found = crate::equal(
        &Float64Array::from(vec![Some(1.0), Some(0.0), None]),
        &Float64Array::from(vec![Some(1.0), Some(1.0), Some(1.0)]),
    );
    let found: Vec<_> = (0..found.len())
        .map(|i| {
            if found.is_null(i) {
                None
            } else {
                Some(found.value(i))
            }
        })
        .collect();
    assert_eq!(vec![Some(true), Some(false), None], found);
    // String
    let found = crate::equal(
        &StringArray::from(vec![Some("1"), Some("2"), None]),
        &StringArray::from(vec![Some("1"), Some("1"), Some("1")]),
    );
    let found: Vec<_> = (0..found.len())
        .map(|i| {
            if found.is_null(i) {
                None
            } else {
                Some(found.value(i))
            }
        })
        .collect();
    assert_eq!(vec![Some(true), Some(false), None], found);
}

#[test]
fn test_is() {
    // Int64
    let found = crate::is(
        &Int64Array::from(vec![Some(1), Some(0), None]),
        &Int64Array::from(vec![Some(1), Some(1), Some(1)]),
    );
    let found: Vec<_> = (0..found.len())
        .map(|i| {
            if found.is_null(i) {
                None
            } else {
                Some(found.value(i))
            }
        })
        .collect();
    assert_eq!(vec![Some(true), Some(false), Some(false)], found);
    // Float64
    let found = crate::is(
        &Float64Array::from(vec![Some(1.0), Some(0.0), None]),
        &Float64Array::from(vec![Some(1.0), Some(1.0), Some(1.0)]),
    );
    let found: Vec<_> = (0..found.len())
        .map(|i| {
            if found.is_null(i) {
                None
            } else {
                Some(found.value(i))
            }
        })
        .collect();
    assert_eq!(vec![Some(true), Some(false), Some(false)], found);
    // String
    let found = crate::is(
        &StringArray::from(vec![Some("1"), Some("2"), None]),
        &StringArray::from(vec![Some("1"), Some("1"), Some("1")]),
    );
    let found: Vec<_> = (0..found.len())
        .map(|i| {
            if found.is_null(i) {
                None
            } else {
                Some(found.value(i))
            }
        })
        .collect();
    assert_eq!(vec![Some(true), Some(false), Some(false)], found);
}
