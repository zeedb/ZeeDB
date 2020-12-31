use crate::{bool_array::*, primitive_array::*, string_array::*};

#[test]
fn test_conflict() {
    let indexes = I32Array::from(vec![10, 11, 12, 10]);
    let mask = BoolArray::from(vec![true, true, true, false]);
    assert!(!indexes.conflict(&mask, 13));
    let mask = BoolArray::from(vec![true, true, true, true]);
    assert!(indexes.conflict(&mask, 13));
}
