use arrow::array::*;

#[test]
fn test_bit_or() {
    let left = BooleanArray::from(vec![true, false]);
    let right = BooleanArray::from(vec![false, true]);
    let or = crate::bit_or(&left, &right);
    assert_eq!(or.value(0), true);
    assert_eq!(or.value(1), true);
}
