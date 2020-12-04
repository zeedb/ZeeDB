use crate::array::*;
use arrow::array::*;
use std::sync::Arc;

#[test]
fn test_bit_or() {
    let left: Arc<dyn Array> = Arc::new(BooleanArray::from(vec![true, false]));
    let right: Arc<dyn Array> = Arc::new(BooleanArray::from(vec![false, true]));
    let or = bit_or(&left, &right);
    let or: &BooleanArray = or.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(or.value(0), true);
    assert_eq!(or.value(1), true);
}
