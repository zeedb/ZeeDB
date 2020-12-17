use crate::scatter::*;
use arrow::array::*;

#[test]
fn test_scatter_boolean() {
    let into = BooleanArray::from(vec![false].repeat(5));
    let indexes = UInt32Array::from(vec![0, 2, 4]);
    let from = BooleanArray::from(vec![true, true, true]);
    let update = scatter(&into, &indexes, &from);
    let found: Vec<bool> = (0..update.len()).map(|i| update.value(i)).collect();
    assert_eq!(vec![true, false, true, false, true], found);
}

#[test]
fn test_scatter_int() {
    let into = Int64Array::from((0..5).collect::<Vec<i64>>());
    let indexes = UInt32Array::from(vec![0, 2, 4]);
    let from = Int64Array::from((10..13).collect::<Vec<i64>>());
    let update = scatter(&into, &indexes, &from);
    let found = update.value_slice(0, update.len()).to_vec();
    assert_eq!(vec![10, 1, 11, 3, 12], found);
}
