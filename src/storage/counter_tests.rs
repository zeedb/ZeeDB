use crate::counter::*;
use arrow::array::{Array, Int64Array};
use std::sync::Arc;

#[test]
fn test_count() {
    let mut counter = Counter::new();
    let column: Arc<dyn Array> = Arc::new(Int64Array::from(vec![10i64].repeat(1000)));
    counter.insert(&column);
    assert!(0f64 < counter.count());
    assert!(counter.count() < 2f64);
}
