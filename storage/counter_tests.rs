use crate::counter::*;
use kernel::*;

#[test]
fn test_count() {
    let mut counter = Counter::new();
    let column: AnyArray = AnyArray::I64(I64Array::from(vec![10i64].repeat(1000)));
    counter.insert(&column);
    assert!(0f64 < counter.count());
    assert!(counter.count() < 2f64);
}
