use crate::sketch::*;
use kernel::*;

#[test]
fn test_count_distinct() {
    let mut sketch = Sketch::new(DataType::I64);
    let column: AnyArray = AnyArray::I64(I64Array::from_values(vec![10i64].repeat(1000)));
    sketch.insert(&column);
    assert!(0f64 < sketch.count_distinct());
    assert!(sketch.count_distinct() < 2f64);
}
