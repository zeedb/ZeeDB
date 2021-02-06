use kernel::*;

use crate::ColumnStatistics;

#[test]
fn test_count_distinct() {
    let mut stats = ColumnStatistics::new(DataType::I64);
    let column: AnyArray = AnyArray::I64(I64Array::from_values(vec![10i64].repeat(1000)));
    stats.insert(&column);
    assert!(0f64 < stats.count_distinct());
    assert!(stats.count_distinct() < 2f64);
}
