use crate::aggregate::*;
use arrow::array::*;
use ast::AggregateFn;
use std::sync::*;

#[test]
#[ignore]
fn test_groups() {
    let group_by_columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(Int64Array::from(vec![1, 1, 2, 2, 3])),
        Arc::new(Int64Array::from(vec![10, 10, 20, 21, 30])),
    ];
    let aggregate_columns: Vec<Arc<dyn Array>> =
        vec![Arc::new(Int64Array::from(vec![1, 10, 100, 1000, 10000]))];
    let aggregate_fns = vec![AggregateFn::Sum];
    let operator = HashAggregate::new(group_by_columns, aggregate_fns, aggregate_columns);
    let results = operator.finish();
    assert_eq!("", format!("{:?}", results));
}
