use crate::hash_table::*;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::sync::Arc;

#[test]
fn test_hash_table() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a#0", DataType::Int64, true),
        Field::new("b#0", DataType::Int64, true),
    ]));
    let a: Vec<i64> = (0..100).collect();
    let b: Vec<i64> = (1000..1100).collect();
    let columns: Vec<Arc<dyn Array>> =
        vec![Arc::new(Int64Array::from(a)), Arc::new(Int64Array::from(b))];
    let input = RecordBatch::try_new(schema, columns).unwrap();
    let partition_by = vec![input.column(0).clone()];
    let table = HashTable::new(&input, &partition_by).unwrap();
    let (left, right_index) = table.probe(&partition_by);
    let right = kernel::gather(&input, &right_index);
    let output = kernel::zip(&left, &right);
    let a1: &Int64Array = as_primitive_array(output.column(0));
    let b1: &Int64Array = as_primitive_array(output.column(1));
    let a2: &Int64Array = as_primitive_array(output.column(2));
    let b2: &Int64Array = as_primitive_array(output.column(3));
    let mut count = 0;
    for i in 0..output.num_rows() {
        if a1.value(i) == a2.value(i) && b1.value(i) == b2.value(i) {
            count += 1;
        }
    }
    assert_eq!(count, 100);
}
