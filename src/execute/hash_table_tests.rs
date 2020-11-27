use crate::hash_table::*;
use crate::state::State;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use ast::*;
use std::sync::Arc;
use storage::Storage;

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
    let scalars = vec![Scalar::Column(Column {
        created: Phase::Plan,
        id: 0,
        name: "a".to_string(),
        table: None,
        data: DataType::Int64,
    })];
    let input = RecordBatch::try_new(schema, columns).unwrap();
    let mut storage = Storage::new();
    let mut state = State::new(&mut storage);
    let table = HashTable::new(&scalars, &mut state, &input).unwrap();
    let buckets = table.hash_buckets(&scalars, &mut state, &input).unwrap();
    let output = table.bucket_cross_product(&input, &buckets);
    let a1: &Int64Array = kernel::coerce(output.column(0));
    let b1: &Int64Array = kernel::coerce(output.column(1));
    let a2: &Int64Array = kernel::coerce(output.column(2));
    let b2: &Int64Array = kernel::coerce(output.column(3));
    let mut count = 0;
    for i in 0..output.num_rows() {
        if a1.value(i) == a2.value(i) && b1.value(i) == b2.value(i) {
            count += 1;
        }
    }
    assert_eq!(count, 100);
}
