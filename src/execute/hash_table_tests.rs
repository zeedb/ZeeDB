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
        Field::new("a#0", DataType::Int64, false),
        Field::new("b#0", DataType::Int64, false),
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
    let hashes = table.hash(&scalars, &mut state, &input).unwrap();
    for i in 0..hashes.len() {
        let a = i as i64;
        let hash = hashes[i];
        let bucket = table.get(hash as usize);
        let b: &Int64Array = bucket
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let find = 1000 + a;
        assert!(b.value_slice(0, b.len()).contains(&find))
    }
}
