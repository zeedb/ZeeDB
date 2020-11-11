use crate::bytes::*;
use crate::cluster::*;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::sync::Arc;

#[test]
fn test_insert_delete() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
        Arc::new(Int64Array::from(vec![Some(10), Some(20)])),
    ];
    let cluster = Cluster::empty(schema.clone());
    cluster.insert(
        i64_key(1)..=i64_key(10),
        vec![RecordBatch::try_new(schema, columns).unwrap()],
        1000,
    );
    assert_eq!(
        "a,b,$xmin,$xmax\n1,10,1000,18446744073709551615\n2,20,1000,18446744073709551615\n",
        format!("{}", cluster)
    );
    let heap = cluster.update(i64_key(1)..=i64_key(10));
    let pax = &heap[0];
    pax.delete(1, 2000);
    assert_eq!(
        "a,b,$xmin,$xmax\n1,10,1000,18446744073709551615\n2,20,1000,2000\n",
        format!("{}", cluster)
    );
}
