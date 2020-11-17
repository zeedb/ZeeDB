use crate::byte_key::*;
use crate::heap::*;
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
    let cluster = Heap::empty(schema.clone());
    cluster.insert(&RecordBatch::try_new(schema, columns).unwrap(), 1000);
    assert_eq!(
        "a,b,$xmin,$xmax\n1,10,1000,18446744073709551615\n2,20,1000,18446744073709551615\n",
        format!("{}", cluster)
    );
    for page in cluster.scan(ByteKey::key(1)..=ByteKey::key(10)).iter() {
        page.delete(1, 2000);
    }
    assert_eq!(
        "a,b,$xmin,$xmax\n1,10,1000,18446744073709551615\n2,20,1000,2000\n",
        format!("{}", cluster)
    );
}

#[test]
fn test_insert_new_page() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let values: Vec<i64> = (0..crate::page::PAGE_SIZE as i64 * 2).collect();
    let column = Arc::new(Int64Array::from(values));
    let cluster = Heap::empty(schema.clone());
    cluster.insert(&RecordBatch::try_new(schema, vec![column]).unwrap(), 1000);
}
