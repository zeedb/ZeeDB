use crate::heap::*;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::*;
use std::sync::Arc;

#[test]
fn test_insert_delete() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
        Arc::new(Int64Array::from(vec![Some(10), Some(20)])),
    ];
    let mut heap = Heap::empty();
    heap.insert(&RecordBatch::try_new(schema, columns).unwrap(), 1000);
    assert_eq!(
        "a,b,$xmin,$xmax\n1,10,1000,9223372036854775807\n2,20,1000,9223372036854775807\n",
        format!("{:?}", heap)
    );
    for page in heap.scan().iter() {
        page.delete(1, 2000);
    }
    assert_eq!(
        "a,b,$xmin,$xmax\n1,10,1000,9223372036854775807\n2,20,1000,2000\n",
        format!("{:?}", heap)
    );
}

#[test]
fn test_insert_new_page() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let values: Vec<i64> = (0..crate::page::PAGE_SIZE as i64 * 2).collect();
    let column = Arc::new(Int64Array::from(values));
    let mut cluster = Heap::empty();
    cluster.insert(&RecordBatch::try_new(schema, vec![column]).unwrap(), 1000);
}
