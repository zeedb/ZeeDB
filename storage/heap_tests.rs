use crate::heap::*;
use kernel::*;

#[test]
fn test_insert_delete() {
    let mut heap = Heap::empty();
    heap.insert(
        &RecordBatch::new(vec![
            (
                "a".to_string(),
                AnyArray::I64(I64Array::from(vec![Some(1), Some(2)])),
            ),
            (
                "b".to_string(),
                AnyArray::I64(I64Array::from(vec![Some(10), Some(20)])),
            ),
        ]),
        1000,
    );
    assert_eq!(
        r#"
a b  $xmin $xmax              
1 10 1000  9223372036854775807
2 20 1000  9223372036854775807
        "#
        .trim(),
        format!("{:?}", heap).trim()
    );
    for mut page in heap.scan() {
        assert!(page.delete(1, 2000));
    }
    assert_eq!(
        r#"
a b  $xmin $xmax              
1 10 1000  9223372036854775807
2 20 1000  2000
        "#
        .trim(),
        format!("{:?}", heap).trim()
    );
}

#[test]
fn test_insert_new_page() {
    let ints: Vec<_> = (0..crate::page::PAGE_SIZE as i64 * 2).collect();
    let mut heap = Heap::empty();
    heap.insert(
        &RecordBatch::new(vec![("a".to_string(), AnyArray::I64(I64Array::from(ints)))]),
        1000,
    );
}
