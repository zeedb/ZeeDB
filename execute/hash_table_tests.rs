use kernel::*;

use crate::hash_table::*;

#[test]
fn test_hash_table() {
    let a: Vec<i64> = (0..100).collect();
    let b: Vec<i64> = (1000..1100).collect();
    let columns = vec![
        ("a".to_string(), AnyArray::I64(I64Array::from_values(a))),
        ("b".to_string(), AnyArray::I64(I64Array::from_values(b))),
    ];
    let probe = RecordBatch::new(columns);
    let partition_by = vec![probe.columns[0].1.clone()];
    let table = HashTable::new(&probe, &partition_by);
    let (left_index, right_index) = table.probe(&partition_by);
    let left = table.build().gather(&left_index);
    let right = probe.gather(&right_index);
    let output = RecordBatch::zip(left, right);
    let a1 = match &output.columns[0].1 {
        AnyArray::I64(array) => array,
        _ => panic!(),
    };
    let b1 = match &output.columns[1].1 {
        AnyArray::I64(array) => array,
        _ => panic!(),
    };
    let a2 = match &output.columns[2].1 {
        AnyArray::I64(array) => array,
        _ => panic!(),
    };
    let b2 = match &output.columns[3].1 {
        AnyArray::I64(array) => array,
        _ => panic!(),
    };
    let mut count = 0;
    for i in 0..output.len() {
        if a1.get(i) == a2.get(i) && b1.get(i) == b2.get(i) {
            count += 1;
        }
    }
    assert_eq!(count, 100);
}
