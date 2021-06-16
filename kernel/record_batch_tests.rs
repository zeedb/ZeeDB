use crate::{fixed_width::fixed_width, Array, I64Array, RecordBatch};

#[test]
fn test_serialize_deserialize() {
    let examples = vec![RecordBatch::new(vec![(
        "a".to_string(),
        I64Array::from_values(vec![]).as_any(),
    )])];
    for batch in examples {
        let circle = bincode::deserialize(&bincode::serialize(&batch).unwrap()).unwrap();
        assert_eq!(fixed_width(&vec![batch]), fixed_width(&vec![circle]));
    }
}
