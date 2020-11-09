use crate::page::*;
use arrow::datatypes::*;
use std::sync::Arc;

#[test]
fn test_insert() {
    let a = Field::new("a", DataType::Int64, false);
    let b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![a, b]));
    let pax = Pax::empty(&schema);
    assert!(pax.insert(vec![Some(Box::new(1i64)), Some(Box::new(10i64))], 1000));
    assert!(pax.insert(vec![Some(Box::new(2i64)), Some(Box::new(20i64))], 1001));
    assert!(pax.delete(1, 2000));
    assert!(!pax.delete(1, 2001));
    assert_eq!(
        "a,b,$xmin,$xmax\n1,10,1000,18446744073709551615\n2,20,1001,2000\n",
        format!("{}", pax)
    );
}
