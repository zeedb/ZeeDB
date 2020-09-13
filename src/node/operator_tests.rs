use crate::operator::*;

#[test]
fn test_column_iterator() {
    let a = Column {
        id: 0,
        table: None,
        name: "a".to_string(),
        typ: encoding::Type::Int64,
    };
    let b = Column {
        id: 1,
        table: None,
        name: "b".to_string(),
        typ: encoding::Type::Int64,
    };
    let c = Column {
        id: 2,
        table: None,
        name: "c".to_string(),
        typ: encoding::Type::Int64,
    };
    let x = Scalar::Call(
        Function::Equal,
        vec![
            Scalar::Call(
                Function::Equal,
                vec![Scalar::Column(a.clone()), Scalar::Column(b.clone())],
                encoding::Type::Bool,
            ),
            Scalar::Column(c.clone()),
        ],
        encoding::Type::Bool,
    );
    let expected = vec![c.clone(), b.clone(), a.clone()];
    let found: Vec<Column> = x.columns().map(|c| c.clone()).collect();
    assert_eq!(expected, found)
}
