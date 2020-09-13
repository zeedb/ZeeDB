use crate::plan::*;

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

#[test]
fn test_bottom_up_rewrite() {
    let before = Expr(Operator::LogicalFilter(
        vec![],
        Box::new(Expr(Operator::LogicalGetWith("x".to_string()))),
    ));
    let visitor = |expr: Expr| match expr {
        Expr(Operator::LogicalGetWith(_)) => Expr(Operator::LogicalGetWith("y".to_string())),
        _ => expr,
    };
    let after = before.bottom_up_rewrite(&visitor);
    let expected = Expr(Operator::LogicalFilter(
        vec![],
        Box::new(Expr(Operator::LogicalGetWith("y".to_string()))),
    ));
    assert_eq!(expected, after);
}
