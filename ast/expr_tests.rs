use crate::expr::*;

#[test]
fn test_bottom_up_rewrite() {
    let before = Expr::LogicalFilter {
        predicates: vec![],
        input: Box::new(Expr::LogicalGetWith {
            name: "x".to_string(),
            columns: vec![],
        }),
    };
    let visitor = |expr: Expr| match expr {
        Expr::LogicalGetWith { .. } => Expr::LogicalGetWith {
            name: "y".to_string(),
            columns: vec![],
        },
        _ => expr,
    };
    let after = before.bottom_up_rewrite(&visitor);
    let expected = Expr::LogicalFilter {
        predicates: vec![],
        input: Box::new(Expr::LogicalGetWith {
            name: "y".to_string(),
            columns: vec![],
        }),
    };
    assert_eq!(expected, after);
}

#[test]
fn test_serde() {
    let before = Expr::LogicalFilter {
        predicates: vec![],
        input: Box::new(Expr::LogicalGetWith {
            name: "x".to_string(),
            columns: vec![],
        }),
    };
    let buf = bincode::serialize(&before).unwrap();
    let after: Expr = bincode::deserialize(&buf).unwrap();
    assert_eq!(before, after);
}
