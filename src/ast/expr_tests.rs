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
    let mut visitor = |expr: Expr| match expr {
        Expr::LogicalGetWith { .. } => Expr::LogicalGetWith {
            name: "y".to_string(),
            columns: vec![],
        },
        _ => expr,
    };
    let after = before.bottom_up_rewrite(&mut visitor);
    let expected = Expr::LogicalFilter {
        predicates: vec![],
        input: Box::new(Expr::LogicalGetWith {
            name: "y".to_string(),
            columns: vec![],
        }),
    };
    assert_eq!(expected, after);
}
