use crate::expr::*;

#[test]
fn test_bottom_up_rewrite() {
    let before = Expr::LogicalFilter(
        vec![],
        Box::new(Expr::LogicalGetWith("x".to_string(), vec![])),
    );
    let visitor = |expr: Expr| match expr {
        Expr::LogicalGetWith(_, _) => (Expr::LogicalGetWith("y".to_string(), vec![])),
        _ => expr,
    };
    let after = before.bottom_up_rewrite(&visitor);
    let expected = Expr::LogicalFilter(
        vec![],
        Box::new(Expr::LogicalGetWith("y".to_string(), vec![])),
    );
    assert_eq!(expected, after);
}
