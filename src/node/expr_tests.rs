use crate::expr::*;
use crate::operator::*;

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
