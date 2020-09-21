use crate::expr::*;
use crate::operator::*;

#[test]
fn test_bottom_up_rewrite() {
    let before = Expr::new(Operator::LogicalFilter(
        vec![],
        Expr::new(Operator::LogicalGetWith("x".to_string())),
    ));
    let visitor = |expr: Expr| match expr.as_ref() {
        Operator::LogicalGetWith(_) => Expr::new(Operator::LogicalGetWith("y".to_string())),
        _ => expr,
    };
    let after = before.bottom_up_rewrite(&visitor);
    let expected = Expr::new(Operator::LogicalFilter(
        vec![],
        Expr::new(Operator::LogicalGetWith("y".to_string())),
    ));
    assert_eq!(expected, after);
}
