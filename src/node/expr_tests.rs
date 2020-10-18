use crate::expr::*;
use crate::operator::*;

#[test]
fn test_bottom_up_rewrite() {
    let before = Expr::new(Operator::LogicalFilter(
        vec![],
        Expr::new(Operator::LogicalGetWith("x".to_string(), vec![])),
    ));
    let visitor = |expr: Expr| match expr.as_ref() {
        Operator::LogicalGetWith(_, _) => {
            Expr::new(Operator::LogicalGetWith("y".to_string(), vec![]))
        }
        _ => expr,
    };
    let after = before.bottom_up_rewrite(&visitor);
    let expected = Expr::new(Operator::LogicalFilter(
        vec![],
        Expr::new(Operator::LogicalGetWith("y".to_string(), vec![])),
    ));
    assert_eq!(expected, after);
}
