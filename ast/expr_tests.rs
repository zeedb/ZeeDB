use crate::expr::*;

#[test]
fn test_rewrite() {
    let mut expr = Expr::LogicalFilter {
        predicates: vec![],
        input: Box::new(Expr::LogicalGetWith {
            name: "x".to_string(),
            columns: vec![],
        }),
    };
    fn pre_order(expr: &mut Expr) {
        match expr {
            Expr::LogicalGetWith { name, .. } => {
                *name = "y".to_string();
            }
            _ => {}
        }
        for i in 0..expr.len() {
            pre_order(&mut expr[i])
        }
    }
    pre_order(&mut expr);
    let expected = Expr::LogicalFilter {
        predicates: vec![],
        input: Box::new(Expr::LogicalGetWith {
            name: "y".to_string(),
            columns: vec![],
        }),
    };
    assert_eq!(expected, expr);
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
