use crate::optimize::*;
use fixtures::*;

macro_rules! ok {
    ($path:expr, $sql:expr, $errors:expr) => {
        let mut parser = parser::ParseProvider::new();
        let sql = $sql.to_string();
        let (_, expr) = parser.parse(&sql, 0, &adventure_works()).unwrap();
        let expr = optimize(expr);
        let found = format!("{}\n\n{}", &sql, expr);
        if !matches_expected(&$path.to_string(), found) {
            $errors.push($path.to_string());
        }
    };
}

#[test]
#[rustfmt::skip]
fn test_optimize() {
    let mut errors = vec![];
    ok!("examples/optimize/index_scan.txt", r#"select customer_id from customer where store_id = 1"#, errors);
    ok!("examples/optimize/nested_loop.txt", r#"select customer.customer_id, store.store_id from customer, store"#, errors);
    if !errors.is_empty() {
        panic!("{:#?}", errors);
    }
}
