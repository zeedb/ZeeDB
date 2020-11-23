use crate::catalog::CatalogProvider;
use execute::*;

#[test]
#[ignore]
fn test_catalog() {
    let mut storage = storage::Storage::new();
    let mut catalog = CatalogProvider::new();
    let mut parser = parser::ParseProvider::new();
    let expr = parser
        .analyze(
            &"create table foo (id int64);".to_string(),
            catalog.catalog(&mut storage),
        )
        .unwrap();
    let expr = planner::optimize(expr, &mut parser);
    expr.start(&mut storage).unwrap().next().unwrap();
    let (_, root) = catalog.catalog(&mut storage);
    assert_eq!(format!("{:?}", root), "");
}
