use crate::catalog_provider::CatalogProvider;
use crate::execute;
use planner::optimize;
use storage::Storage;

#[test]
fn test_catalog() {
    let mut storage = Storage::new();
    let mut provider = CatalogProvider::new();
    let catalog = provider.catalog(&mut storage);
    let mut parser = parser::ParseProvider::new();
    let expr = parser
        .analyze(&"create table foo (id int64);".to_string(), &catalog)
        .unwrap();
    let expr = optimize(expr, &catalog, &mut parser);
    execute(expr, &mut storage)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let root = provider.catalog(&mut storage);
    assert_eq!(format!("{:?}", root.catalog), "");
}
