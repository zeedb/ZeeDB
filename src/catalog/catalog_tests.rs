use crate::catalog::CatalogProvider;

#[test]
fn test_catalog() {
    let mut storage = storage::Storage::new();
    let mut catalog = CatalogProvider::new();
    let (_, expr) = parser::ParseProvider::new()
        .parse(
            &"create table foo (id int64);".to_string(),
            0,
            catalog.catalog(&"".to_string(), &mut storage),
        )
        .unwrap();
    execute::execute(&expr, &mut storage).unwrap();
    let root = catalog.catalog(&"".to_string(), &mut storage);
    assert_eq!(format!("{:?}", root), "");
}
