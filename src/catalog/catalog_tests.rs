use crate::catalog::CatalogProvider;

#[test]
fn test_catalog() {
    let mut catalog = CatalogProvider::new();
    let (_, expr) = parser::ParseProvider::new()
        .parse(
            &"create table foo (id int64);".to_string(),
            0,
            catalog.catalog(&"".to_string()),
        )
        .unwrap();
    execute::execute(&expr).unwrap();
    let root = catalog.catalog(&"".to_string());
    assert_eq!(format!("{:?}", root), "");
}
