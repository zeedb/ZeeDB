use crate::parser::*;
use ast::*;

#[test]
fn test_analyze() {
    let mut parser = ParseProvider::new();
    let (_, expr) = parser
        .parse(&"select 1".to_string(), 0, empty_catalog())
        .unwrap();
    match expr.as_ref() {
        LogicalMap { .. } => (),
        other => panic!("{:?}", other),
    }
}

fn empty_catalog() -> zetasql::SimpleCatalogProto {
    let mut cat = fixtures::catalog();
    cat.catalog.push(fixtures::bootstrap_metadata_catalog());
    cat
}

#[test]
fn test_split() {
    let mut parser = ParseProvider::new();
    let sql = "select 1; select 2".to_string();
    let (select1, _) = parser.parse(&sql, 0, empty_catalog()).unwrap();
    assert!(select1 > 0);
    let (select2, _) = parser.parse(&sql, select1, empty_catalog()).unwrap();
    assert_eq!(select2 as usize, sql.len());
}

#[test]
fn test_not_available_fn() {
    let mut parser = ParseProvider::new();
    match parser.parse(&"select to_proto(true)".to_string(), 0, empty_catalog()) {
        Ok(_) => panic!("expected error"),
        Err(_) => (),
    }
}

#[test]
fn test_metadata() {
    let mut parser = ParseProvider::new();
    let q = "
        select * 
        from column 
        join table using (table_id) 
        join catalog using (catalog_id)";
    let (offset, _) = parser
        .parse(&q.to_string(), 0, fixtures::bootstrap_metadata_catalog())
        .unwrap();
    assert!(offset > 0);
}

#[test]
fn test_format() {
    let mut parser = ParseProvider::new();
    let q = "select 1 as foo from bar";
    let format = parser.format(&q.to_string()).unwrap();
    assert_eq!("SELECT\n  1 AS foo\nFROM\n  bar;", format);
}
