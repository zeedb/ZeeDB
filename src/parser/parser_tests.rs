use crate::parser::*;
use ast::*;
use catalog::Catalog;

#[test]
fn test_analyze() {
    let mut parser = ParseProvider::new();
    let expr = parser
        .analyze(&"select 1".to_string(), &Catalog::empty())
        .unwrap();
    if let LogicalOut { input, .. } = &expr {
        if let LogicalMap { .. } = input.as_ref() {
            return;
        }
    }
    panic!("{:?}", &expr);
}

#[test]
fn test_split() {
    let mut parser = ParseProvider::new();
    let sql = "select 1; select 2".to_string();
    parser.analyze(&sql, &Catalog::empty()).unwrap();
}

#[test]
fn test_not_available_fn() {
    let mut parser = ParseProvider::new();
    match parser.analyze(&"select to_proto(true)".to_string(), &Catalog::empty()) {
        Ok(_) => panic!("expected error"),
        Err(_) => (),
    }
}

#[test]
fn test_metadata() {
    let mut parser = ParseProvider::new();
    let q = "
        select * 
        from metadata.column 
        join metadata.table using (table_id) 
        join metadata.catalog using (catalog_id)";
    parser.analyze(&q.to_string(), &Catalog::empty()).unwrap();
}

#[test]
fn test_format() {
    let mut parser = ParseProvider::new();
    let q = "select 1 as foo from bar";
    let format = parser.format(&q.to_string()).unwrap();
    assert_eq!("SELECT\n  1 AS foo\nFROM\n  bar;", format);
}

#[test]
fn test_script() {
    let mut parser = ParseProvider::new();
    let sql = "set x = 1;".to_string();
    parser.analyze(&sql, &Catalog::empty()).unwrap();
}

#[test]
fn test_custom_function() {
    let mut parser = ParseProvider::new();
    let sql = "select metadata.next_val(1);".to_string();
    parser.analyze(&sql, &Catalog::empty()).unwrap();
}

#[test]
fn test_call() {
    let mut parser = ParseProvider::new();
    let sql = "call metadata.create_table(1);".to_string();
    parser.analyze(&sql, &Catalog::empty()).unwrap();
}
