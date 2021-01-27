use crate::parser::*;
use ast::*;

#[test]
fn test_analyze() {
    let expr = analyze(
        catalog::ROOT_CATALOG_ID,
        &catalog::default_catalog(),
        "select 1",
    )
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
    let sql = "select 1; select 2";
    analyze(catalog::ROOT_CATALOG_ID, &catalog::default_catalog(), &sql).unwrap();
}

#[test]
fn test_not_available_fn() {
    match analyze(
        catalog::ROOT_CATALOG_ID,
        &catalog::default_catalog(),
        "select to_proto(true)",
    ) {
        Ok(_) => panic!("expected error"),
        Err(_) => (),
    }
}

#[test]
fn test_metadata() {
    let q = "
        select * 
        from metadata.column 
        join metadata.table using (table_id) 
        join metadata.catalog using (catalog_id)";
    analyze(
        catalog::ROOT_CATALOG_ID,
        &catalog::default_catalog(),
        &q.to_string(),
    )
    .unwrap();
}

#[test]
fn test_format() {
    let q = "select 1 as foo from bar";
    let format = format(&q.to_string()).unwrap();
    assert_eq!("SELECT\n  1 AS foo\nFROM\n  bar;", format);
}

#[test]
fn test_script() {
    let sql = "set x = 1;";
    analyze(catalog::ROOT_CATALOG_ID, &catalog::default_catalog(), &sql).unwrap();
}

#[test]
fn test_custom_function() {
    let sql = "select metadata.next_val(1);";
    analyze(catalog::ROOT_CATALOG_ID, &catalog::default_catalog(), &sql).unwrap();
}

#[test]
fn test_call() {
    let sql = "call metadata.create_table(1);";
    analyze(catalog::ROOT_CATALOG_ID, &catalog::default_catalog(), &sql).unwrap();
}
