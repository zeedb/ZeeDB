use crate::*;
use node::*;
use zetasql::*;

#[test]
fn test_analyze() {
    let mut parser = ParseProvider::new();
    let (_, plan) = parser.parse("select 1", 0, catalog()).unwrap();
    match plan {
        Plan::Logical(Logical::Project(_), _) => (),
        other => panic!("{:?}", other),
    }
}

#[test]
fn test_split() {
    let mut parser = ParseProvider::new();
    let sql = "select 1; select 2";
    let (select1, _) = parser.parse(sql, 0, catalog()).unwrap();
    assert!(select1 > 0);
    let (select2, _) = parser.parse(sql, select1, catalog()).unwrap();
    assert_eq!(select2 as usize, sql.len());
}

#[test]
fn test_not_available_fn() {
    let mut parser = ParseProvider::new();
    match parser.parse("select to_proto(true)", 0, catalog()) {
        Ok(_) => panic!("expected error"),
        Err(_) => (),
    }
}

#[test]
fn test_add_table() {
    let table = SimpleTableProto {
        name: Some(String::from("test_table")),
        serialization_id: Some(1),
        column: vec![SimpleColumnProto {
            name: Some(String::from("test_column")),
            r#type: Some(TypeProto {
                type_kind: Some(TypeKind::TypeInt64 as i32),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut catalog = catalog();
    catalog.table.push(table);
}
