use crate::Parser;

#[test]
fn test_format() {
    assert_eq!("SELECT\n  1;", Parser::default().format("select 1"));
}
