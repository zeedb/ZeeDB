use crate::parser::*;

#[test]
fn test_format() {
    assert_eq!("SELECT\n  1;", format("select 1"));
}

#[test]
fn test_split() {
    assert_eq!(vec!["select 1;", " select 2"], split("select 1; select 2"));
    assert_eq!(
        vec!["select 1;", " select 2;"],
        split("select 1; select 2;")
    );
    assert_eq!(
        vec!["select 1;", " select 2; "],
        split("select 1; select 2; ")
    );
}
