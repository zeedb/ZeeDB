use ast::Value;

use crate::test_runner::TestRunner;

#[test]
fn test_insert() {
    println!("[");
    let mut t = TestRunner::default();
    t.run("create table test (i int64)", vec![]);
    assert_eq!(
        "EMPTY",
        t.run(
            "insert into test values (@i)",
            vec![("i".to_string(), Value::I64(Some(0)))],
        )
    );
}

#[test]
fn test_aggregate() {
    assert!(!TestRunner::default().rewrite("tests/aggregate.testlog"));
}

#[test]
fn test_complex_correlated_subquery() {
    assert!(!TestRunner::default().rewrite("tests/complex_correlated_subquery.testlog"));
}

#[test]
fn test_correlated_exists() {
    assert!(!TestRunner::default().rewrite("tests/correlated_exists.testlog"));
}

#[test]
fn test_correlated_subquery() {
    assert!(!TestRunner::default().rewrite("tests/correlated_subquery.testlog"));
}

#[test]
fn test_ddl() {
    assert!(!TestRunner::default().rewrite("tests/ddl.testlog"));
}

#[test]
fn test_dml() {
    assert!(!TestRunner::default().rewrite("tests/dml.testlog"));
}

#[test]
fn test_explain() {
    assert!(!TestRunner::default().rewrite("tests/explain.testlog"));
}

#[test]
fn test_join_nested_loop() {
    assert!(!TestRunner::default().rewrite("tests/join_nested_loop.testlog"));
}

#[test]
fn test_join_using() {
    assert!(!TestRunner::default().rewrite("tests/join_using.testlog"));
}

#[test]
fn test_limit() {
    assert!(!TestRunner::default().rewrite("tests/limit.testlog"));
}

#[test]
fn test_literals() {
    assert!(!TestRunner::default().rewrite("tests/literals.testlog"));
}

#[test]
fn test_scalar_expressions() {
    assert!(!TestRunner::default().rewrite("tests/scalar_expressions.testlog"));
}

#[test]
fn test_set_operations() {
    assert!(!TestRunner::default().rewrite("tests/set_operations.testlog"));
}

#[test]
fn test_subquery_join() {
    assert!(!TestRunner::default().rewrite("tests/subquery_join.testlog"));
}

#[test]
fn test_update_index() {
    assert!(!TestRunner::default().rewrite("tests/update_index.testlog"));
}

#[test]
fn test_with() {
    assert!(!TestRunner::default().rewrite("tests/with.testlog"));
}

#[test]
fn test_variables() {
    assert_eq!(
        "$col1\n1    ".to_string(),
        TestRunner::default().run(
            "select @var",
            vec![("var".to_string(), Value::I64(Some(1)))]
        )
    );
}
