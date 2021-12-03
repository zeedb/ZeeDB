use sqllogictest::runner::test;

#[test]
fn test_index() {
    rpc::runtime().block_on(test(vec!["./tests/duckdb/index/test_simple_index.test"]));
}
