use sqllogictest::runner::test;

#[test]
fn test_cte() {
    rpc::runtime().block_on(test(vec![
        "./tests/duckdb/cte/test_cte_in_cte.test",
        "./tests/duckdb/cte/test_cte.test",
    ]));
}
