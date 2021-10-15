use sqllogictest::runner::test;

#[test]
fn test_aggregates() {
    rpc::runtime().block_on(test(vec![
        "./tests/duckdb/aggregate/aggregates/test_aggr_string.test",
    ]));
}
