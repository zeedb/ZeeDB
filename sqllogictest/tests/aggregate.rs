use sqllogictest::runner::test;

#[test]
fn test_aggregates() {
    rpc::runtime().block_on(test(vec![
        "./tests/duckdb/aggregate/aggregates/test_aggr_string.test",
        "./tests/duckdb/aggregate/aggregates/test_aggregate_types_scalar.test",
        "./tests/duckdb/aggregate/aggregates/test_aggregate_types.test",
    ]));
}
