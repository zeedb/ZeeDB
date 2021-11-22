use sqllogictest::runner::test;

#[test]
fn test_aggregates() {
    rpc::runtime().block_on(test(vec![
        "./tests/duckdb/aggregate/aggregates/test_aggr_string.test",
        "./tests/duckdb/aggregate/aggregates/test_aggregate_types_scalar.test",
        "./tests/duckdb/aggregate/aggregates/test_aggregate_types.test",
        "./tests/duckdb/aggregate/aggregates/test_avg.test",
        "./tests/duckdb/aggregate/aggregates/test_bool.test",
        "./tests/duckdb/aggregate/aggregates/test_count_star.test",
        "./tests/duckdb/aggregate/aggregates/test_count.test",
        "./tests/duckdb/aggregate/aggregates/test_distinct_aggr.test",
        "./tests/duckdb/aggregate/aggregates/test_empty_aggregate.test",
        "./tests/duckdb/aggregate/aggregates/test_first_noninlined.test",
    ]));
}
