use std::path::Path;

use sqllogictest::runner::{run_path, RunConfig};

#[test]
fn test_types_date() {
    rpc::runtime().block_on(test(vec![
        "./tests/duckdb/types/date/date_limits.test",
        "./tests/duckdb/types/date/date_parsing.test",
        "./tests/duckdb/types/date/test_date.test",
        "./tests/duckdb/types/date/test_incorrect_dates.test",
    ]));
}

#[test]
fn test_types_null() {
    rpc::runtime().block_on(test(vec![
        "./tests/duckdb/types/null/test_boolean_null.test",
        "./tests/duckdb/types/null/test_is_null.test",
        "./tests/duckdb/types/null/test_null_aggr.test",
        "./tests/duckdb/types/null/test_null_cast.test",
        "./tests/duckdb/types/null/test_null.test",
    ]));
}

#[test]
fn test_types_timestamp() {
    rpc::runtime().block_on(test(vec![
        "./tests/duckdb/types/timestamp/test_incorrect_timestamp.test",
        "./tests/duckdb/types/timestamp/test_timestamp_ms.test",
        "./tests/duckdb/types/timestamp/test_timestamp.test",
        "./tests/duckdb/types/timestamp/timestamp_limits.test",
        "./tests/duckdb/types/timestamp/utc_offset.test",
    ]));
}

async fn test(paths: Vec<&'static str>) {
    let config = RunConfig {
        verbosity: 2,
        workers: 1,
        no_fail: false,
    };
    for path in paths {
        let outcomes = run_path(&config, &Path::new(path)).await.unwrap();
        assert!(!outcomes.any_failed());
    }
}
