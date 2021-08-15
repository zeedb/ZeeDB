use std::path::Path;

use sqllogictest::runner::{run_path, RunConfig};

#[test]
fn test_all() {
    rpc::runtime().block_on(async {
        let paths = vec!["./tests/duckdb/types/date/date_parsing.test"];
        let config = RunConfig {
            verbosity: 2,
            workers: 1,
            no_fail: false,
        };
        for path in paths {
            let outcomes = run_path(&config, &Path::new(path)).await.unwrap();
            assert!(!outcomes.any_failed());
        }
    });
}
