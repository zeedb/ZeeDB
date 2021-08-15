// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use sqllogictest::runner::{run_path, run_string, RunConfig};

#[test]
fn test_run_string() {
    rpc::runtime().block_on(async {
        let config = RunConfig {
            verbosity: 1,
            workers: 1,
            no_fail: false,
        };
        let input = "statement ok
CREATE TABLE t1(a INT64, b INT64, c INT64, d INT64, e INT64)

statement ok
INSERT INTO t1(e,c,b,d,a) VALUES(NULL,102,NULL,101,104)

query IIIII
SELECT * FROM t1
----
104
NULL
102
101
NULL
";
        let outcomes = run_string(&config, "<test>", input).await.unwrap();
        assert!(!outcomes.any_failed());
    });
}

#[test]
fn test_run_path() {
    rpc::runtime().block_on(async {
        let config = RunConfig {
            verbosity: 1,
            workers: 1,
            no_fail: false,
        };
        let outcomes = run_path(&config, &Path::new("./tests/example_path"))
            .await
            .unwrap();
        assert!(!outcomes.any_failed());
    });
}
