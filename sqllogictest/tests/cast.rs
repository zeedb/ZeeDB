use sqllogictest::runner::test;

#[test]
fn test_cast() {
    rpc::runtime().block_on(test(vec![
        "./tests/duckdb/cast/test_boolean_cast.test",
        "./tests/duckdb/cast/test_exponent_in_cast.test",
        "./tests/duckdb/cast/test_string_cast.test",
    ]));
}
