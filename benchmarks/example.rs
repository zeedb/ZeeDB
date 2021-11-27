use ast::Value;
use criterion::{criterion_group, criterion_main, Criterion};
use e2e_tests::TestRunner;

fn bench(c: &mut Criterion) {
    bench_query(
        c,
        vec!["create table test (i int64)"],
        "insert into test values (@i)",
    );
    bench_query(
        c,
        vec![
            "create table table_with_index (i int64)",
            "create index index_i on table_with_index (i)",
        ],
        "insert into table_with_index values (@i)",
    );
    bench_query(
        c,
        vec![
            "create table table_with_index (indexed_column int64)",
            "create index index_indexed_column on table_with_index (indexed_column)",
            "insert into table_with_index values (0)",
        ],
        "update table_with_index set indexed_column = @i where indexed_column = @i - 1",
    );
}

fn bench_query(c: &mut Criterion, preamble: Vec<&str>, query: &str) {
    c.bench_function(query, |b| {
        let mut t = TestRunner::default();
        for stmt in &preamble {
            t.test(stmt, vec![]);
        }
        let mut i = 0;
        b.iter(|| {
            t.test(query, vec![("i".to_string(), Value::I64(Some(i)))]);
            i += 1;
        });
        let json = t.bench(query, vec![("i".to_string(), Value::I64(Some(i)))]);
        let path = format!("../target/criterion/{}/trace.json", query);
        let contents = serde_json::to_string(&json).unwrap();
        std::fs::write(path, contents).unwrap();
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20);
    targets = bench
}
criterion_main!(benches);
