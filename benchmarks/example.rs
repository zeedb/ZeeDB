use ast::Value;
use criterion::{criterion_group, criterion_main, Criterion};
use e2e_tests::TestRunner;

fn bench(c: &mut Criterion) {
    c.bench_function("insert into test values (i)", |b| {
        let mut t = TestRunner::default();
        t.run("create table test (i int64)", vec![]);
        let mut i = 0;
        let mut timing = vec![];
        b.iter(|| {
            timing.push(t.run(
                "insert into test values (@i)",
                vec![("i".to_string(), Value::I64(Some(i)))],
            ));
            i += 1;
        });
    });

    c.bench_function("insert into table_with_index values (i)", |b| {
        let mut t = TestRunner::default();
        t.run("create table table_with_index (i int64)", vec![]);
        t.run("create index index_i on table_with_index (i)", vec![]);
        let mut i = 0;
        let mut timing = vec![];
        b.iter(|| {
            timing.push(t.run(
                "insert into table_with_index values (@i)",
                vec![("i".to_string(), Value::I64(Some(i)))],
            ));
            i += 1;
        })
    });

    c.bench_function(
        "update table_with_index set indexed_column = i where indexed_column = i - 1",
        |b| {
            let mut t = TestRunner::default();
            t.run(
                "create table table_with_index (indexed_column int64)",
                vec![],
            );
            t.run(
                "create index index_indexed_column on table_with_index (indexed_column)",
                vec![],
            );
            t.run("insert into table_with_index values (0)", vec![]);
            let mut i = 1;
            let mut timing = vec![];
            b.iter(|| {
                timing.push(t.run(
                    "update table_with_index set indexed_column = @i where indexed_column = @i - 1",
                    vec![("i".to_string(), Value::I64(Some(i)))],
                ));
                i += 1;
            })
        },
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench
}
criterion_main!(benches);
