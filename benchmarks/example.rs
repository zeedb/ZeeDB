use criterion::{criterion_group, criterion_main, Criterion};
use e2e_tests::TestRunner;
use pprof::criterion::{Output, PProfProfiler};

fn bench(c: &mut Criterion) {
    c.bench_function("insert into test values (i)", |b| {
        let mut t = TestRunner::default();
        t.run("create table test (i int64)");
        let mut i = 0;
        b.iter(|| {
            let _ = t.run(&format!("insert into test values ({})", i));
            i += 1;
        })
    });

    c.bench_function("insert into table_with_index values (i)", |b| {
        let mut t = TestRunner::default();
        t.run("create table table_with_index (i int64)");
        t.run("create index index_i on table_with_index (i)");
        let mut i = 0;
        b.iter(|| {
            let _ = t.run(&format!("insert into table_with_index values ({})", i));
            i += 1;
        })
    });

    c.bench_function(
        "update table_with_index set indexed_column = i where indexed_column = i - 1",
        |b| {
            let mut t = TestRunner::default();
            t.run("create table table_with_index (indexed_column int64)");
            t.run("create index index_indexed_column on table_with_index (indexed_column)");
            t.run("insert into table_with_index values (0)");
            let mut i = 1;
            b.iter(|| {
                let _ = t.run(&format!(
                    "update table_with_index set indexed_column = {} where indexed_column = {}",
                    i,
                    i - 1
                ));
                i += 1;
            })
        },
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(500, Output::Flamegraph(None)));
    targets = bench
}
criterion_main!(benches);

// cargo install flamegraph
// cargo flamegraph -p benchmarks --bench example -o benchmarks/profiles/example.svg
