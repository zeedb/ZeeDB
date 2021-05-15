use criterion::{criterion_group, criterion_main, Criterion};
use e2e_tests::TestRunner;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("insert into test values (1)", |b| {
        let mut t = TestRunner::default();
        t.run("create table test (i int64)");

        b.iter(|| {
            let _ = t.run(&format!("insert into test values (1)",));
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

// cargo install flamegraph
// sudo cargo flamegraph -p benchmarks --bench example -o benchmarks/profiles/example.svg
