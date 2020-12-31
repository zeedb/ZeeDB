use catalog::Index;
use cpuprofiler::PROFILER;
use execute::Program;
use std::collections::HashMap;
use storage::Storage;
use zetasql::SimpleCatalogProto;

/// View profiles with ~/go/bin/pprof --http localhost:8888 src/benchmarks/profiles/___.profile
fn main() {
    bench(
        "src/benchmarks/profiles/index-lookup.profile",
        "select * from person where person_id = 100",
    );
}

fn bench(profile: &str, q: &str) {
    let mut storage = execute::adventure_works();
    let catalog = execute::catalog(&mut storage, 100);
    let indexes = execute::indexes(&mut storage, 100);
    let program = compile(&mut storage, &catalog, &indexes, &q);
    let mut profiler = PROFILER.lock().unwrap();
    profiler.start(profile).unwrap();
    for txn in 0..1000 {
        program.execute(&mut storage, 100 + txn).last();
    }
    profiler.stop().unwrap();
}

fn compile<'a>(
    storage: &'a mut Storage,
    catalog: &SimpleCatalogProto,
    indexes: &HashMap<i64, Vec<Index>>,
    sql: &str,
) -> Program {
    let expr = parser::analyze(catalog::ROOT_CATALOG_ID, catalog, sql).expect(sql);
    let expr = planner::optimize(catalog::ROOT_CATALOG_ID, catalog, indexes, storage, expr);
    let program = execute::compile(expr);
    program
}
