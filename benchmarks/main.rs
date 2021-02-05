use catalog::CATALOG_KEY;
use context::Context;
use cpuprofiler::PROFILER;
use execute::MetadataCatalog;
use parser::{Parser, PARSER_KEY};
use statistics::STATISTICS_KEY;
use std::collections::HashMap;
use storage::STORAGE_KEY;

/// View profiles with ~/go/bin/pprof --http localhost:8888 benchmarks/profiles/___.profile
fn main() {
    bench(
        "benchmarks/profiles/index-lookup.profile",
        "select * from person where person_id = 100",
    );
}

fn bench(profile: &str, sql: &str) {
    let mut context = Context::default();
    let (storage, statistics) = execute::adventure_works();
    context.insert(STORAGE_KEY, storage);
    context.insert(STATISTICS_KEY, statistics);
    context.insert(PARSER_KEY, Parser::default());
    context.insert(CATALOG_KEY, Box::new(MetadataCatalog));
    let expr = context[PARSER_KEY].analyze(sql, catalog::ROOT_CATALOG_ID, 100, vec![], &context);
    let expr = planner::optimize(expr, 100, &context);
    let mut profiler = PROFILER.lock().unwrap();
    profiler.start(profile).unwrap();
    for txn in 0..1000 {
        execute::execute_mut(expr.clone(), 100 + txn, HashMap::new(), &mut context).last();
    }
    profiler.stop().unwrap();
}
