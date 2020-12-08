use catalog::Index;
use cpuprofiler::PROFILER;
use execute::Program;
use regex::Regex;
use std::collections::HashMap;
use storage::Storage;
use zetasql::SimpleCatalogProto;

/// View profiles with ~/go/bin/pprof --http localhost:8888 src/benchmarks/profiles/___.profile
fn main() {
    let mut test = BenchProvider::new();
    PROFILER
        .lock()
        .unwrap()
        .start("./src/benchmarks/profiles/index-lookup.profile")
        .unwrap();
    let n = 100;
    for i in 0..n {
        println!("Iteration {}/{}", i, n);
        test.test("select * from person where person_id = 100");
    }
    PROFILER.lock().unwrap().stop().unwrap();
}

pub struct BenchProvider {
    storage: Storage,
}

impl BenchProvider {
    pub fn new() -> Self {
        Self {
            storage: execute::adventure_works(),
        }
    }

    pub fn test(&mut self, sql: &str) {
        let trim = Regex::new(r"(?m)^\s+").unwrap();
        let sql = trim.replace_all(sql, "").trim().to_string();
        let catalog = execute::catalog(&mut self.storage, 100);
        let indexes = execute::indexes(&mut self.storage, 100);
        let program = Self::execute(&mut self.storage, &catalog, &indexes, 100, &sql);
        if let Some(Err(err)) = program.last() {
            panic!(err);
        }
    }

    fn execute<'a>(
        storage: &'a mut Storage,
        catalog: &SimpleCatalogProto,
        indexes: &HashMap<i64, Vec<Index>>,
        txn: u64,
        sql: &str,
    ) -> Program<'a> {
        let expr = parser::analyze(catalog::ROOT_CATALOG_ID, catalog, sql).expect(sql);
        let expr = planner::optimize(catalog::ROOT_CATALOG_ID, catalog, indexes, storage, expr);
        let program = execute::execute(storage, txn, expr);
        program
    }
}
