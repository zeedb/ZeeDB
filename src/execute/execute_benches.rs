#![feature(test)]

extern crate test;

use crate::execute::*;
use catalog::Index;
use regex::Regex;
use std::collections::HashMap;
use storage::Storage;
use zetasql::SimpleCatalogProto;

#[bench]
fn bench_index_lookup(b: &mut test::Bencher) {
    let mut test = BenchProvider::new();

    b.iter(|| test.test("select * from person where person_id = 100"))
}

pub struct BenchProvider {
    storage: Storage,
}

impl BenchProvider {
    pub fn new() -> Self {
        Self {
            storage: crate::adventure_works::adventure_works(),
        }
    }

    pub fn test(&mut self, sql: &str) {
        let trim = Regex::new(r"(?m)^\s+").unwrap();
        let sql = trim.replace_all(sql, "").trim().to_string();
        let catalog = crate::catalog::catalog(&mut self.storage, 100);
        let indexes = crate::catalog::indexes(&mut self.storage, 100);
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
        let program = execute(storage, txn, expr);
        program
    }
}
