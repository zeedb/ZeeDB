use crate::execute::*;
use arrow::record_batch::RecordBatch;
use catalog::Index;
use regex::Regex;
use std::collections::HashMap;
use storage::Storage;
use test_fixtures::*;
use zetasql::SimpleCatalogProto;

#[test]
fn test_aggregate() {
    let mut test = TestProvider::new(None);
    test.test(
        "examples/execute/aggregate/simple_aggregate.txt",
        vec![
            "create table foo (id int64);",
            "insert into foo values (1), (2), (3)",
            "select sum(id) from foo",
        ],
    );
    // test.test(
    //     "examples/execute/aggregate/group_by.txt",
    //     vec![
    //         "create table foo (id int64);",
    //         "insert into foo values (1), (2), (3), (1)",
    //         "select id, sum(id) from foo",
    //     ],
    // );
    test.finish();
}

#[test]
fn test_ddl() {
    let mut test = TestProvider::new(None);
    test.test(
        "examples/execute/ddl/create_table.txt",
        vec!["create table foo (id int64)"],
    );
    test.test(
        "examples/execute/ddl/drop_table.txt",
        vec![
            "create table foo (id int64);",
            "insert into foo (id) values (1);",
            "drop table foo;",
            "create table foo (id int64);",
            "select * from foo;",
        ],
    );
    test.test(
        "examples/execute/ddl/create_index.txt",
        vec![
            "create table foo (id int64);",
            "create index foo_id on foo (id);",
        ],
    );
    test.test(
        "examples/execute/ddl/drop_index.txt",
        vec![
            "create table foo (id int64);",
            "create index foo_id on foo (id);",
            "drop index foo_id;",
        ],
    );
    test.finish();
}

#[test]
fn test_dml() {
    let mut test = TestProvider::new(None);
    test.test(
        "examples/execute/dml/insert.txt",
        vec![
            "create table foo (id int64);",
            "insert into foo (id) values (1);",
            "select * from foo;",
        ],
    );
    test.test(
        "examples/execute/dml/insert_vary_order.txt",
        vec![
            "create table foo (id int64, ok bool);",
            "insert into foo (id, ok) values (1, false);",
            "insert into foo (ok, id) values (true, 2);",
            "select * from foo;",
        ],
    );
    test.test(
        "examples/execute/dml/delete.txt",
        vec![
            "create table foo (id int64);",
            "insert into foo (id) values (1);",
            "delete from foo where id = 1;",
            "select * from foo;",
        ],
    );
    test.test(
        "examples/execute/dml/delete_then_insert.txt",
        vec![
            "create table foo (id int64);",
            "insert into foo (id) values (1);",
            "delete from foo where id = 1;",
            "insert into foo (id) values (2);",
            "select * from foo;",
        ],
    );
    test.test(
        "examples/execute/dml/update.txt",
        vec![
            "create table foo (id int64);",
            "insert into foo (id) values (1);",
            "update foo set id = 2 where id = 1;",
            "select * from foo;",
        ],
    );
    test.test(
        "examples/execute/dml/insert_into_index.txt",
        vec![
            "create table foo (id int64);",
            "create index foo_id on foo (id);",
            "insert into foo (id) values (1);",
            "select * from foo where id = 1",
        ],
    );
    test.test(
        "examples/execute/dml/update_index.txt",
        vec![
            "create table foo (id int64);",
            "create index foo_id on foo (id);",
            "insert into foo (id) values (1);",
            "update foo set id = 2 where id = 1;",
            "select * from foo where id = 2;",
        ],
    );
    test.finish();
}

#[test]
fn test_index() {
    let mut test = TestProvider::new(Some(crate::adventure_works::adventure_works()));
    test.test(
        "examples/execute/index/index_lookup.txt",
        vec!["select * from customer where customer_id = 1"],
    );
    test.test(
        "examples/execute/index/lookup_join.txt",
        vec!["select * from customer join store using (store_id) where customer_id = 1"],
    );
    test.finish();
}

pub struct TestProvider {
    persistent_storage: Option<Storage>,
    errors: Vec<String>,
}

impl TestProvider {
    pub fn new(persistent_storage: Option<Storage>) -> Self {
        Self {
            persistent_storage,
            errors: vec![],
        }
    }

    pub fn test(&mut self, path: &str, script: Vec<&str>) {
        let trim = Regex::new(r"(?m)^\s+").unwrap();
        let mut output = "".to_string();
        let mut temporary_storage = Storage::new();
        let storage = self
            .persistent_storage
            .as_mut()
            .unwrap_or(&mut temporary_storage);
        for (txn, sql) in script.iter().enumerate() {
            let txn = 100 + txn as u64;
            let sql = trim.replace_all(sql, "").trim().to_string();
            let catalog = crate::catalog::catalog(storage, txn);
            let indexes = crate::catalog::indexes(storage, txn);
            let program = Self::compile(storage, &catalog, &indexes, &sql);
            let execute = program.execute(storage, txn);
            match execute.last() {
                Some(Ok(last)) => output = Self::csv(last),
                Some(Err(err)) => output = format!("Error: {:?}", err),
                None => output = "Empty".to_string(),
            }
        }
        let found = format!("{}\n\n{}", script.join("\n"), output);
        if !matches_expected(&path.to_string(), found) {
            self.errors.push(path.to_string());
        }
    }

    pub fn finish(&mut self) {
        if !self.errors.is_empty() {
            panic!("{:#?}", self.errors);
        }
    }

    fn compile<'a>(
        storage: &'a mut Storage,
        catalog: &SimpleCatalogProto,
        indexes: &HashMap<i64, Vec<Index>>,
        sql: &str,
    ) -> Program {
        let expr = parser::analyze(catalog::ROOT_CATALOG_ID, catalog, sql).expect(sql);
        let expr = planner::optimize(catalog::ROOT_CATALOG_ID, catalog, indexes, storage, expr);
        compile(expr)
    }

    fn csv(record_batch: RecordBatch) -> String {
        let header: Vec<String> = record_batch
            .schema()
            .fields()
            .iter()
            .map(|field| storage::base_name(field.name()).to_string())
            .collect();
        let mut csv_bytes = vec![];
        csv_bytes.extend_from_slice(header.join(",").as_bytes());
        csv_bytes.extend_from_slice("\n".as_bytes());
        arrow::csv::WriterBuilder::new()
            .has_headers(false)
            .build(&mut csv_bytes)
            .write(&record_batch)
            .unwrap();
        String::from_utf8(csv_bytes).unwrap()
    }
}
