use ast::Expr;
use regex::Regex;
use std::fmt::Write;
use storage::Storage;

pub struct TestSuite {
    storage: Storage,
    txn: i64,
    log: String,
}

impl TestSuite {
    pub fn new(persistent_storage: Option<Storage>) -> Self {
        Self {
            storage: persistent_storage.unwrap_or(Storage::new()),
            txn: 100,
            log: "".to_string(),
        }
    }

    pub fn setup(&mut self, sql: &str) {
        run(&mut self.storage, &sql, self.txn);
        writeln!(&mut self.log, "setup: {}", trim(&sql)).unwrap();
        self.txn += 1;
    }

    pub fn comment(&mut self, comment: &str) {
        writeln!(&mut self.log, "# {}", &comment).unwrap();
    }

    pub fn ok(&mut self, sql: &str) {
        writeln!(
            &mut self.log,
            "ok: {}\n{}\n",
            trim(&sql),
            run(&mut self.storage, &sql, self.txn)
        )
        .unwrap();
        self.txn += 1;
    }

    pub fn error(&mut self, sql: &str) {
        writeln!(
            &mut self.log,
            "error: {}\n{}\n",
            trim(&sql),
            run(&mut self.storage, &sql, self.txn)
        )
        .unwrap_err();
        self.txn += 1;
    }

    pub fn finish(self, output: &str) {
        if !test_fixtures::matches_expected(output, self.log) {
            panic!("{}", output)
        }
    }
}

fn trim(sql: &str) -> String {
    let trim = Regex::new(r"(?m)^\s+").unwrap();
    trim.replace_all(sql, "").trim().to_string()
}

fn plan(storage: &mut Storage, sql: &str, txn: i64) -> Expr {
    let catalog = crate::catalog::catalog(storage, txn);
    let indexes = crate::catalog::indexes(storage, txn);
    let parse = parser::analyze(catalog::ROOT_CATALOG_ID, &catalog, &sql).expect(&sql);
    planner::optimize(catalog::ROOT_CATALOG_ID, &catalog, &indexes, storage, parse)
}

fn run(storage: &mut Storage, sql: &str, txn: i64) -> String {
    let plan = plan(storage, sql, txn);
    let program = crate::execute::compile(plan);
    let batches: Vec<_> = program.execute(storage, txn).collect();
    if batches.is_empty() {
        "EMPTY".to_string()
    } else {
        kernel::fixed_width(&batches)
    }
}
