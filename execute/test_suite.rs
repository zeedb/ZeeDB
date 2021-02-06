use std::{collections::HashMap, fmt::Write, sync::Mutex};

use catalog::CATALOG_KEY;
use context::Context;
use parser::{Parser, PARSER_KEY};
use regex::Regex;
use statistics::{Statistics, STATISTICS_KEY};
use storage::{Storage, STORAGE_KEY};

use crate::MetadataCatalog;

pub struct TestSuite {
    txn: i64,
    context: Context,
    log: String,
}

impl TestSuite {
    pub fn new(storage: Storage, statistics: Statistics) -> Self {
        let mut context = Context::default();
        context.insert(STORAGE_KEY, Mutex::new(storage));
        context.insert(STATISTICS_KEY, Mutex::new(statistics));
        context.insert(PARSER_KEY, Parser::default());
        context.insert(CATALOG_KEY, Box::new(MetadataCatalog));
        Self {
            txn: 100,
            context,
            log: "".to_string(),
        }
    }

    pub fn setup(&mut self, sql: &str) {
        run(&sql, self.txn, &mut self.context);
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
            run(&sql, self.txn, &mut self.context)
        )
        .unwrap();
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
    let mut trimmed = trim.replace_all(sql, "").trim().to_string();
    if trimmed.len() > 200 {
        trimmed.truncate(197);
        trimmed.push_str("...");
    }
    trimmed
}

fn run(sql: &str, txn: i64, context: &mut Context) -> String {
    let parser = &context[PARSER_KEY];
    let expr = parser.analyze(sql, catalog::ROOT_CATALOG_ID, txn, vec![], context);
    let expr = planner::optimize(expr, txn, context);
    let batches: Vec<_> = crate::execute::execute(expr, txn, HashMap::new(), context).collect();
    if batches.is_empty() {
        "EMPTY".to_string()
    } else {
        kernel::fixed_width(&batches)
    }
}
