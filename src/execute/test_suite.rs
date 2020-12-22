use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
use ast::Expr;
use regex::Regex;
use std::{fmt::Write, sync::Arc};
use storage::Storage;

pub struct TestSuite {
    persistent_storage: Option<Storage>,
    txn: i64,
    log: Vec<TestLog>,
}

enum TestLog {
    Comment(String),
    Preamble(String),
    Plan(String),
    Query(String),
    Error(String),
}

pub struct TestSuiteBuilder {
    suite: TestSuite,
}

impl TestSuite {
    pub fn builder(persistent_storage: Option<Storage>) -> TestSuiteBuilder {
        TestSuiteBuilder::new(persistent_storage)
    }

    pub fn check(mut self, output: &str) {
        let mut result = "".to_string();
        let mut temporary_storage = Storage::new();
        let mut storage = self
            .persistent_storage
            .as_mut()
            .unwrap_or(&mut temporary_storage);
        writeln!(&mut result).unwrap();
        for log in self.log {
            match log {
                TestLog::Comment(comment) => {
                    writeln!(&mut result, "# {}", &comment).unwrap();
                }
                TestLog::Preamble(sql) => {
                    Self::run(&mut storage, &sql, self.txn);
                    writeln!(&mut result, "setup: {}", Self::trim(&sql)).unwrap();
                    self.txn += 1;
                }
                TestLog::Plan(sql) => {
                    writeln!(
                        &mut result,
                        "plan: {}\n{}\n",
                        Self::trim(&sql),
                        Self::plan(&mut storage, &sql, self.txn)
                    )
                    .unwrap();
                    self.txn += 1;
                }
                TestLog::Query(sql) => {
                    writeln!(
                        &mut result,
                        "ok: {}\n{}\n",
                        Self::trim(&sql),
                        Self::run(&mut storage, &sql, self.txn)
                    )
                    .unwrap();
                    self.txn += 1;
                }
                TestLog::Error(sql) => {
                    writeln!(
                        &mut result,
                        "error: {}\n{}\n",
                        Self::trim(&sql),
                        Self::run(&mut storage, &sql, self.txn)
                    )
                    .unwrap_err();
                    self.txn += 1;
                }
            }
        }
        if !test_fixtures::matches_expected(output, result) {
            panic!("{}", output)
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
        let plan = Self::plan(storage, sql, txn);
        let program = crate::execute::compile(plan);
        let results: Result<Vec<_>, _> = program.execute(storage, txn).collect();
        match results {
            Ok(batches) if batches.is_empty() => "EMPTY".to_string(),
            Ok(batches) => Self::tsv(&kernel::cat(&batches)),
            Err(err) => format!("{:?}", err),
        }
    }

    fn tsv(record_batch: &RecordBatch) -> String {
        let columns: Vec<Vec<String>> = (0..record_batch.num_columns())
            .map(|i| Self::tsv_column(record_batch.schema().field(i), record_batch.column(i)))
            .collect();
        let widths: Vec<usize> = columns
            .iter()
            .map(|column| column.iter().map(|value| value.len()).max().unwrap_or(0))
            .collect();
        let padded: Vec<Vec<String>> = (0..columns.len())
            .map(|i| {
                columns[i]
                    .iter()
                    .map(|value| format!("{:1$}", value, widths[i]))
                    .collect()
            })
            .collect();
        let rows: Vec<String> = (0..record_batch.num_rows() + 1)
            .map(|i| {
                (0..record_batch.num_columns())
                    .map(|j| padded[j][i].clone())
                    .collect::<Vec<String>>()
                    .join(" ")
            })
            .collect();
        rows.join("\n")
    }

    fn tsv_column(field: &Field, column: &Arc<dyn Array>) -> Vec<String> {
        let values: Vec<String> = match field.data_type() {
            DataType::Int64 => {
                let column = as_primitive_array::<Int64Type>(column);
                (0..column.len())
                    .map(|i| {
                        if column.is_null(i) {
                            "NULL".to_string()
                        } else {
                            column.value(i).to_string()
                        }
                    })
                    .collect()
            }
            DataType::Boolean => {
                let column = as_primitive_array::<BooleanType>(column);
                (0..column.len())
                    .map(|i| {
                        if column.is_null(i) {
                            "NULL".to_string()
                        } else {
                            column.value(i).to_string()
                        }
                    })
                    .collect()
            }
            DataType::Float64 => {
                let column = as_primitive_array::<Float64Type>(column);
                (0..column.len())
                    .map(|i| {
                        if column.is_null(i) {
                            "NULL".to_string()
                        } else {
                            column.value(i).to_string()
                        }
                    })
                    .collect()
            }
            DataType::Utf8 => {
                let column = as_string_array(column);
                (0..column.len())
                    .map(|i| {
                        if column.is_null(i) {
                            "NULL".to_string()
                        } else {
                            column.value(i).to_string()
                        }
                    })
                    .collect()
            }
            DataType::Date32(DateUnit::Day) => {
                let column = as_primitive_array::<Date32Type>(column);
                (0..column.len())
                    .map(|i| {
                        if column.is_null(i) {
                            "NULL".to_string()
                        } else {
                            column.value_as_date(i).unwrap().format("%F").to_string()
                        }
                    })
                    .collect()
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let column: &TimestampMicrosecondArray =
                    as_primitive_array::<TimestampMicrosecondType>(column);
                (0..column.len())
                    .map(|i| {
                        if column.is_null(i) {
                            "NULL".to_string()
                        } else {
                            column
                                .value_as_datetime(i)
                                .unwrap()
                                .format("%F %T")
                                .to_string()
                        }
                    })
                    .collect()
            }
            other => panic!("{:?} not supported", other),
        };
        let mut header = vec![storage::base_name(field.name()).to_string()];
        header.extend_from_slice(&values);
        header
    }
}

impl TestSuiteBuilder {
    pub fn new(persistent_storage: Option<Storage>) -> Self {
        Self {
            suite: TestSuite {
                persistent_storage,
                txn: 100,
                log: vec![],
            },
        }
    }

    pub fn setup(&mut self, sql: &str) {
        self.suite.log.push(TestLog::Preamble(sql.to_string()));
    }

    pub fn comment(&mut self, comment: &str) {
        self.suite.log.push(TestLog::Comment(comment.to_string()));
    }

    pub fn plan(&mut self, sql: &str) {
        self.suite.log.push(TestLog::Plan(sql.to_string()));
    }

    pub fn ok(&mut self, sql: &str) {
        self.suite.log.push(TestLog::Query(sql.to_string()));
    }

    pub fn error(&mut self, sql: &str) {
        self.suite.log.push(TestLog::Error(sql.to_string()));
    }

    pub fn finish(self, output: &str) {
        self.suite.check(output)
    }
}
