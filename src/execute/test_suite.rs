use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
use ast::Expr;
use regex::Regex;
use std::{fmt::Write, sync::Arc};
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

    pub fn plan(&mut self, sql: &str) {
        writeln!(
            &mut self.log,
            "plan: {}\n{}\n",
            trim(&sql),
            plan(&mut self.storage, &sql, self.txn)
        )
        .unwrap();
        self.txn += 1;
    }

    pub fn plan_error(&mut self, sql: &str) {
        // TODO need to actually get the error from the planner here.
        plan(&mut self.storage, &sql, self.txn);
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
    let results: Result<Vec<_>, _> = program.execute(storage, txn).collect();
    match results {
        Ok(batches) if batches.is_empty() => "EMPTY".to_string(),
        Ok(batches) => tsv(&kernel::cat(&batches)),
        Err(err) => format!("{:?}", err),
    }
}

fn tsv(record_batch: &RecordBatch) -> String {
    let columns: Vec<Vec<String>> = (0..record_batch.num_columns())
        .map(|i| tsv_column(record_batch.schema().field(i), record_batch.column(i)))
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
