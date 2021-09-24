// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    collections::HashMap,
    fmt,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    net::TcpListener,
    ops,
    path::Path,
    str,
    sync::{
        atomic::{AtomicI64, Ordering},
        Mutex,
    },
    time::Duration,
};

use anyhow::{anyhow, bail};
use catalog::{RESERVED_IDS, ROOT_CATALOG_ID};
use chrono::{Date, DateTime, NaiveDate, TimeZone, Utc};
use coordinator::CoordinatorNode;
use kernel::{AnyArray, Array, RecordBatch};
use lazy_static::lazy_static;
use md5::{Digest, Md5};
use once_cell::sync::{Lazy, OnceCell};
use regex::Regex;
use rpc::{
    coordinator_client::CoordinatorClient, coordinator_server::CoordinatorServer,
    worker_server::WorkerServer, CheckRequest, QueryRequest,
};
use tonic::transport::{Channel, Endpoint, Server};
use walkdir::WalkDir;
use worker::WorkerNode;

use crate::{
    ast::{Location, Output, QueryOutput, Record, Sort, Type},
    util,
};

#[derive(Debug)]
pub enum Outcome<'a> {
    Unsupported {
        error: anyhow::Error,
        location: Location,
    },
    ParseFailure {
        error: anyhow::Error,
        location: Location,
    },
    PlanFailure {
        error: anyhow::Error,
        location: Location,
    },
    UnexpectedPlanSuccess {
        expected_error: &'a str,
        location: Location,
    },
    WrongNumberOfRowsInserted {
        expected_count: u64,
        actual_count: u64,
        location: Location,
    },
    WrongColumnCount {
        expected_count: usize,
        actual_count: usize,
        location: Location,
    },
    WrongColumnNames {
        expected_column_names: &'a Vec<String>,
        actual_column_names: Vec<String>,
        location: Location,
    },
    OutputFailure {
        expected_output: &'a Output,
        actual_raw_output: RecordBatch,
        actual_output: Output,
        location: Location,
    },
    Bail {
        cause: Box<Outcome<'a>>,
        location: Location,
    },
    Success,
}

const NUM_OUTCOMES: usize = 10;
const SUCCESS_OUTCOME: usize = NUM_OUTCOMES - 1;

impl<'a> Outcome<'a> {
    fn code(&self) -> usize {
        match self {
            Outcome::Unsupported { .. } => 0,
            Outcome::ParseFailure { .. } => 1,
            Outcome::PlanFailure { .. } => 2,
            Outcome::UnexpectedPlanSuccess { .. } => 3,
            Outcome::WrongNumberOfRowsInserted { .. } => 4,
            Outcome::WrongColumnCount { .. } => 5,
            Outcome::WrongColumnNames { .. } => 6,
            Outcome::OutputFailure { .. } => 7,
            Outcome::Bail { .. } => 8,
            Outcome::Success => 9,
        }
    }

    fn success(&self) -> bool {
        matches!(self, Outcome::Success)
    }
}

impl fmt::Display for Outcome<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Outcome::*;
        const INDENT: &str = "\n        ";
        match self {
            Unsupported { error, location } => write!(f, "Unsupported:{}:\n{:#}", location, error),
            ParseFailure { error, location } => {
                write!(f, "ParseFailure:{}:\n{:#}", location, error)
            }
            PlanFailure { error, location } => write!(f, "PlanFailure:{}:\n{:#}", location, error),
            UnexpectedPlanSuccess {
                expected_error,
                location,
            } => write!(
                f,
                "UnexpectedPlanSuccess:{} expected error: {}",
                location, expected_error
            ),
            WrongNumberOfRowsInserted {
                expected_count,
                actual_count,
                location,
            } => write!(
                f,
                "WrongNumberOfRowsInserted:{}{}expected: {}{}actually: {}",
                location, INDENT, expected_count, INDENT, actual_count
            ),
            WrongColumnCount {
                expected_count,
                actual_count,
                location,
            } => write!(
                f,
                "WrongColumnCount:{}{}expected: {}{}actually: {}",
                location, INDENT, expected_count, INDENT, actual_count
            ),
            WrongColumnNames {
                expected_column_names,
                actual_column_names,
                location,
            } => write!(
                f,
                "Wrong Column Names:{}:{}expected column names: {}{}inferred column names: {}",
                location,
                INDENT,
                expected_column_names
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(" "),
                INDENT,
                actual_column_names
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
            OutputFailure {
                expected_output,
                actual_raw_output,
                actual_output,
                location,
            } => write!(
                f,
                "OutputFailure:{}{}expected: {:?}{}actually: {:?}{}actual raw: {:?}",
                location, INDENT, expected_output, INDENT, actual_output, INDENT, actual_raw_output
            ),
            Bail { cause, location } => write!(f, "Bail:{} {}", location, cause),
            Success => f.write_str("Success"),
        }
    }
}

#[derive(Default, Debug, Eq, PartialEq)]
pub struct Outcomes([usize; NUM_OUTCOMES]);

impl ops::AddAssign<Outcomes> for Outcomes {
    fn add_assign(&mut self, rhs: Outcomes) {
        for (lhs, rhs) in self.0.iter_mut().zip(rhs.0.iter()) {
            *lhs += rhs
        }
    }
}
impl Outcomes {
    pub fn any_failed(&self) -> bool {
        self.0[SUCCESS_OUTCOME] < self.0.iter().sum::<usize>()
    }

    pub fn as_json(&self) -> serde_json::Value {
        serde_json::json!({
            "unsupported": self.0[0],
            "parse_failure": self.0[1],
            "plan_failure": self.0[2],
            "unexpected_plan_success": self.0[3],
            "wrong_number_of_rows_affected": self.0[4],
            "wrong_column_count": self.0[5],
            "wrong_column_names": self.0[6],
            "output_failure": self.0[7],
            "bail": self.0[8],
            "success": self.0[9],
        })
    }

    pub fn display(&self, no_fail: bool) -> OutcomesDisplay<'_> {
        OutcomesDisplay {
            inner: self,
            no_fail,
        }
    }
}

pub struct OutcomesDisplay<'a> {
    inner: &'a Outcomes,
    no_fail: bool,
}

impl<'a> fmt::Display for OutcomesDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let total: usize = self.inner.0.iter().sum();
        write!(
            f,
            "{}:",
            if self.inner.0[SUCCESS_OUTCOME] == total {
                "PASS"
            } else if self.no_fail {
                "FAIL-IGNORE"
            } else {
                "FAIL"
            }
        )?;
        lazy_static! {
            static ref NAMES: Vec<&'static str> = vec![
                "unsupported",
                "parse-failure",
                "plan-failure",
                "unexpected-plan-success",
                "wrong-number-of-rows-inserted",
                "wrong-column-count",
                "wrong-column-names",
                "output-failure",
                "bail",
                "success",
                "total",
            ];
        }
        for (i, n) in self.inner.0.iter().enumerate() {
            if *n > 0 {
                write!(f, " {}={}", NAMES[i], n)?;
            }
        }
        write!(f, " total={}", total)
    }
}

pub(crate) struct Runner {
    catalog_id: i64,
    // Drop order matters for these fields.
    client: CoordinatorClient<Channel>,
}

fn format_datum(column: &AnyArray, typ: &Type, row: usize, col: usize) -> Option<String> {
    let string = match (typ, column) {
        (Type::Integer, AnyArray::I64(i)) => i.get(row)?.to_string(),
        (Type::Integer, AnyArray::F64(f)) => format!("{:.0}", f.get(row)?),
        // This is so wrong, but sqlite needs it.
        (Type::Integer, AnyArray::String(_)) => "0".to_string(),
        (Type::Integer, AnyArray::Bool(b)) => i8::from(b.get(row)?).to_string(),
        (Type::Real, AnyArray::I64(i)) => format!("{:.3}", i.get(row)?),
        (Type::Real, AnyArray::F64(f)) => format!("{:.3}", f.get(row)?),
        (Type::Text, AnyArray::String(s)) => {
            let s = s.get(row)?;
            if s.is_empty() {
                "(empty)".to_string()
            } else {
                s
            }
        }
        (Type::Text, AnyArray::Bool(b)) => b.get(row)?.to_string(),
        (Type::Text, AnyArray::F64(f)) => format!("{:.3}", f.get(row)?),
        (Type::Integer, AnyArray::Date(f)) | (Type::Text, AnyArray::Date(f)) => {
            date(f.get(row)?).format("%F").to_string()
        }
        (Type::Integer, AnyArray::Timestamp(f)) | (Type::Text, AnyArray::Timestamp(f)) => {
            timestamp(f.get(row)?).format("%F %T").to_string()
        }
        (_, d) => panic!(
            "Don't know how to format {:?} as {:?} in column {}",
            d, typ, col,
        ),
    };
    Some(string)
}

fn format_row(rows: &RecordBatch, i: usize, types: &[Type]) -> Vec<String> {
    let mut formatted: Vec<String> = vec![];
    for j in 0..rows.columns.len() {
        let (_, column) = &rows.columns[j];
        let t: Option<String> = format_datum(column, &types[j], i, j);
        formatted.push(match t {
            Some(t) => t,
            None => "NULL".into(),
        });
    }
    formatted
}

impl Runner {
    pub async fn start() -> Self {
        let mut client = connect_to_cluster().await;
        let catalog_id = next_catalog(&mut client).await;
        Runner { catalog_id, client }
    }

    async fn run_record<'a>(
        &mut self,
        record: &'a Record<'a>,
    ) -> Result<Outcome<'a>, anyhow::Error> {
        match &record {
            Record::Statement {
                expected_error,
                rows_affected,
                sql,
                location,
            } => {
                match self
                    .run_statement(*expected_error, *rows_affected, sql, location.clone())
                    .await?
                {
                    Outcome::Success => Ok(Outcome::Success),
                    other => {
                        if expected_error.is_some() {
                            Ok(other)
                        } else {
                            // If we failed to execute a statement that was supposed to succeed,
                            // running the rest of the tests in this file will probably cause
                            // false positives, so just give up on the file entirely.
                            Ok(Outcome::Bail {
                                cause: Box::new(other),
                                location: location.clone(),
                            })
                        }
                    }
                }
            }
            Record::Query {
                sql,
                output,
                location,
            } => self.run_query(sql, output, location.clone()).await,
            _ => Ok(Outcome::Success),
        }
    }

    async fn run_statement<'a>(
        &mut self,
        expected_error: Option<&'a str>,
        expected_rows_affected: Option<u64>,
        sql: &'a str,
        location: Location,
    ) -> Result<Outcome<'a>, anyhow::Error> {
        match self.statement(sql).await {
            Ok(actual) => {
                if let Some(expected_error) = expected_error {
                    return Ok(Outcome::UnexpectedPlanSuccess {
                        expected_error,
                        location,
                    });
                }
                match expected_rows_affected {
                    None => Ok(Outcome::Success),
                    Some(expected) => {
                        if expected != actual {
                            Ok(Outcome::WrongNumberOfRowsInserted {
                                expected_count: expected,
                                actual_count: actual,
                                location,
                            })
                        } else {
                            Ok(Outcome::Success)
                        }
                    }
                }
            }
            Err(error) => {
                if let Some(expected_error) = expected_error {
                    if Regex::new(expected_error)?.is_match(&format!("{:#}", error)) {
                        return Ok(Outcome::Success);
                    }
                }
                Ok(Outcome::PlanFailure {
                    error: anyhow!(error),
                    location,
                })
            }
        }
    }

    async fn run_query<'a>(
        &mut self,
        sql: &'a str,
        output: &'a Result<QueryOutput<'_>, &'a str>,
        location: Location,
    ) -> Result<Outcome<'a>, anyhow::Error> {
        let rows = match self.query(sql).await {
            Ok(rows) => rows,
            Err(error) => {
                return match output {
                    Ok(_) => {
                        let error_string = format!("{}", error);
                        if error_string.contains("supported") || error_string.contains("overload") {
                            // this is a failure, but it's caused by lack of support rather than by bugs
                            Ok(Outcome::Unsupported {
                                error: anyhow!(error),
                                location,
                            })
                        } else {
                            Ok(Outcome::PlanFailure {
                                error: anyhow!(error),
                                location,
                            })
                        }
                    }
                    Err(expected_error) => {
                        if Regex::new(expected_error)?.is_match(&format!("{:#}", error)) {
                            Ok(Outcome::Success)
                        } else {
                            Ok(Outcome::PlanFailure {
                                error: anyhow!(error),
                                location,
                            })
                        }
                    }
                };
            }
        };

        // unpack expected output
        let QueryOutput {
            sort,
            types: expected_types,
            column_names: expected_column_names,
            output: expected_output,
            ..
        } = match output {
            Err(expected_error) => {
                return Ok(Outcome::UnexpectedPlanSuccess {
                    expected_error,
                    location,
                });
            }
            Ok(query_output) => query_output,
        };

        // Various checks as long as there are returned rows.
        if rows.len() > 0 {
            // check column names
            if let Some(expected_column_names) = expected_column_names {
                let actual_column_names =
                    rows.columns.iter().map(|(name, _)| name.clone()).collect();
                if expected_column_names != &actual_column_names {
                    return Ok(Outcome::WrongColumnNames {
                        expected_column_names,
                        actual_column_names,
                        location,
                    });
                }
            }
        }

        if rows.columns.len() != expected_types.len() {
            return Ok(Outcome::WrongColumnCount {
                expected_count: expected_types.len(),
                actual_count: rows.columns.len(),
                location,
            });
        }

        // format output
        let mut formatted_rows = vec![];
        for i in 0..rows.len() {
            let row = format_row(&rows, i, &expected_types);
            formatted_rows.push(row);
        }

        // sort formatted output
        if let Sort::Row = sort {
            formatted_rows.sort();
        }
        let mut values = formatted_rows.into_iter().flatten().collect::<Vec<_>>();
        if let Sort::Value = sort {
            values.sort();
        }

        // check output
        match expected_output {
            Output::Values(expected_values) => {
                if values != *expected_values {
                    return Ok(Outcome::OutputFailure {
                        expected_output,
                        actual_raw_output: rows,
                        actual_output: Output::Values(values),
                        location,
                    });
                }
            }
            Output::Hashed {
                num_values,
                md5: expected_md5,
            } => {
                let mut hasher = Md5::new();
                for value in &values {
                    hasher.update(value);
                    hasher.update("\n");
                }
                let md5 = format!("{:x}", hasher.finalize());
                if values.len() != *num_values || md5 != *expected_md5 {
                    return Ok(Outcome::OutputFailure {
                        expected_output,
                        actual_raw_output: rows,
                        actual_output: Output::Hashed {
                            num_values: values.len(),
                            md5,
                        },
                        location,
                    });
                }
            }
        }

        Ok(Outcome::Success)
    }

    async fn statement(&mut self, sql: &str) -> Result<u64, anyhow::Error> {
        let request = QueryRequest {
            sql: sql.to_string(),
            catalog_id: self.catalog_id,
            txn: None,
            variables: HashMap::default(),
        };
        let _response = self.client.statement(request).await?.into_inner();
        Ok(0) // TODO
    }

    async fn query(&mut self, sql: &str) -> Result<RecordBatch, anyhow::Error> {
        let request = QueryRequest {
            sql: sql.to_string(),
            catalog_id: self.catalog_id,
            txn: None,
            variables: HashMap::default(),
        };
        let response = self.client.query(request).await?.into_inner();
        let record_batch: RecordBatch = bincode::deserialize(&response.record_batch).unwrap();
        Ok(record_batch)
    }
}

pub trait WriteFmt {
    fn write_fmt(&self, fmt: fmt::Arguments<'_>);
}

pub struct RunConfig {
    pub verbosity: usize,
    pub workers: usize,
    pub no_fail: bool,
}

fn print_record(record: &Record) {
    match record {
        Record::Statement { sql, .. } | Record::Query { sql, .. } => {
            println!("{}", crate::util::indent(sql, 4))
        }
        _ => (),
    }
}

pub async fn run_string(
    config: &RunConfig,
    source: &str,
    input: &str,
) -> Result<Outcomes, anyhow::Error> {
    let mut outcomes = Outcomes::default();
    let mut state = Runner::start().await;
    let mut parser = crate::parser::Parser::new(source, input);
    println!("==> {}", source);
    for record in parser.parse_records()? {
        // In maximal-verbosity print the query before attempting to run
        // it. Running the query might panic, so it is important to print out
        // what query we are trying to run *before* we panic.
        if config.verbosity >= 2 {
            print_record(&record);
        }

        let outcome = state
            .run_record(&record)
            .await
            .map_err(|err| format!("In {}:\n{}", source, err))
            .unwrap();

        // Print failures in verbose mode.
        if config.verbosity >= 1 && !outcome.success() {
            if config.verbosity < 2 {
                // If `verbosity >= 2`, we'll already have printed the record,
                // so don't print it again. Yes, this is an ugly bit of logic.
                // Please don't try to consolidate it with the `print_record`
                // call above, as it's important to have a mode in which records
                // are printed before they are run, so that if running the
                // record panics, you can tell which record caused it.
                print_record(&record);
            }
            println!("{}", util::indent(&outcome.to_string(), 4));
            println!("{}", util::indent("----", 4));
        }

        outcomes.0[outcome.code()] += 1;

        if let Outcome::Bail { .. } = outcome {
            break;
        }
    }
    Ok(outcomes)
}

pub async fn run_path(config: &RunConfig, path: &Path) -> Result<Outcomes, anyhow::Error> {
    let mut outcomes = Outcomes::default();
    for entry in WalkDir::new(path) {
        let entry = entry?;
        if entry.file_type().is_file() {
            let o = run_file(&config, entry.path()).await?;
            if o.any_failed() || config.verbosity >= 1 {
                println!(
                    "{}",
                    util::indent(&o.display(config.no_fail).to_string(), 4)
                );
            }
            outcomes += o;
        }
    }
    Ok(outcomes)
}

pub async fn run_file(config: &RunConfig, filename: &Path) -> Result<Outcomes, anyhow::Error> {
    let mut input = String::new();
    File::open(filename)?.read_to_string(&mut input)?;
    run_string(config, &format!("{}", filename.display()), &input).await
}

pub async fn run_stdin(config: &RunConfig) -> Result<Outcomes, anyhow::Error> {
    let mut input = String::new();
    std::io::stdin().lock().read_to_string(&mut input)?;
    run_string(config, "<stdin>", &input).await
}

pub async fn rewrite_file(filename: &Path) -> Result<(), anyhow::Error> {
    let mut file = OpenOptions::new().read(true).write(true).open(filename)?;

    let mut input = String::new();
    file.read_to_string(&mut input)?;

    let mut buf = RewriteBuffer::new(&input);

    let mut state = Runner::start().await;
    let mut parser = crate::parser::Parser::new(filename.to_str().unwrap_or(""), &input);
    println!("==> {}", filename.display());
    for record in parser.parse_records()? {
        let record = record;
        let outcome = state.run_record(&record).await?;

        // If we see an output failure for a query, rewrite the expected output
        // to match the observed output.
        if let (
            Record::Query {
                output:
                    Ok(QueryOutput {
                        output: Output::Values(_),
                        output_str: expected_output,
                        types,
                        ..
                    }),
                ..
            },
            Outcome::OutputFailure {
                actual_output: Output::Values(actual_output),
                ..
            },
        ) = (&record, &outcome)
        {
            // Output everything before this record.
            let offset = expected_output.as_ptr() as usize - input.as_ptr() as usize;
            buf.flush_to(offset);
            buf.skip_to(offset + expected_output.len());

            // Attempt to install the result separator (----), if it does
            // not already exist.
            if buf.peek_last(5) == "\n----" {
                buf.append("\n");
            } else if buf.peek_last(6) != "\n----\n" {
                buf.append("\n----\n");
            }

            for (i, row) in actual_output.chunks(types.len()).enumerate() {
                for (j, col) in row.iter().enumerate() {
                    if i != 0 || j != 0 {
                        buf.append("\n");
                    }
                    buf.append(col);
                }
            }
        } else if let Outcome::Success = outcome {
            // Ok.
        } else {
            bail!("unexpected: {}", outcome);
        }
    }

    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(buf.finish().as_bytes())?;
    file.sync_all()?;
    Ok(())
}

struct RewriteBuffer<'a> {
    input: &'a str,
    input_offset: usize,
    output: String,
}

impl<'a> RewriteBuffer<'a> {
    fn new(input: &'a str) -> RewriteBuffer<'a> {
        RewriteBuffer {
            input,
            input_offset: 0,
            output: String::new(),
        }
    }

    fn flush_to(&mut self, offset: usize) {
        assert!(offset >= self.input_offset);
        let chunk = &self.input[self.input_offset..offset];
        self.output.push_str(chunk);
        self.input_offset = offset;
    }

    fn skip_to(&mut self, offset: usize) {
        assert!(offset >= self.input_offset);
        self.input_offset = offset;
    }

    fn append(&mut self, s: &str) {
        self.output.push_str(s);
    }

    fn peek_last(&self, n: usize) -> &str {
        &self.output[self.output.len() - n..]
    }

    fn finish(mut self) -> String {
        self.flush_to(self.input.len());
        self.output
    }
}

pub async fn connect_to_cluster() -> CoordinatorClient<Channel> {
    static LOCK: OnceCell<Mutex<()>> = OnceCell::new();
    let _lock = LOCK.get_or_init(|| Mutex::default()).lock().unwrap();
    if !std::env::var("COORDINATOR").is_ok() {
        create_cluster().await
    }
    let coordinator = std::env::var("COORDINATOR").unwrap();
    let mut client = CoordinatorClient::new(
        Endpoint::new(coordinator.clone())
            .unwrap()
            .connect_lazy()
            .unwrap(),
    );
    // Check that coordinator is running.
    for _ in 0..10usize {
        match client.check(CheckRequest {}).await {
            Ok(_) => return client,
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
    panic!("Failed to connect to coordinator at {}", coordinator)
}

const N_WORKERS: usize = 2;

async fn create_cluster() {
    let (coordinator_port, worker_ports) = find_cluster_ports();
    set_common_env_variables(coordinator_port, &worker_ports);
    spawn_coordinator(coordinator_port);
    for worker_id in 0..N_WORKERS {
        spawn_worker(worker_id, worker_ports[worker_id]);
    }
}

fn find_cluster_ports() -> (u16, Vec<u16>) {
    let coordinator_port = free_port(MIN_PORT);
    let mut worker_ports: Vec<u16> = vec![];
    let mut worker_port = coordinator_port;
    for _ in 0..N_WORKERS {
        worker_port = free_port(worker_port + 1);
        worker_ports.push(worker_port);
    }
    (coordinator_port, worker_ports)
}

fn set_common_env_variables(coordinator_port: u16, worker_ports: &Vec<u16>) {
    std::env::set_var("WORKER_COUNT", N_WORKERS.to_string());
    std::env::set_var(
        "COORDINATOR",
        format!("http://127.0.0.1:{}", coordinator_port).as_str(),
    );
    for worker_id in 0..N_WORKERS {
        std::env::set_var(
            format!("WORKER_{}", worker_id),
            format!("http://127.0.0.1:{}", worker_ports[worker_id]).as_str(),
        );
    }
}

fn spawn_coordinator(coordinator_port: u16) {
    std::env::set_var("COORDINATOR_PORT", coordinator_port.to_string());
    let coordinator = CoordinatorNode::default();
    let addr = format!("127.0.0.1:{}", coordinator_port).parse().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(CoordinatorServer::new(coordinator))
            .serve(addr)
            .await
            .unwrap()
    });
}

fn spawn_worker(worker_id: usize, worker_port: u16) {
    std::env::set_var("WORKER_ID", worker_id.to_string());
    std::env::set_var("WORKER_PORT", worker_port.to_string());
    let worker = WorkerNode::default();
    let addr = format!("127.0.0.1:{}", worker_port).parse().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerServer::new(worker))
            .serve(addr)
            .await
            .unwrap()
    });
}

const MIN_PORT: u16 = 50100;
const MAX_PORT: u16 = 51100;

fn free_port(min: u16) -> u16 {
    for port in min..MAX_PORT {
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!(
        "Could not find a free port between {} and {}",
        min, MAX_PORT
    )
}

async fn next_catalog(client: &mut CoordinatorClient<Channel>) -> i64 {
    // Find the id of the next database.
    // TODO this is an evil trick that just happens to match, we should query this value from the metadata schema.
    static NEXT_CATALOG: Lazy<AtomicI64> = Lazy::new(|| AtomicI64::new(RESERVED_IDS));
    let catalog_id = NEXT_CATALOG.fetch_add(1, Ordering::Relaxed);
    // Create a new, empty database.
    client
        .query(QueryRequest {
            sql: format!("create database test{}", catalog_id),
            catalog_id: ROOT_CATALOG_ID,
            txn: None,
            variables: HashMap::default(),
        })
        .await
        .unwrap();
    catalog_id
}

fn date(value: i32) -> Date<Utc> {
    let naive = NaiveDate::from_ymd(1970, 1, 1) + chrono::Duration::days(value as i64);
    Utc.from_utc_date(&naive)
}

fn timestamp(value: i64) -> DateTime<Utc> {
    Utc.timestamp(
        value / MICROSECONDS,
        (value % MICROSECONDS * MILLISECONDS) as u32,
    )
}

/// Number of milliseconds in a second
const MILLISECONDS: i64 = 1_000;
/// Number of microseconds in a second
const MICROSECONDS: i64 = 1_000_000;
