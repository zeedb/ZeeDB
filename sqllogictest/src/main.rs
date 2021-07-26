// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{fs::File, path::PathBuf, process};

use structopt::StructOpt;
use walkdir::WalkDir;

use sqllogictest::{
    runner::{self, Outcomes, RunConfig},
    util,
};

/// Runs sqllogictest scripts to verify database engine correctness.
#[derive(StructOpt)]
struct Args {
    /// Increase verbosity.
    ///
    /// If specified once, print summary for each source file.
    /// If specified twice, also show descriptions of each error.
    /// If specified thrice, also print each query before it is executed.
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    verbosity: usize,
    /// Don't exit with a failing code if not all queries are successful.
    #[structopt(long)]
    no_fail: bool,
    /// Rewrite expected output based on actual output.
    #[structopt(long)]
    rewrite_results: bool,
    /// Save a JSON-formatted summary to FILE.
    #[structopt(long, value_name = "FILE")]
    json_summary_file: Option<PathBuf>,
    /// Run with N materialized workers.
    #[structopt(long, value_name = "N", default_value = "3")]
    workers: usize,
    /// Path to sqllogictest script to run.
    #[structopt(value_name = "PATH", required = true)]
    paths: Vec<String>,
}

#[tokio::main]
async fn main() {
    let args: Args = Args::from_args();

    let config = RunConfig {
        verbosity: args.verbosity,
        workers: args.workers,
        no_fail: args.no_fail,
    };

    if args.rewrite_results {
        return rewrite(args).await;
    }

    let json_summary_file = match args.json_summary_file {
        Some(filename) => match File::create(&filename) {
            Ok(file) => Some(file),
            Err(err) => {
                eprintln!("creating {}: {}", filename.display(), err);
                process::exit(1);
            }
        },
        None => None,
    };
    let mut bad_file = false;
    let mut outcomes = Outcomes::default();
    for path in &args.paths {
        if path == "-" {
            match sqllogictest::runner::run_stdin(&config).await {
                Ok(o) => outcomes += o,
                Err(err) => {
                    eprintln!("error: parsing stdin: {}", err);
                    bad_file = true;
                }
            }
        } else {
            for entry in WalkDir::new(path) {
                match entry {
                    Ok(entry) if entry.file_type().is_file() => {
                        match runner::run_file(&config, entry.path()).await {
                            Ok(o) => {
                                if o.any_failed() || config.verbosity >= 1 {
                                    println!(
                                        "{}",
                                        util::indent(&o.display(config.no_fail).to_string(), 4)
                                    );
                                }
                                outcomes += o;
                            }
                            Err(err) => {
                                eprintln!("error: parsing file: {}", err);
                                bad_file = true;
                            }
                        }
                    }
                    Ok(_) => (),
                    Err(err) => {
                        eprintln!("error: reading directory entry: {}", err);
                        bad_file = true;
                    }
                }
            }
        }
    }
    if bad_file {
        process::exit(1);
    }

    println!("{}", outcomes.display(config.no_fail));

    if let Some(json_summary_file) = json_summary_file {
        match serde_json::to_writer(json_summary_file, &outcomes.as_json()) {
            Ok(()) => (),
            Err(err) => {
                eprintln!("error: unable to write summary file: {}", err);
                process::exit(2);
            }
        }
    }

    if outcomes.any_failed() && !args.no_fail {
        process::exit(1);
    }
}

async fn rewrite(args: Args) {
    if args.json_summary_file.is_some() {
        eprintln!("--rewrite-results is not compatible with --json-summary-file");
        process::exit(1);
    }

    if args.paths.iter().any(|path| path == "-") {
        eprintln!("--rewrite-results cannot be used with stdin");
        process::exit(1);
    }

    let mut bad_file = false;
    for path in args.paths {
        for entry in WalkDir::new(path) {
            match entry {
                Ok(entry) => {
                    if entry.file_type().is_file() {
                        if let Err(err) = runner::rewrite_file(entry.path()).await {
                            eprintln!("error: rewriting file: {}", err);
                            bad_file = true;
                        }
                    }
                }
                Err(err) => {
                    eprintln!("error: reading directory entry: {}", err);
                    bad_file = true;
                }
            }
        }
    }
    if bad_file {
        process::exit(1);
    }
}
