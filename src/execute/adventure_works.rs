use crate::execute::*;
use arrow::record_batch::RecordBatch;
use ast::Expr;
use catalog::Index;
use chrono::*;
use once_cell::sync::OnceCell;
use planner::*;
use rand::*;
use regex::Regex;
use std::collections::HashMap;
use std::sync::Mutex;
use storage::Storage;
use test_fixtures::*;
use zetasql::SimpleCatalogProto;

fn generate_adventure_works() -> Storage {
    fn timestamp(secs: i64) -> DateTime<Utc> {
        DateTime::from_utc(NaiveDateTime::from_timestamp(secs, 0), Utc)
    }
    println!("Initialize adventure_works database...");
    let mut storage = Storage::new();
    let mut txn = 0;
    let mut execute = |sql: &str| {
        let catalog = crate::catalog::catalog(&mut storage, txn);
        let indexes = crate::catalog::indexes(&mut storage, txn);
        let expr = parser::analyze(catalog::ROOT_CATALOG_ID, &catalog, sql).unwrap();
        let expr = optimize(catalog::ROOT_CATALOG_ID, &catalog, &indexes, &storage, expr);
        crate::execute(&mut storage, txn, expr)
            .last()
            .unwrap()
            .unwrap();
        txn += 1;
    };
    // Create tables.
    execute(vec![
        "create table store (store_id int64, name string, modified_date timestamp);",
        "create table customer (customer_id int64, person_id int64, store_id int64, account_number int64, modified_date timestamp);",
        "create table person (person_id int64, first_name string, last_name string, modified_date timestamp);",
    ].join("\n").as_str());
    // Create indexes.
    execute(
        vec![
            "create index store_id on store (store_id);",
            "create index customer_id on customer (customer_id);",
            "create index person_id on person (person_id);",
        ]
        .join("\n")
        .as_str(),
    );
    // Populate tables.
    const N_STORE: usize = 1000;
    const N_CUSTOMER: usize = 10_000;
    const N_PERSON: usize = 20_000;
    const LOW_TIME: i64 = 946688400;
    const HIGH_TIME: i64 = 1577840400;
    let mut rng = rngs::SmallRng::from_seed([0; 16]);
    // Store.
    let lines: Vec<_> = (0..N_STORE)
        .map(|store_id| {
            let store_name = rng.gen_range(0, 1_000_000);
            let modified_date = timestamp(rng.gen_range(LOW_TIME, HIGH_TIME)).to_rfc3339();
            format!(
                "({}, '{}', timestamp '{}')",
                store_id, store_name, modified_date
            )
        })
        .collect();
    execute(format!("insert into store values\n{};", lines.join(",\n")).as_str());
    println!("...wrote {} rows into store", N_STORE);
    // Customer.
    let lines: Vec<_> = (0..N_CUSTOMER)
        .map(|customer_id| {
            let person_id = rng.gen_range(0, N_PERSON);
            let store_id = rng.gen_range(0, N_STORE);
            let account_number = rng.gen_range(0, N_CUSTOMER * 10);
            let modified_date = timestamp(rng.gen_range(LOW_TIME, HIGH_TIME)).to_rfc3339();
            format!(
                "({}, {}, {}, {}, timestamp '{}')",
                customer_id, person_id, store_id, account_number, modified_date
            )
        })
        .collect();
    execute(format!("insert into customer values\n{};", lines.join(",\n")).as_str());
    println!("...wrote {} rows into customer", N_CUSTOMER);
    // Person.
    let mut remaining = N_PERSON;
    while remaining > 0 {
        let queued = remaining.min(10_000);
        let lines: Vec<_> = (0..queued)
            .map(|person_id| {
                let first_name = rng.gen_range(0, 1_000_000);
                let last_name = rng.gen_range(0, 1_000_000);
                let modified_date = timestamp(rng.gen_range(LOW_TIME, HIGH_TIME)).to_rfc3339();
                format!(
                    "({}, '{}', '{}', timestamp '{}')",
                    person_id, first_name, last_name, modified_date
                )
            })
            .collect();
        execute(format!("insert into person values\n{};", lines.join(",\n")).as_str());
        remaining -= queued;
        println!("...wrote {} rows into person", queued);
    }

    storage
}

pub(crate) fn copy() -> Storage {
    static ADVENTURE_WORKS: OnceCell<Storage> = OnceCell::new();

    ADVENTURE_WORKS
        .get_or_init(generate_adventure_works)
        .clone()
}
