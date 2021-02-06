use std::collections::HashMap;

use catalog::CATALOG_KEY;
use chrono::*;
use context::Context;
use once_cell::sync::OnceCell;
use parser::{Parser, PARSER_KEY};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use statistics::{Statistics, STATISTICS_KEY};
use storage::{Storage, STORAGE_KEY};

use crate::MetadataCatalog;

pub fn adventure_works() -> (Storage, Statistics) {
    static ADVENTURE_WORKS: OnceCell<(Storage, Statistics)> = OnceCell::new();

    ADVENTURE_WORKS
        .get_or_init(|| generate_adventure_works(1_000))
        .clone()
}

fn generate_adventure_works(n_store: usize) -> (Storage, Statistics) {
    let mut context = Context::default();
    context.insert(STORAGE_KEY, Storage::default());
    context.insert(STATISTICS_KEY, Statistics::default());
    context.insert(PARSER_KEY, Parser::default());
    context.insert(CATALOG_KEY, Box::new(MetadataCatalog));
    populate_adventure_works(n_store, &mut context);
    (context.remove(STORAGE_KEY), context.remove(STATISTICS_KEY))
}

fn populate_adventure_works(n_store: usize, context: &mut Context) {
    let n_customer = n_store * 10;
    let n_person = n_customer * 2;
    fn timestamp(secs: i64) -> DateTime<Utc> {
        DateTime::from_utc(NaiveDateTime::from_timestamp(secs, 0), Utc)
    }
    println!("Initialize adventure_works database...");
    let mut txn = 0;
    // Create tables.
    execute(vec![
        "create table store (store_id int64, name string, modified_date timestamp);",
        "create table customer (customer_id int64, person_id int64, store_id int64, account_number int64, modified_date timestamp);",
        "create table person (person_id int64, first_name string, last_name string, modified_date timestamp);",
    ], &mut txn, context);
    // Create indexes.
    execute(
        vec![
            "create index store_id_index on store (store_id);",
            "create index customer_id_index on customer (customer_id);",
            "create index person_id_index on person (person_id);",
        ],
        &mut txn,
        context,
    );
    // Populate tables.
    const LOW_TIME: i64 = 946688400;
    const HIGH_TIME: i64 = 1577840400;
    let mut rng = SmallRng::from_seed([0; 32]);
    // Store.
    let lines: Vec<_> = (0..n_store)
        .map(|store_id| {
            let store_name = rng.gen_range(0..1_000_000);
            let modified_date = timestamp(rng.gen_range(LOW_TIME..HIGH_TIME)).to_rfc3339();
            format!(
                "({}, '{}', timestamp '{}')",
                store_id, store_name, modified_date
            )
        })
        .collect();
    let insert = format!("insert into store values\n{};", lines.join(",\n"));
    execute(vec![&insert], &mut txn, context);
    println!("...wrote {} rows into store", n_store);
    // Customer.
    let customers = sample(n_person, n_customer);
    for customer_id_start in (0..n_customer).step_by(10_000) {
        let lines: Vec<_> = (customer_id_start..n_customer.min(customer_id_start + 10_000))
            .map(|customer_id| {
                let person_id = customers[customer_id];
                // TODO some stores might have no customers.
                let store_id = rng.gen_range(0..n_store);
                let account_number = rng.gen_range(0..n_customer * 10);
                let modified_date = timestamp(rng.gen_range(LOW_TIME..HIGH_TIME)).to_rfc3339();
                format!(
                    "({}, {}, {}, {}, timestamp '{}')",
                    customer_id, person_id, store_id, account_number, modified_date
                )
            })
            .collect();
        let insert = format!("insert into customer values\n{};", lines.join(",\n"));
        execute(vec![&insert], &mut txn, context);
        println!("...wrote 10_000 rows into customer");
    }
    // Person.
    for person_id_start in (0..n_person).step_by(10_000) {
        let lines: Vec<_> = (person_id_start..n_person.min(person_id_start + 10_000))
            .map(|person_id| {
                let first_name = rng.gen_range(0..1_000_000);
                let last_name = rng.gen_range(0..1_000_000);
                let modified_date = timestamp(rng.gen_range(LOW_TIME..HIGH_TIME)).to_rfc3339();
                format!(
                    "({}, '{}', '{}', timestamp '{}')",
                    person_id, first_name, last_name, modified_date
                )
            })
            .collect();
        let insert = format!("insert into person values\n{};", lines.join(",\n"));
        execute(vec![&insert], &mut txn, context);
        println!("...wrote 10_000 rows into person");
    }
}

fn execute(script: Vec<&str>, txn: &mut i64, context: &mut Context) {
    let sql = script.join("\n");
    let parser = &context[PARSER_KEY];
    let expr = parser.analyze(&sql, catalog::ROOT_CATALOG_ID, *txn, vec![], context);
    let expr = planner::optimize(expr, *txn, context);
    crate::execute::execute_mut(expr, *txn, HashMap::new(), context).last();
    *txn += 1;
}

fn sample(universe: usize, n: usize) -> Vec<usize> {
    let mut rng = SmallRng::from_seed([0; 32]);
    let mut array: Vec<_> = (0..universe).collect();
    for i in 0..n {
        let j = rng.gen_range(i..universe);
        array.swap(i, j);
    }
    array.drain(0..n).collect()
}
