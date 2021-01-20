use chrono::*;
use once_cell::sync::OnceCell;
use planner::*;
use rand::{rngs::SmallRng, Rng, SeedableRng};
use storage::Storage;

pub fn adventure_works() -> Storage {
    static ADVENTURE_WORKS: OnceCell<Storage> = OnceCell::new();

    ADVENTURE_WORKS
        .get_or_init(|| generate_adventure_works(1_000))
        .clone()
}

fn generate_adventure_works(n_store: usize) -> Storage {
    let n_customer = n_store * 10;
    let n_person = n_customer * 2;
    fn timestamp(secs: i64) -> DateTime<Utc> {
        DateTime::from_utc(NaiveDateTime::from_timestamp(secs, 0), Utc)
    }
    println!("Initialize adventure_works database...");
    let mut storage = Storage::new();
    let mut txn = 0;
    // Create tables.
    execute(&mut storage, vec![
        "create table store (store_id int64, name string, modified_date timestamp);",
        "create table customer (customer_id int64, person_id int64, store_id int64, account_number int64, modified_date timestamp);",
        "create table person (person_id int64, first_name string, last_name string, modified_date timestamp);",
    ].join("\n").as_str(), &mut txn,);
    // Create indexes.
    execute(
        &mut storage,
        vec![
            "create index store_id on store (store_id);",
            "create index customer_id on customer (customer_id);",
            "create index person_id on person (person_id);",
        ]
        .join("\n")
        .as_str(),
        &mut txn,
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
    execute(
        &mut storage,
        format!("insert into store values\n{};", lines.join(",\n")).as_str(),
        &mut txn,
    );
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
        execute(
            &mut storage,
            format!("insert into customer values\n{};", lines.join(",\n")).as_str(),
            &mut txn,
        );
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
        execute(
            &mut storage,
            format!("insert into person values\n{};", lines.join(",\n")).as_str(),
            &mut txn,
        );
        println!("...wrote 10_000 rows into person");
    }

    storage
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

fn execute(storage: &mut Storage, sql: &str, txn: &mut i64) {
    let catalog = crate::catalog::catalog(storage, *txn);
    let indexes = crate::catalog::indexes(storage, *txn);
    let expr = parser::analyze(catalog::ROOT_CATALOG_ID, &catalog, sql).unwrap();
    let expr = optimize(catalog::ROOT_CATALOG_ID, &catalog, &indexes, &storage, expr);
    let _result: Vec<_> = crate::compile(expr).execute(storage, *txn).collect();
    *txn += 1;
}
