use chrono::*;
use once_cell::sync::OnceCell;
use planner::*;
use rand::*;
use storage::Storage;

pub fn adventure_works() -> Storage {
    static ADVENTURE_WORKS: OnceCell<Storage> = OnceCell::new();

    ADVENTURE_WORKS
        .get_or_init(generate_adventure_works)
        .clone()
}

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
        let result: Result<Vec<_>, _> = crate::compile(expr).execute(&mut storage, txn).collect();
        result.unwrap();
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
    let customers = sample(N_PERSON, N_CUSTOMER);
    let lines: Vec<_> = (0..N_CUSTOMER)
        .map(|customer_id| {
            let person_id = customers[customer_id];
            // TODO some stores might have no customers.
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
    let mut offset = 0;
    while offset < N_PERSON {
        let n = (N_PERSON - offset).min(10_000);
        let lines: Vec<_> = (offset..offset + n)
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
        offset += n;
        println!("...wrote {} rows into person", n);
    }

    storage
}

fn sample(universe: usize, n: usize) -> Vec<usize> {
    let mut rng = rngs::SmallRng::from_seed([0; 16]);
    let mut array: Vec<_> = (0..universe).collect();
    for i in 0..n {
        let j = rng.gen_range(i, universe);
        array.swap(i, j);
    }
    array.drain(0..n).collect()
}
