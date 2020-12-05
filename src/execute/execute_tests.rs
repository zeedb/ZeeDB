use crate::catalog_provider::CatalogProvider;
use crate::execute::*;
use arrow::record_batch::RecordBatch;
use catalog::Catalog;
use rand::Rng;
use rand::SeedableRng;
use regex::Regex;
use storage::Storage;
use test_fixtures::*;

fn run(path: &str, script: Vec<&str>, storage: &mut Storage, errors: &mut Vec<String>) {
    let trim = Regex::new(r"(?m)^\s+").unwrap();
    let mut output = "".to_string();
    for (txn, sql) in script.iter().enumerate() {
        let sql = trim.replace_all(sql, "").trim().to_string();
        let catalog = CatalogProvider::new().catalog((txn + 100) as u64, storage);
        let program = compile(&sql, (txn + 100) as u64, &catalog, storage);
        if let Some(Ok(last)) = program.last() {
            output = csv(last);
        } else {
            output = "".to_string();
        }
    }
    let found = format!("{}\n\n{}", script.join("\n"), output);
    if !matches_expected(&path.to_string(), found) {
        errors.push(path.to_string());
    }
}

fn setup(script: Vec<&str>, storage: &mut Storage) {
    let trim = Regex::new(r"(?m)^\s+").unwrap();
    for (txn, sql) in script.iter().enumerate() {
        let sql = trim.replace_all(sql, "").trim().to_string();
        let catalog = CatalogProvider::new().catalog(txn as u64, storage);
        let program = compile(&sql, txn as u64, &catalog, storage);
        program.last().unwrap().unwrap();
    }
}

fn compile<'a>(
    sql: &String,
    txn: u64,
    catalog: &'a Catalog,
    storage: &'a mut Storage,
) -> Program<'a> {
    let mut parser = parser::ParseProvider::new();
    let expr = parser.analyze(sql, &catalog).expect(sql.as_str());
    let expr = planner::optimize(expr, &catalog, &storage, &mut parser);
    execute(expr, txn, &catalog, storage)
}

fn csv(record_batch: RecordBatch) -> String {
    let trim = |field: &String| {
        if let Some(captures) = Regex::new(r"(.*)#\d+").unwrap().captures(field) {
            captures.get(1).unwrap().as_str().to_string()
        } else {
            field.clone()
        }
    };
    let header: Vec<String> = record_batch
        .schema()
        .fields()
        .iter()
        .map(|field| trim(field.name()))
        .collect();
    let mut csv_bytes = vec![];
    csv_bytes.extend_from_slice(header.join(",").as_bytes());
    csv_bytes.extend_from_slice("\n".as_bytes());
    arrow::csv::WriterBuilder::new()
        .has_headers(false)
        .build(&mut csv_bytes)
        .write(&record_batch)
        .unwrap();
    String::from_utf8(csv_bytes).unwrap()
}

#[test]
fn test_execute() {
    let mut errors = vec![];
    run(
        "examples/select_1.txt",
        vec!["select 1;"],
        &mut Storage::new(),
        &mut errors,
    );
    run(
        "examples/create_table.txt",
        vec!["create table foo (id int64)"],
        &mut Storage::new(),
        &mut errors,
    );
    run(
        "examples/insert.txt",
        vec![
            "create table foo (id int64);",
            "insert into foo (id) values (1);",
            "select * from foo;",
        ],
        &mut Storage::new(),
        &mut errors,
    );
    run(
        "examples/insert_vary_order.txt",
        vec![
            "create table foo (id int64, ok bool);",
            "insert into foo (id, ok) values (1, false);",
            "insert into foo (ok, id) values (true, 2);",
            "select * from foo;",
        ],
        &mut Storage::new(),
        &mut errors,
    );
    run(
        "examples/delete.txt",
        vec![
            "create table foo (id int64);",
            "insert into foo (id) values (1);",
            "delete from foo where id = 1;",
            "select * from foo;",
        ],
        &mut Storage::new(),
        &mut errors,
    );
    run(
        "examples/delete_then_insert.txt",
        vec![
            "create table foo (id int64);",
            "insert into foo (id) values (1);",
            "delete from foo where id = 1;",
            "insert into foo (id) values (2);",
            "select * from foo;",
        ],
        &mut Storage::new(),
        &mut errors,
    );
    run(
        "examples/update.txt",
        vec![
            "create table foo (id int64);",
            "insert into foo (id) values (1);",
            "update foo set id = 2 where id = 1;",
            "select * from foo;",
        ],
        &mut Storage::new(),
        &mut errors,
    );
    run(
        "examples/drop_table.txt",
        vec![
            "create table foo (id int64);",
            "insert into foo (id) values (1);",
            "drop table foo;",
            "create table foo (id int64);",
            "select * from foo;",
        ],
        &mut Storage::new(),
        &mut errors,
    );
    run(
        "examples/create_index.txt",
        vec![
            "create table foo (id int64);",
            "create index foo_id on foo (id);",
        ],
        &mut Storage::new(),
        &mut errors,
    );
    run(
        "examples/insert_into_index.txt",
        vec![
            "create table foo (id int64);",
            "create index foo_id on foo (id);",
            "insert into foo (id) values (1);",
            "select * from foo where id = 1",
        ],
        &mut Storage::new(),
        &mut errors,
    );
    run(
        "examples/update_index.txt",
        vec![
            "create table foo (id int64);",
            "create index foo_id on foo (id);",
            "insert into foo (id) values (1);",
            "update foo set id = 2 where id = 1;",
            "select * from foo where id = 2;",
        ],
        &mut Storage::new(),
        &mut errors,
    );
    if !errors.is_empty() {
        panic!("{:#?}", errors);
    }
}

#[test]
fn test_indexes() {
    let mut storage = Storage::new();
    // Setup.
    let mut rng = rand::rngs::StdRng::from_seed([0; 32]);
    let mut populate_fact = vec![];
    let mut populate_dim = vec![];
    for fact_id in 1..10_000 {
        let dim_id = rng.gen_range(0, 10_000);
        let fact_attr = rng.gen_range(0, 1_000_000);
        populate_fact.push(format!("({}, {}, {})", fact_id, dim_id, fact_attr));
    }
    for dim_id in 1..1_000 {
        let dim_attr = rng.gen_range(0, 1_000_000);
        populate_dim.push(format!("({}, {})", dim_id, dim_attr));
    }
    setup(
        vec![
            "create table fact (fact_id int64, dim_id int64, fact_attr int64);
            create table dim(dim_id int64, dim_attr int64);",
            "create index fact_id_index on fact (fact_id);
            create index dim_id_index on dim (dim_id);",
            format!(
                "insert into fact (fact_id, dim_id, fact_attr) values {};",
                populate_fact.join(", ")
            )
            .as_str(),
            format!(
                "insert into dim (dim_id, dim_attr) values {};",
                populate_dim.join(", ")
            )
            .as_str(),
        ],
        &mut storage,
    );
    // Tests.
    let mut errors = vec![];
    run(
        "examples/index_lookup.txt",
        vec!["select * from fact where fact_id = 1"],
        &mut storage,
        &mut errors,
    );
    run(
        "examples/lookup_join.txt",
        vec!["select * from fact join dim using (dim_id) where fact_id = 1"],
        &mut storage,
        &mut errors,
    );
    if !errors.is_empty() {
        panic!("{:#?}", errors);
    }
}
