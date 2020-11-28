use crate::catalog_provider::CatalogProvider;
use crate::execute::*;
use arrow::record_batch::RecordBatch;
use regex::Regex;
use storage::Storage;
use test_fixtures::*;

fn run(path: &str, script: Vec<&str>, errors: &mut Vec<String>) {
    let trim = Regex::new(r"(?m)^\s+").unwrap();
    let mut output = "".to_string();
    let mut storage = Storage::new();
    for (txn, sql) in script.iter().enumerate() {
        let sql = trim.replace_all(sql, "").trim().to_string();
        let program = compile(&sql, txn as u64, &mut storage);
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

fn compile<'a>(sql: &String, txn: u64, storage: &'a mut Storage) -> Program<'a> {
    let mut parser = parser::ParseProvider::new();
    let catalog = CatalogProvider::new().catalog(txn, storage);
    let expr = parser.analyze(sql, &catalog).unwrap();
    let expr = planner::optimize(expr, &catalog, &mut parser);
    execute(expr, txn, storage)
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
    run("examples/select_1.txt", vec!["select 1;"], &mut errors);
    run(
        "examples/create_table.txt",
        vec!["create table foo (id int64)"],
        &mut errors,
    );
    run(
        "examples/insert.txt",
        vec![
            "create table foo (id int64);",
            "insert into foo (id) values (1);",
            "select * from foo;",
        ],
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
        &mut errors,
    );
    if !errors.is_empty() {
        panic!("{:#?}", errors);
    }
}
