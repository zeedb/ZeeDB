use crate::execute::*;
use arrow::record_batch::RecordBatch;
use fixtures::*;
use regex::Regex;
use storage::Storage;

macro_rules! ok {
    ($path:expr, $sql:expr, $errors:expr) => {
        let trim = Regex::new(r"(?m)^\s+").unwrap();
        let sql = trim.replace_all($sql, "").trim().to_string();
        let mut storage = Storage::new();
        let mut program = compile(&sql, &mut storage);
        let found = format!("{}\n\n{}", &sql, csv(program.next().unwrap().unwrap()));
        if !matches_expected(&$path.to_string(), found) {
            $errors.push($path.to_string());
        }
    };
}

fn compile<'a>(sql: &String, storage: &'a mut Storage) -> Program<'a> {
    let mut parser = parser::ParseProvider::new();
    let expr = parser.analyze(sql, (1, adventure_works())).unwrap();
    let expr = planner::optimize(expr, &mut parser);
    execute(expr, storage).unwrap()
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
    ok!(
        "examples/select_1.txt",
        r#"
            select 1;
        "#,
        errors
    );
    // ok!(
    //     "examples/create_table.txt",
    //     r#"
    //         create table foo (id int64);
    //     "#,
    //     errors
    // );
    if !errors.is_empty() {
        panic!("{:#?}", errors);
    }
}
