use catalog::*;
use std::fs;
use std::io;
use std::io::{Read, Write};
use zetasql::*;

pub fn read_expected(path: &String) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut expect = String::new();
    file.read_to_string(&mut expect)?;
    Ok(expect)
}

pub fn matches_expected(path: &String, found: String) -> bool {
    match read_expected(path) {
        Ok(expect) if expect == found => true,
        _ => {
            match fs::File::create(path) {
                Ok(mut file) => file.write_all(found.as_bytes()).unwrap(),
                Err(err) => println!("{}: {}", path, err),
            }
            false
        }
    }
}

pub fn adventure_works() -> Catalog {
    let mut table_count = 0;
    let mut table = |name: &str, columns: Vec<SimpleColumnProto>| -> SimpleTableProto {
        let serialization_id = table_count;
        table_count += 1;
        SimpleTableProto {
            name: Some(String::from(name)),
            column: columns,
            serialization_id: Some(serialization_id),
            ..Default::default()
        }
    };
    let column = |name: &str, kind: TypeKind| -> SimpleColumnProto {
        SimpleColumnProto {
            name: Some(String::from(name)),
            r#type: Some(TypeProto {
                type_kind: Some(kind as i32),
                ..Default::default()
            }),
            ..Default::default()
        }
    };
    let mut index_count = 0;
    let mut index = |table_id: i64, column_name: &str| -> Index {
        let index_id = index_count;
        index_count += 1;
        Index {
            table_id,
            index_id,
            columns: vec![column_name.to_string()],
        }
    };
    let customer_id = column("customer_id", TypeKind::TypeInt64);
    let person_id = column("person_id", TypeKind::TypeInt64);
    let store_id = column("store_id", TypeKind::TypeInt64);
    let account_number = column("account_number", TypeKind::TypeInt64);
    let modified_date = column("modified_date", TypeKind::TypeTimestamp);
    let customer = table(
        "customer",
        vec![
            customer_id,
            person_id,
            store_id,
            account_number,
            modified_date,
        ],
    );
    let customer_table_id = customer.serialization_id.unwrap();

    let person_id = column("person_id", TypeKind::TypeInt64);
    let last_name = column("first_name", TypeKind::TypeString);
    let first_name = column("last_name", TypeKind::TypeString);
    let modified_date = column("modified_date", TypeKind::TypeTimestamp);
    let person = table(
        "person",
        vec![person_id, last_name, first_name, modified_date],
    );
    let person_table_id = person.serialization_id.unwrap();

    let store_id = column("store_id", TypeKind::TypeInt64);
    let name = column("name", TypeKind::TypeString);
    let modified_date = column("modified_date", TypeKind::TypeTimestamp);
    let store = table("store", vec![store_id, name, modified_date]);
    let store_table_id = store.serialization_id.unwrap();

    let mut cat = Catalog::empty(1);
    cat.catalog.name = Some(String::from("aw"));
    cat.catalog.table = vec![customer, person, store];
    cat.indexes.insert(
        customer_table_id,
        vec![
            index(customer_table_id, "customer_id"),
            index(customer_table_id, "person_id"),
            index(customer_table_id, "store_id"),
        ],
    );
    cat.indexes
        .insert(person_table_id, vec![index(customer_table_id, "person_id")]);
    cat.indexes
        .insert(store_table_id, vec![index(customer_table_id, "store_id")]);

    cat
}
