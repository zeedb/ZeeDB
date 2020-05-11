use crate::*;
use std::fs;
use std::io;
use std::io::{Read, Write};
use zetasql::*;

fn read_expected(path: &String) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut expect = String::new();
    file.read_to_string(&mut expect)?;
    Ok(expect)
}

fn matches_expected(path: &String, found: String) -> bool {
    match read_expected(path) {
        Ok(expect) if expect == found => true,
        _ => {
            let mut file = fs::File::create(path).unwrap();
            file.write_all(found.as_bytes()).unwrap();
            false
        }
    }
}

macro_rules! ok {
    ($path:expr, $sql:expr, $errors:expr) => {
        let mut parser = ParseProvider::new();
        let sql = $sql.to_string();
        let (_, found) = parser.parse(&sql, 0, &adventure_works()).unwrap();
        let found = format!("{}\n\n{}", &sql, found);
        if !matches_expected(&$path.to_string(), found) {
            $errors.push($path.to_string());
        }
    };
}

fn adventure_works() -> SimpleCatalogProto {
    let mut count = 0;
    let mut table = |name: &str, columns: Vec<SimpleColumnProto>| -> SimpleTableProto {
        let serialization_id = count;
        count += 1;
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

    let person_id = column("person_id", TypeKind::TypeInt64);
    let last_name = column("first_name", TypeKind::TypeString);
    let first_name = column("last_name", TypeKind::TypeString);
    let modified_date = column("modified_date", TypeKind::TypeTimestamp);
    let person = table(
        "person",
        vec![person_id, last_name, first_name, modified_date],
    );

    let store_id = column("store_id", TypeKind::TypeInt64);
    let name = column("name", TypeKind::TypeString);
    let modified_date = column("modified_date", TypeKind::TypeTimestamp);
    let store = table("store", vec![store_id, name, modified_date]);

    let mut cat = catalog();
    cat.name = Some(String::from("aw"));
    cat.table = vec![customer, person, store];

    cat
}

#[test]
#[rustfmt::skip]
fn test_convert() {
    let mut errors = vec![];
    ok!("examples/aggregate/count.txt", "select count(*) from person", errors);
    ok!("examples/aggregate/distinct.txt", "select distinct store_id from customer", errors);
    ok!("examples/aggregate/group_by.txt", "select store_id from customer group by 1", errors);
    ok!("examples/aggregate/group_by_having.txt", "select store_id from customer group by 1 having count(*) > 1", errors);
    ok!("examples/aggregate/sum.txt", "select sum(1) from person", errors);
    ok!("examples/correlated/exists.txt", "select exists (select store_id from store where store.store_id = customer.store_id) as ok from customer", errors);
    ok!("examples/correlated/in.txt", "select 1 in (select store_id from store where store.store_id = customer.store_id) as ok from customer", errors);
    ok!("examples/correlated/join_on_in.txt", "select 1 from person join store on person.person_id in (select person_id from customer where customer.store_id = store.store_id)", errors); // TODO this is wrong
    ok!("examples/correlated/join_on_subquery.txt", "select 1 from person join customer on (select person.person_id = customer.person_id)", errors);
    ok!("examples/correlated/left_join_on_in.txt", "select 1 from person left join store on person.person_id in (select person_id from customer where customer.store_id = store.store_id)", errors); // TODO this is wrong
    ok!("examples/correlated/subquery.txt", "select (select name from store where store.store_id = customer.store_id) from customer", errors);
    ok!("examples/ddl/add_column.txt", "alter table customer add column foo string", errors);
    ok!("examples/ddl/create_database.txt", "create database foo", errors);
    ok!("examples/ddl/create_index.txt", "create index first_name_index on person (first_name)", errors);
    ok!("examples/ddl/create_table.txt", "create table foo (id int64 primary key, attr string)", errors);
    ok!("examples/ddl/create_table_as.txt", "create table foo (person_id int64 primary key, store_id int64) as select person_id, store_id from customer", errors);
    ok!("examples/ddl/create_table_partition_cluster.txt", "create table foo (id int64, data string) partition by (id) cluster by (id)", errors);
    ok!("examples/ddl/drop_column.txt", "alter table person drop column last_name", errors);
    ok!("examples/ddl/drop_table.txt", "drop table person", errors);
    ok!("examples/ddl/rename_column.txt", "rename column person.last_name to name", errors);
    ok!("examples/ddl/rename_column_upper.txt", "RENAME COLUMN PERSON.LAST_NAME TO NAME", errors);
    ok!("examples/dml/delete.txt", "delete customer where person_id = 1", errors);
    ok!("examples/dml/insert_multiple.txt", "insert into person (person_id, modified_date) values (1, (select current_timestamp())), (2, (select current_timestamp()))", errors);
    ok!("examples/dml/insert_subquery.txt", "insert into person (person_id, first_name, last_name, modified_date) values (1, \"Foo\", \"Bar\", (select current_timestamp()))", errors);
    ok!("examples/dml/insert_values.txt", "insert into person (person_id, first_name, last_name, modified_date) values (1, \"Foo\", \"Bar\", current_timestamp())", errors);
    ok!("examples/dml/update.txt", "update person set first_name = \"Foo\" where person_id = 1", errors);
    ok!("examples/dml/update_join.txt", "update customer set account_number = account_number + 1 from person where customer.person_id = person.person_id", errors);
    ok!("examples/dml/update_join_subquery.txt", "update customer set account_number = (select person.person_id) from person where customer.person_id = person.person_id", errors);
    ok!("examples/dml/update_subquery.txt", "update person set first_name = (select last_name) where person_id = 1", errors);
    ok!("examples/dml/update_where_in.txt", "update customer set account_number = 0 where person_id in (select person_id from person where first_name = \"Joe\")", errors);
    ok!("examples/expr/cast_column.txt", "select cast(customer_id as string) from customer", errors);
    ok!("examples/expr/cast_literal.txt", "select cast(1 as numeric) as bignum", errors);
    ok!("examples/expr/numeric_max.txt", "select cast(99999999999999999999999999999.999999999 as numeric)", errors); // TODO this is wrong
    ok!("examples/expr/numeric_min.txt", "select cast(-99999999999999999999999999999.999999999 as numeric)", errors); // TODO this is wrong
    ok!("examples/expr/numeric_neg.txt", "select cast(-1 as numeric)", errors); // TODO this is wrong
    ok!("examples/expr/numeric_one.txt", "select cast(1 as numeric)", errors); // TODO this is wrong
    ok!("examples/expr/numeric_zero.txt", "select cast(0 as numeric)", errors); // TODO this is wrong
    ok!("examples/expr/select_as.txt", "select \"foo\" as a", errors);
    ok!("examples/expr/select_from.txt", "select customer_id as a from customer", errors);
    ok!("examples/expr/select_literal.txt", "select customer_id as a, \"foo\" as b from customer", errors);
    ok!("examples/expr/select_one.txt", "select 1", errors);
    ok!("examples/filter/where.txt", "select customer_id from customer where store_id = 1", errors);
    ok!("examples/filter/where_exists.txt", "select 1 from person where exists (select person_id from customer where customer.person_id = person.person_id)", errors);
    ok!("examples/filter/where_in_correlated_subquery.txt", "select 1 from person, store where person.person_id in (select person_id from customer where customer.store_id = store.store_id)", errors); // TODO check this
    ok!("examples/filter/where_in_subquery.txt", "select 1 from person where person_id in (select person_id from customer)", errors);
    ok!("examples/filter/where_not_exists.txt", "select 1 from person where not exists (select person_id from customer where customer.person_id = person.person_id)", errors);
    ok!("examples/filter/where_not_in_subquery.txt", "select 1 from person where person_id not in (select person_id from customer)", errors);
    ok!("examples/join/explicit_equi_join.txt", "select customer.customer_id, store.store_id from customer join store on customer.store_id = store.store_id", errors);
    ok!("examples/join/full_join.txt", "select customer.customer_id, store.store_id from customer full outer join store on customer.store_id = store.store_id", errors);
    ok!("examples/join/implicit_cross_join.txt", "select customer.customer_id, store.store_id from customer, store", errors);
    ok!("examples/join/implicit_equi_join.txt", "select customer.customer_id, store.store_id from customer, store where customer.store_id = store.store_id", errors);
    ok!("examples/join/join_on_subquery_and_equi.txt", "select 1 from person, customer where person.person_id = customer.person_id and customer.store_id in (select store_id from store)", errors);
    ok!("examples/join/left_join.txt", "select customer.customer_id, store.store_id from customer left join store on customer.store_id = store.store_id", errors);
    ok!("examples/join/union.txt", "select 1 as a, 2 as b union all select 4 as b, 3 as a union all select 5 as a, 6 as b", errors);
    ok!("examples/sort/order_by_different.txt", "select customer_id from customer order by modified_date", errors);
    ok!("examples/sort/order_by_expr.txt", "select customer_id from customer order by customer_id + 1", errors);
    ok!("examples/sort/order_by_limit_offset.txt", "select customer_id from customer order by modified_date limit 100 offset 10", errors);
    ok!("examples/sort/order_by_same.txt", "select customer_id from customer order by customer_id", errors);
    ok!("examples/sort/unordered_limit_offset.txt", "select customer_id from customer limit 100 offset 10", errors);
    ok!("examples/subquery/from_select.txt", "select customer_id from (select customer_id from customer) as foo", errors);
    ok!("examples/subquery/with.txt", "with foo as (select customer_id from customer) select customer_id from foo", errors);   
    if !errors.is_empty() {
        panic!("{:#?}", errors);
    }
}
