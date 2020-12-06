use crate::parser::*;
use catalog::*;
use regex::Regex;
use zetasql::*;

macro_rules! ok {
    ($path:expr, $sql:expr, $errors:expr) => {
        let mut parser = ParseProvider::new();
        let trim = Regex::new(r"(?m)^\s+").unwrap();
        let sql = trim.replace_all($sql, "").trim().to_string();
        let catalog = adventure_works();
        let found = parser.analyze(&sql, &catalog).unwrap();
        let found = format!("{}\n\n{}", &sql, found);
        if !test_fixtures::matches_expected(&$path.to_string(), found) {
            $errors.push($path.to_string());
        }
    };
}

#[test]
fn test_convert() {
    let mut errors = vec![];
    ok!(
        "examples/aggregate/count.txt",
        r#"
            select count(*) 
            from person
        "#,
        errors
    );
    ok!(
        "examples/aggregate/distinct.txt",
        r#"
            select distinct store_id 
            from customer
        "#,
        errors
    );
    ok!(
        "examples/aggregate/group_by.txt",
        r#"
            select store_id 
            from customer 
            group by 1
        "#,
        errors
    );
    ok!(
        "examples/aggregate/group_by_having.txt",
        r#"
            select store_id 
            from customer 
            group by 1 
            having count(*) > 1
        "#,
        errors
    );
    ok!(
        "examples/aggregate/sum.txt",
        r#"
            select sum(1) 
            from person
        "#,
        errors
    );
    if !errors.is_empty() {
        panic!("{:#?}", errors);
    }
}

#[test]
fn test_correlated() {
    let mut errors = vec![];
    ok!(
        "examples/correlated/exists.txt",
        r#"
            select exists (select store_id from store where store.store_id = customer.store_id) as ok 
            from customer
        "#,
        errors
    );
    ok!(
        "examples/correlated/in.txt",
        r#"
            select 1 in (select store_id from store where store.store_id = customer.store_id) as ok 
            from customer
        "#,
        errors
    );
    ok!(
        "examples/correlated/join_on_in.txt",
        r#"
            select 1 
            from person 
            join store on person.person_id in (select person_id from customer where customer.store_id = store.store_id)
        "#,
        errors
    );
    ok!(
        "examples/correlated/join_on_subquery.txt",
        r#"
            select 1 
            from person 
            join customer on (select person.person_id = customer.person_id)
        "#,
        errors
    );
    // ok!(
    //     "examples/correlated/left_join_on_in.txt",
    //     r#"
    //         select 1
    //         from person
    //         left join store on person.person_id in (select person_id from customer where customer.store_id = store.store_id)
    //     "#,
    //     errors
    // ); TODO error handling
    ok!(
        "examples/correlated/subquery.txt",
        r#"
            select (select name from store where store.store_id = customer.store_id) 
            from customer
        "#,
        errors
    );
    if !errors.is_empty() {
        panic!("{:#?}", errors);
    }
}

#[test]
fn test_ddl() {
    let mut errors = vec![];
    ok!(
        "examples/ddl/create_database.txt",
        r#"
            create database foo
        "#,
        errors
    );
    ok!(
        "examples/ddl/create_index.txt",
        r#"
            create index first_name_index on person (first_name)
        "#,
        errors
    );
    ok!(
        "examples/ddl/create_table.txt",
        r#"
            create table foo (id int64 primary key, attr string)
        "#,
        errors
    );
    ok!(
        "examples/ddl/drop_table.txt",
        r#"
            drop table person
        "#,
        errors
    );
    if !errors.is_empty() {
        panic!("{:#?}", errors);
    }
}

#[test]
fn test_dml() {
    let mut errors = vec![];
    ok!(
        "examples/dml/delete.txt",
        r#"
            delete customer 
            where person_id = 1
        "#,
        errors
    );
    ok!(
        "examples/dml/insert_multiple.txt",
        r#"
            insert into person (person_id, modified_date) 
            values (1, (select current_timestamp())), (2, (select current_timestamp()))
        "#,
        errors
    );
    ok!(
        "examples/dml/insert_subquery.txt",
        r#"
            insert into person (person_id, first_name, last_name, modified_date) 
            values (1, "Foo", "Bar", (select current_timestamp()))
        "#,
        errors
    );
    ok!(
        "examples/dml/insert_values.txt",
        r#"
            insert into person (person_id, first_name, last_name, modified_date) 
            values (1, "Foo", "Bar", current_timestamp())
        "#,
        errors
    );
    ok!(
        "examples/dml/update.txt",
        r#"
            update person 
            set first_name = "Foo" 
            where person_id = 1
        "#,
        errors
    );
    ok!(
        "examples/dml/update_join.txt",
        r#"
            update customer 
            set account_number = account_number + 1 
            from person 
            where customer.person_id = person.person_id
        "#,
        errors
    );
    ok!(
        "examples/dml/update_join_subquery.txt",
        r#"
            update customer 
            set account_number = (select person.person_id) 
            from person 
            where customer.person_id = person.person_id
        "#,
        errors
    );
    ok!(
        "examples/dml/update_subquery.txt",
        r#"
            update person 
            set first_name = (select last_name) 
            where person_id = 1
        "#,
        errors
    );
    ok!(
        "examples/dml/update_where_in.txt",
        r#"
            update customer 
            set account_number = 0 
            where person_id in (select person_id from person where first_name = "Joe")
        "#,
        errors
    );
    if !errors.is_empty() {
        panic!("{:#?}", errors);
    }
}

#[test]
fn test_expr() {
    let mut errors = vec![];
    ok!(
        "examples/expr/cast_column.txt",
        r#"
            select cast(customer_id as string) 
            from customer
        "#,
        errors
    );
    ok!(
        "examples/expr/cast_literal.txt",
        r#"
            select cast(1 as numeric) as bignum
        "#,
        errors
    );
    ok!(
        "examples/expr/date_literal.txt",
        r#"
            select date '2001-01-01'
        "#,
        errors
    );
    ok!(
        "examples/expr/timestamp_literal.txt",
        r#"
            select timestamp '2001-01-01'
        "#,
        errors
    );
    ok!(
        "examples/expr/numeric_max.txt",
        r#"
            select cast(99999999999999999999999999999.999999999 as numeric)
        "#,
        errors
    ); // TODO this is wrong
    ok!(
        "examples/expr/numeric_min.txt",
        r#"
            select cast(-99999999999999999999999999999.999999999 as numeric)
        "#,
        errors
    ); // TODO this is wrong
    ok!(
        "examples/expr/numeric_neg.txt",
        r#"
            select cast(-1 as numeric)
        "#,
        errors
    ); // TODO this is wrong
    ok!(
        "examples/expr/numeric_one.txt",
        r#"
            select cast(1 as numeric)
        "#,
        errors
    ); // TODO this is wrong
    ok!(
        "examples/expr/numeric_zero.txt",
        r#"
            select cast(0 as numeric)
        "#,
        errors
    ); // TODO this is wrong
    ok!(
        "examples/expr/rename_column.txt",
        r#"
            select a + 1 
            from (select customer_id as a from customer)
        "#,
        errors
    ); // TODO this is wrong
    ok!(
        "examples/expr/select_as.txt",
        r#"
            select "foo" as a
        "#,
        errors
    );
    ok!(
        "examples/expr/select_from.txt",
        r#"
            select customer_id as a 
            from customer
        "#,
        errors
    );
    ok!(
        "examples/expr/select_literal.txt",
        r#"
            select customer_id as a, "foo" as b 
            from customer
        "#,
        errors
    );
    ok!(
        "examples/expr/select_one.txt",
        r#"
            select 1
        "#,
        errors
    );
    if !errors.is_empty() {
        panic!("{:#?}", errors);
    }
}

#[test]
fn test_filter() {
    let mut errors = vec![];
    ok!(
        "examples/filter/where.txt",
        r#"
            select customer_id 
            from customer 
            where store_id = 1
        "#,
        errors
    );
    ok!(
        "examples/filter/where_exists.txt",
        r#"
            select 1 
            from person 
            where exists (select person_id from customer where customer.person_id = person.person_id)
        "#,
        errors
    );
    ok!(
        "examples/filter/where_in_correlated_subquery.txt",
        r#"
            select 1 
            from person, store 
            where person.person_id in (select person_id from customer where customer.store_id = store.store_id)
        "#,
        errors
    ); // TODO check this
    ok!(
        "examples/filter/where_in_subquery.txt",
        r#"
            select 1 
            from person 
            where person_id in (select person_id from customer)
        "#,
        errors
    );
    ok!(
        "examples/filter/where_not_exists.txt",
        r#"
            select 1 
            from person 
            where not exists (select person_id from customer where customer.person_id = person.person_id)
        "#,
        errors
    );
    ok!(
        "examples/filter/where_not_in_subquery.txt",
        r#"
            select 1 
            from person 
            where person_id not in (select person_id from customer)
        "#,
        errors
    );
    if !errors.is_empty() {
        panic!("{:#?}", errors);
    }
}

#[test]
fn test_join() {
    let mut errors = vec![];
    ok!(
        "examples/join/explicit_equi_join.txt",
        r#"
            select customer.customer_id, store.store_id 
            from customer 
            join store on customer.store_id = store.store_id
        "#,
        errors
    );
    ok!(
        "examples/join/full_join.txt",
        r#"
            select customer.customer_id, store.store_id 
            from customer full outer 
            join store on customer.store_id = store.store_id
        "#,
        errors
    );
    ok!(
        "examples/join/implicit_cross_join.txt",
        r#"
            select customer.customer_id, store.store_id 
            from customer, store
        "#,
        errors
    );
    ok!(
        "examples/join/implicit_equi_join.txt",
        r#"
            select customer.customer_id, store.store_id 
            from customer, store 
            where customer.store_id = store.store_id
        "#,
        errors
    );
    ok!(
        "examples/join/join_on_subquery_and_equi.txt",
        r#"
            select 1 
            from person, customer 
            where person.person_id = customer.person_id and customer.store_id in (select store_id from store)
        "#,
        errors
    );
    ok!(
        "examples/join/left_join.txt",
        r#"
            select customer.customer_id, store.store_id 
            from customer 
            left join store on customer.store_id = store.store_id
        "#,
        errors
    );
    ok!(
        "examples/join/union.txt",
        r#"
            select 1 as a, 2 as b union all select 4 as b, 3 as a union all select 5 as a, 6 as b
        "#,
        errors
    );
    if !errors.is_empty() {
        panic!("{:#?}", errors);
    }
}

#[test]
fn test_sort() {
    let mut errors = vec![];
    ok!(
        "examples/sort/order_by_different.txt",
        r#"
            select customer_id 
            from customer 
            order by modified_date
        "#,
        errors
    );
    ok!(
        "examples/sort/order_by_expr.txt",
        r#"
            select customer_id 
            from customer 
            order by customer_id + 1
        "#,
        errors
    );
    ok!(
        "examples/sort/order_by_limit_offset.txt",
        r#"
            select customer_id 
            from customer 
            order by modified_date 
            limit 100 
            offset 10
        "#,
        errors
    );
    ok!(
        "examples/sort/order_by_same.txt",
        r#"
            select customer_id 
            from customer 
            order by customer_id
        "#,
        errors
    );
    ok!(
        "examples/sort/unordered_limit_offset.txt",
        r#"
            select customer_id 
            from customer 
            limit 100 
            offset 10
        "#,
        errors
    );
    if !errors.is_empty() {
        panic!("{:#?}", errors);
    }
}

#[test]
fn test_subquery() {
    let mut errors = vec![];
    ok!(
        "examples/subquery/from_select.txt",
        r#"
            select customer_id 
            from (select customer_id from customer) as foo
        "#,
        errors
    );
    ok!(
        "examples/subquery/with.txt",
        r#"
            with foo as (select customer_id from customer) select customer_id 
            from foo
        "#,
        errors
    );
    ok!(
        "examples/subquery/project_then_filter_twice.txt",
        r#"
            select * 
            from (select * from (select customer_id / 2 as id from customer) where id > 10) where id < 100
        "#,
        errors
    );
    if !errors.is_empty() {
        panic!("{:#?}", errors);
    }
}

#[test]
fn test_script() {
    let mut errors = vec![];
    ok!(
        "examples/script/set.txt",
        r#"
            set x = (select 1);
        "#,
        errors
    );
    ok!(
        "examples/script/select_variable.txt",
        r#"
            set x = 1;
            select @x;
        "#,
        errors
    );
    if !errors.is_empty() {
        panic!("{:#?}", errors);
    }
}

// #[test]
// fn test_single() {
//     let mut errors = vec![];
//     ok!(
//         "examples/script/select_variable.txt",
//         r#"
//             set x = 1;
//             select @x;
//         "#,
//         errors
//     );
//     if !errors.is_empty() {
//         panic!("{:#?}", errors);
//     }
// }

pub fn adventure_works() -> Catalog {
    let mut table_count = 100;
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
    let mut index_count = 100;
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

    let mut cat = Catalog::empty();
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
