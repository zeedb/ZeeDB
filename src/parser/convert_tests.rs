use crate::*;
use node::*;
use zetasql::*;

macro_rules! ok {
    ($sql:expr, $expect:expr) => {
        let sql = $sql;
        match ParseProvider::new().parse(sql, 0, adventure_works()) {
            Ok((_, found)) => {
                let expect = $expect;
                let found = format!("{}", found);
                if found != expect {
                    panic!(
                        "\n\tparse:\t{}\n\texpect:\t{}\n\tfound:\t{}\n",
                        sql, expect, found
                    )
                }
            }
            Err(err) => panic!("\n\tparse:\t{}\n\terror:\t{}\n", sql, err),
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
    let mut column = |name: &str, kind: TypeKind| -> SimpleColumnProto {
        SimpleColumnProto {
            name: Some(String::from(name)),
            r#type: Some(TypeProto {
                type_kind: Some(kind as i32),
                ..Default::default()
            }),
            ..Default::default()
        }
    };
    let customerID = column("customer_id", TypeKind::TypeInt64);
    let personID = column("person_id", TypeKind::TypeInt64);
    let storeID = column("store_id", TypeKind::TypeInt64);
    let accountNumber = column("account_number", TypeKind::TypeInt64);
    let modifiedDate = column("modified_date", TypeKind::TypeTimestamp);
    let customer = table(
        "customer",
        vec![customerID, personID, storeID, accountNumber, modifiedDate],
    );

    let personID = column("person_id", TypeKind::TypeInt64);
    let firstName = column("first_name", TypeKind::TypeString);
    let lastName = column("last_name", TypeKind::TypeString);
    let modifiedDate = column("modified_date", TypeKind::TypeTimestamp);
    let person = table("person", vec![personID, firstName, lastName, modifiedDate]);

    let storeID = column("store_id", TypeKind::TypeInt64);
    let name = column("name", TypeKind::TypeString);
    let modifiedDate = column("modified_date", TypeKind::TypeTimestamp);
    let store = table("store", vec![storeID, name, modifiedDate]);

    let mut cat = catalog();
    cat.name = Some(String::from("aw"));
    cat.table = vec![customer, person, store];

    cat
}

#[test]
#[rustfmt::skip]
fn test_convert() {
    ok!("select 1", "(LogicalProject [1 $col1] LogicalSingleGet)");
    ok!("select \"foo\" as a", "(LogicalProject [\"foo\" a] LogicalSingleGet)");
    ok!("select customer_id as a from customer", "LogicalGet customer");
    ok!("select customer_id as a, \"foo\" as b from customer", "(LogicalProject [\"foo\" b] LogicalGet customer)");
    ok!("select customer_id from customer where store_id = 1", "");
    ok!("select customer.customer_id, store.store_id from customer, store", "");
    ok!("select customer.customer_id, store.store_id from customer, store where customer.store_id = store.store_id", "");
    ok!("select customer.customer_id, store.store_id from customer join store on customer.store_id = store.store_id", "");
    ok!("select customer.customer_id, store.store_id from customer left join store on customer.store_id = store.store_id", "");
    ok!("select customer.customer_id, store.store_id from customer full outer join store on customer.store_id = store.store_id", "");
    ok!("select customer_id from customer limit 100 offset 10", "");
    ok!("select customer_id from customer order by customer_id", "");
    ok!("select customer_id from customer order by modified_date", "");
    ok!("select customer_id from customer order by modified_date limit 100 offset 10", "");
    ok!("select customer_id from customer order by customer_id + 1", "");
    ok!("select customer_id from (select customer_id from customer) as foo", "");
    ok!("with foo as (select customer_id from customer) select customer_id from foo", "");
    ok!("select store_id from customer group by 1", "");
    ok!("select store_id from customer group by 1 having count(*) > 1", "");
    ok!("select count(*) from person", "");
    ok!("select sum(1) from person", "");
    ok!("select distinct store_id from customer", "");
    ok!("select (select name from store where store.store_id = customer.store_id) from customer", "");
    ok!("select exists (select store_id from store where store.store_id = customer.store_id) as ok from customer", "");
    ok!("select 1 in (select store_id from store where store.store_id = customer.store_id) as ok from customer", "");
    ok!("select 1 from person where exists (select person_id from customer where customer.person_id = person.person_id)", "");
    ok!("select 1 from person where not exists (select person_id from customer where customer.person_id = person.person_id)", "");
    ok!("select 1 from person where person_id in (select person_id from customer)", "");
    ok!("select 1 from person where person_id not in (select person_id from customer)", "");
    ok!("select 1 as a, 2 as b union all select 4 as b, 3 as a union all select 5 as a, 6 as b", "");
    ok!("select cast(customer_id as string) from customer", "");
    ok!("select cast(1 as numeric) as bignum", "");
    ok!("select 1 from person, store where person.person_id in (select person_id from customer where customer.store_id = store.store_id)", "");
    ok!("select 1 from person, customer, store where person.person_id = customer.person_id and customer.store_id in (select store_id from store)", "");
    ok!("insert into person (person_id, first_name, last_name, modified_date) values (1, \"Foo\", \"Bar\", current_timestamp())", "");
    ok!("insert into person (person_id, first_name, last_name, modified_date) values (1, \"Foo\", \"Bar\", (select current_timestamp()))", "");
    ok!("insert into person (person_id, modified_date) values (1, (select current_timestamp())), (2, (select current_timestamp()))", "");
    ok!("update person set first_name = \"Foo\" where person_id = 1", "");
    ok!("update person set first_name = (select last_name) where person_id = 1", "");
    ok!("update customer set account_number = account_number + 1 from person where customer.person_id = person.person_id", "");
    ok!("update customer set account_number = (select person.person_id) from person where customer.person_id = person.person_id", "");
    ok!("update customer set account_number = 0 where person_id in (select person_id from person where first_name = \"Joe\")", "");
    ok!("delete customer where person_id = 1", "");
    ok!("create database foo", "");
    ok!("create table foo (id int64 primary key, attr string)", "");
    ok!("create or replace table foo (id int64 primary key, attr string)", "");
    ok!("create table foo (id int64, data string) partition by (id) cluster by (id)", "");
    ok!("create table foo (person_id int64 primary key, store_id int64) as select person_id, store_id from customer", "");
    ok!("create index first_name_index on person (first_name)", "");
    ok!("alter table customer add column foo string", "");
    ok!("alter table if exists customer add column foo string", "");
    ok!("rename column person.last_name to name", "");
    ok!("RENAME COLUMN PERSON.LAST_NAME TO NAME", "");
    ok!("alter table person drop column last_name", "");
    ok!("drop table person", "");
    ok!("select cast(0 as numeric)", "");
    ok!("select cast(-1 as numeric)", "");
    ok!("select cast(1 as numeric)", "");
    ok!("select cast(99999999999999999999999999999.999999999 as numeric)", "");
    ok!("select cast(-99999999999999999999999999999.999999999 as numeric)", "");
}
