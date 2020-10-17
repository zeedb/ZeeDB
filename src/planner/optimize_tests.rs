use crate::optimize::*;
use fixtures::*;
use regex::Regex;

macro_rules! ok {
    ($path:expr, $sql:expr, $errors:expr) => {
        let mut parser = parser::ParseProvider::new();
        let trim = Regex::new(r"(?m)^\s+").unwrap();
        let sql = trim.replace_all($sql, "").to_string();
        let (_, expr) = parser.parse(&sql, 0, &adventure_works()).unwrap();
        let expr = optimize(expr);
        let found = format!("{}\n\n{}", sql, expr);
        if !matches_expected(&$path.to_string(), found) {
            $errors.push($path.to_string());
        }
    };
}

#[test]
#[rustfmt::skip]
fn test_optimize() {
    let mut errors = vec![];
    ok!("examples/optimize/index_scan.txt", r#"
        select customer_id 
        from customer 
        where store_id = 1"#, errors);
    ok!("examples/optimize/nested_loop.txt", r#"
        select customer.customer_id, store.store_id 
        from customer, store"#, errors);
    ok!("examples/optimize/equi_join.txt", r#"
        select customer.customer_id, store.store_id 
        from customer, store 
        where customer.store_id = store.store_id"#, errors);
    ok!("examples/optimize/equi_full_outer_join.txt", r#"
        select customer.customer_id, store.store_id 
        from customer 
        full outer join store on customer.store_id = store.store_id"#, errors);
    ok!("examples/optimize/limit_offset.txt", r#"
        select customer_id 
        from customer 
        limit 100 
        offset 10"#, errors);
    ok!("examples/optimize/order_by.txt", r#"
        select customer_id 
        from customer 
        order by modified_date"#, errors);
    ok!("examples/optimize/group_by.txt", r#"
        select store_id 
        from customer 
        group by 1"#, errors);
    // ok!("examples/optimize/correlated_single_equi_join.txt", r#"
    //     select (select name from store where store.store_id = customer.store_id)
    //     from customer"#, errors);
    ok!("examples/optimize/redundant_table_free_single_join.txt", r#"select (select 1)"#, errors);
    ok!("examples/optimize/semi_join.txt", r#"
        select first_name 
        from person 
        where person_id in (select person_id from customer)"#, errors);
    ok!("examples/optimize/semi_join_or.txt", r#"
        select first_name 
        from person 
        where person_id in (select person_id from customer) 
        or person_id = 1"#, errors);
    // ok!("examples/optimize/correlated_single_equi_join_group_by.txt", r#"select store_id, (select count(1) from customer where customer.store_id = store.store_id) as customers from store"#, errors);
    ok!("examples/optimize/join_left_index_scan.txt", r#"
        select customer.customer_id, store.store_id 
        from customer, store 
        where customer.customer_id = 1"#, errors);
    ok!("examples/optimize/join_right_index_scan.txt", r#"
        select customer.customer_id, store.store_id 
        from customer, store 
        where store.store_id = 1"#, errors);
    ok!("examples/optimize/equi_join_left_index_scan.txt", r#"
        select customer.customer_id, store.store_id 
        from customer, store 
        where customer.store_id = store.store_id 
        and customer.customer_id = 1"#, errors);
    ok!("examples/optimize/equi_join_right_index_scan.txt", r#"
        select customer.customer_id, store.store_id 
        from customer, store 
        where customer.store_id = store.store_id 
        and store.store_id = 1"#, errors);
    ok!("examples/optimize/left_equi_join_right_index_scan.txt", r#"
        select customer.customer_id, store.store_id 
        from customer 
        left join store on customer.store_id = store.store_id 
        where store.store_id = 1"#, errors);
    ok!("examples/optimize/right_equi_join_right_index_scan.txt", r#"
        select customer.customer_id, store.store_id 
        from customer 
        right join store on customer.store_id = store.store_id 
        where store.store_id = 1"#, errors);
    ok!("examples/optimize/union_all.txt", r#"select 1 as a union all select 2 as a"#, errors);
    // ok!("examples/optimize/project_then_filter.txt", r#"select 1 from (select *, rand() as random from customer) where customer_id < random and customer_id <> 0"#, errors);
    // ok!("examples/optimize/project_then_filter_twice.txt", r#"select * from (select * from (select customer_id / 2 as id from customer) where id > 10) where id < 100"#, errors);
    // ok!("examples/optimize/correlated_semi_join.txt", r#"select 1 from person, store where person.person_id in (select person_id from customer where customer.store_id = store.store_id)"#, errors);
    // ok!("examples/optimize/equi_join_semi_join.txt", r#"select 1 from person, customer where person.person_id = customer.person_id and customer.store_id in (select store_id from store)"#, errors);
    // ok!("examples/optimize/correlated_semi_join_anti_join.txt", r#"
    // select 1 from customer
    // where exists (select 1 from person where person.person_id = customer.person_id)
    // and not exists (select 1 from store where store.store_id = customer.store_id and customer.modified_date > store.modified_date)
    // "#, errors);
    // ok!("examples/optimize/correlated_single_join_twice.txt", r#"select (select name from store where store.store_id = customer.customer_id), (select first_name from person where person.person_id = customer.person_id) from customer"#, errors);
    // ok!("examples/optimize/correlated_single_join_twice_plus_condition.txt", r#"select (select name from store where store.store_id = customer.customer_id and store.name like "A%"), (select first_name from person where person.person_id = customer.person_id) from customer"#, errors);
    // ok!("examples/optimize/single_join_in_where_clause.txt", r#"select person_id from person where modified_date = (select max(modified_date) from person)"#, errors);
    // ok!("examples/optimize/correlated_semi_join_to_group_by.txt", r#"select 1 from customer c1 where c1.customer_id in (select max(c2.customer_id) from customer c2 where c1.store_id = c2.store_id group by c2.account_number)"#, errors);
    // ok!("examples/optimize/correlated_semi_join_to_group_by_correlated_column.txt", r#"select 1 from customer c1 where c1.customer_id in (select max(c2.customer_id) from customer c2 where c1.store_id = c2.store_id group by c2.account_number, c1.account_number)"#, errors);
    // ok!("examples/optimize/correlated_semi_equi_join.txt", r#"select 1 from person where person_id in (select person_id from customer where person.modified_date = customer.modified_date)"#, errors);
    // ok!("examples/optimize/correlated_single_join_with_condition.txt", r#"select (select max(modified_date) from customer where customer.store_id = store.store_id and store.name like "A%") from store"#, errors);
    // ok!("examples/optimize/correlated_single_join_with_condition_and_group_by.txt", r#"select (select max(modified_date) from customer where customer.store_id = store.store_id and customer.account_number > 100) from store"#, errors);
    // ok!("examples/optimize/correlated_semi_join_in_where_clause.txt", r#"
    // select customer_id 
    // from customer c1, store where c1.store_id = store.store_id 
    // and c1.modified_date in (select modified_date from customer c2 where c2.account_number > c1.account_number)
    // "#, errors);
    // ok!("examples/optimize/correlated_semi_self_join.txt", r#"select customer_id from (select *, account_number - 1 as prev_account_number from customer) c1 where person_id in (select person_id from customer c2 where c1.prev_account_number = c2.account_number)"#, errors);
    // ok!("examples/optimize/correlated_semi_join_then_order_by.txt", r#"select first_name from person where person_id in (select person_id from customer) order by modified_date limit 10"#, errors);
    // ok!("examples/optimize/count_and_sum_distinct.txt", r#"select count(distinct account_number), sum(distinct account_number) from customer"#, errors);
    // ok!("examples/optimize/use_with_clause_twice.txt", r#"with foo as (select customer_id, store_id from customer) select f1.customer_id, f2.customer_id from foo f1, foo f2 where f1.store_id = f2.store_id"#, errors);
    // ok!("examples/optimize/redundant_with_clause.txt", r#"with foo as (select * from customer) select customer_id from foo"#, errors);
    // ok!("examples/optimize/redundant_with_clause_with_projection.txt", r#"with foo as (select customer_id, current_date() as r from customer) select customer_id, r from foo"#, errors);
    // ok!("examples/optimize/use_with_select_star_twice.txt", r#"with foo as (select * from customer) select f1.customer_id, f2.customer_id from foo f1, foo f2 where f1.store_id = f2.store_id"#, errors);
    // ok!("examples/optimize/use_with_project_twice.txt", r#"with foo as (select *, current_date() as r from customer) select f1.customer_id, f2.customer_id from foo f1, foo f2 where f1.r = f2.r"#, errors);
    // ok!("examples/optimize/insert_values.txt", r#"insert into person (person_id, first_name, last_name, modified_date) values (1, "Foo", "Bar", current_timestamp())"#, errors);
    // ok!("examples/optimize/insert_table_free_single_join.txt", r#"insert into person (person_id, first_name, last_name, modified_date) values (1, "Foo", "Bar", (select current_timestamp()))"#, errors);
    // ok!("examples/optimize/insert_two_table_free_single_joins.txt", r#"insert into person (person_id, modified_date) values (1, (select current_timestamp())), (2, (select current_timestamp()))"#, errors);
    // ok!("examples/optimize/update_where.txt", r#"update person set first_name = "Foo" where person_id = 1"#, errors);
    // ok!("examples/optimize/update_set_table_free_single_join.txt", r#"update person set first_name = (select last_name) where person_id = 1"#, errors);
    // ok!("examples/optimize/update_equi_join.txt", r#"update customer set account_number = account_number + 1 from person where customer.person_id = person.person_id"#, errors);
    // ok!("examples/optimize/update_set_redundant_single_join.txt", r#"update customer set account_number = (select person.person_id) from person where customer.person_id = person.person_id"#, errors);
    // ok!("examples/optimize/update_semi_join.txt", r#"update customer set account_number = 0 where person_id in (select person_id from person where first_name = "Joe")"#, errors);
    // ok!("examples/optimize/update_set_default.txt", r#"update customer set account_number = default where person_id = 1"#, errors);
    // ok!("examples/optimize/delete.txt", r#"delete customer where person_id = 1"#, errors);
    // ok!("examples/optimize/delete_semi_join.txt", r#"delete person where person_id in (select person_id from customer)"#, errors);
    // ok!("examples/optimize/delete_semi_join_with_condition.txt", r#"delete customer where person_id in (select person_id from customer where account_number = 0)"#, errors);
    // ok!("examples/optimize/create_table.txt", r#"create table foo (person_id int64 primary key, store_id int64)"#, errors);
    // ok!("examples/optimize/create_table_as.txt", r#"create table foo (person_id int64 primary key, store_id int64) as select person_id, store_id from customer"#, errors);
    if !errors.is_empty() {
        panic!("{:#?}", errors);
    }
}
