use crate::test_suite::*;

#[test]
fn test_aggregate() {
    let mut t = TestSuite::adventure_works();
    t.comment("combine_consecutive_projects");
    t.ok("explain select a + 1 as b from (select 1 as a)");
    t.comment("combine_consecutive_projects_star");
    t.ok("explain select *, a + 1 as b from (select 1 as a)");
    t.comment("count_and_sum_distinct");
    t.ok(
        "explain select count(distinct account_number), sum(distinct account_number) from customer",
    );
    t.comment("group_by");
    t.ok("explain select store_id from customer group by 1");
    t.comment("avg");
    t.ok("explain select avg(store_id) from customer");
    t.finish("examples/optimize_aggregate.testlog");
}

#[test]
fn test_correlated() {
    let mut t = TestSuite::adventure_works();
    t.comment("redundant_table_free_single_join");
    t.ok("explain select (select 1)");
    t.comment("semi_join");
    t.ok(
        "explain select first_name from person where person_id in (select person_id from customer)",
    );
    t.comment("semi_join_or");
    t.ok(
        "explain select first_name from person where person_id in (select person_id from customer) or person_id = 1",
    );
    t.comment("equi_join_semi_join");
    t.ok(
        "explain select 1 from person, customer where person.person_id = customer.person_id and customer.store_id in (select store_id from store)",
    );
    t.comment("single_join_in_where_clause");
    t.ok(
        "explain select person_id from person where modified_date = (select max(modified_date) from person)",
    );
    t.comment("insert_table_free_single_join");
    t.ok(
        "explain insert into person (person_id, first_name, last_name, modified_date) values (1, 'Foo', 'Bar', (select timestamp '2020-01-01'))",
    );
    t.comment("insert_two_table_free_single_joins");
    t.ok(
        "explain insert into person (person_id, modified_date) values (1, (select timestamp '2020-01-01')), (2, (select timestamp '2020-01-01'))",
    );
    t.comment("update_semi_join");
    t.ok(
        "explain update customer set account_number = 0 where person_id in (select person_id from person where first_name = 'Joe')",
    );
    t.comment("delete_semi_join");
    t.ok("explain delete person where person_id in (select person_id from customer)");
    t.comment("delete_semi_join_with_condition");
    t.ok(
        "explain delete customer where person_id in (select person_id from customer where account_number = 0)",
    );
    t.comment("single_equi_join");
    t.ok(
        "explain select (select name from store where store.store_id = customer.store_id) from customer",
    );
    t.comment("single_equi_join_group_by");
    t.ok(
        "explain select store_id, (select count(1) from customer where customer.store_id = store.store_id) as customers from store",
    );
    t.comment("semi_join_with_condition");
    t.ok(
        "explain select 1 from person, store where person.person_id in (select person_id from customer where customer.store_id = store.store_id)",
    );
    t.comment("semi_join_anti_join");
    t.ok(
        "explain select 1 from customer where exists (select 1 from person where person.person_id = customer.person_id) and not exists (select 1 from store where store.store_id = customer.store_id and customer.modified_date > store.modified_date)",
    );
    t.comment("single_join_twice");
    t.ok(
        "explain select (select name from store where store.store_id = customer.customer_id), (select first_name from person where person.person_id = customer.person_id) from customer",
    );
    t.comment("single_join_twice_plus_condition");
    t.ok(
        "explain select (select name from store where store.store_id = customer.customer_id and store.name like 'A%'), (select first_name from person where person.person_id = customer.person_id) from customer",
    );
    t.comment("semi_join_to_group_by");
    t.ok(
        "explain select 1 from customer c1 where c1.customer_id in (select max(c2.customer_id) from customer c2 where c1.store_id = c2.store_id group by c2.account_number)",
    );
    t.comment("semi_join_to_group_by_correlated_column");
    t.ok(
        "explain select 1 from customer c1 where c1.customer_id in (select max(c2.customer_id) from customer c2 where c1.store_id = c2.store_id group by c2.account_number, c1.account_number)",
    );
    t.comment("semi_equi_join");
    t.ok(
        "explain select 1 from person where person_id in (select person_id from customer where person.modified_date = customer.modified_date)",
    );
    t.comment("single_join_with_condition");
    t.ok(
        "explain select (select max(modified_date) from customer where customer.store_id = store.store_id and store.name like 'A%') from store",
    );
    t.comment("single_join_with_condition_and_group_by");
    t.ok(
        "explain select (select max(modified_date) from customer where customer.store_id = store.store_id and customer.account_number > 100) from store",
    );
    t.comment("semi_join_in_where_clause");
    t.ok(
        "explain select customer_id from customer c1, store where c1.store_id = store.store_id and c1.modified_date in (select modified_date from customer c2 where c2.account_number > c1.account_number)",
    );
    t.comment("semi_self_join");
    t.ok(
        "explain select customer_id from (select *, account_number - 1 as prev_account_number from customer) c1 where person_id in (select person_id from customer c2 where c1.prev_account_number = c2.account_number)",
    );
    t.comment("semi_join_then_order_by");
    t.ok(
        "explain select first_name from person where person_id in (select person_id from customer) order by modified_date limit 10",
    );
    t.comment("update_set_table_free_single_join");
    t.ok("explain update person set first_name = (select last_name) where person_id = 1");
    t.comment("update_set_redundant_single_join");
    t.ok(
        "explain update customer set account_number = (select person.person_id) from person where customer.person_id = person.person_id",
    );
    t.finish("examples/optimize_correlated.testlog");
}

#[test]
fn test_ddl() {
    let mut t = TestSuite::adventure_works();
    t.comment("create_database");
    t.ok("explain create database foo");
    t.comment("drop_database");
    t.ok("explain drop database foo");
    t.comment("create_database_nested");
    t.ok("explain create database nested.foo");
    t.comment("drop_database_nested");
    t.ok("explain drop database nested.foo");
    t.comment("create_table");
    t.ok("explain create table foo (person_id int64 primary key, store_id int64)");
    t.comment("drop_table");
    t.ok("explain drop table foo");
    t.comment("create_table_nested");
    t.ok("explain create table nested.foo (person_id int64 primary key, store_id int64)");
    t.comment("drop_table_nested");
    t.ok("explain drop table nested.foo");
    t.comment("create_index");
    t.ok("explain create index foo_index on person (person_id)");
    t.comment("drop_index");
    t.ok("explain drop index foo_index");
    t.comment("create_index_nested");
    t.ok("explain create index nested.foo_index on person (person_id)");
    t.comment("drop_index_nested");
    t.ok("explain drop index nested.foo_index");
    t.finish("examples/optimize_ddl.testlog");
}

#[test]
fn test_dml() {
    let mut t = TestSuite::adventure_works();
    t.comment("delete");
    t.ok("explain delete customer where person_id = 1");
    t.comment("insert_values");
    t.ok(
        "explain insert into person (person_id, first_name, last_name, modified_date) values (1, 'Foo', 'Bar', timestamp '2020-01-01')",
    );
    t.comment("update_where");
    t.ok("explain update person set first_name = 'Foo' where person_id = 1");
    t.comment("update_equi_join");
    t.ok(
        "explain update customer set account_number = account_number + 1 from person where customer.person_id = person.person_id",
    );
    // t.comment("update_set_default");
    // t.ok("explain update customer set account_number = default where person_id = 1");
    t.finish("examples/optimize_dml.testlog");
}

#[test]
fn test_filter() {
    let mut t = TestSuite::adventure_works();
    t.comment("combine_consecutive_filters");
    t.ok(
        "explain select 1 from (select * from person where first_name like 'A%') where last_name like 'A%'",
    );
    t.comment("index_scan");
    t.ok("explain select store_id from customer where customer_id = 1");
    t.comment("project_then_filter");
    t.ok(
        "explain select 1 from (select *, 1234 as random from customer) where customer_id < random and customer_id <> 0",
    );
    t.comment("project_then_filter_twice");
    t.ok(
        "explain select * from (select * from (select customer_id / 2 as id from customer) where id > 10) where id < 100",
    );
    t.comment("pull_filter_through_aggregate");
    t.ok(
        "explain select store_id, (select count(*) from customer where customer.store_id = store.store_id) from store",
    );
    t.comment("push_filter_through_project");
    t.ok("explain select * from (select *, customer_id + 1 from customer) where customer_id = 1");
    t.finish("examples/optimize_filter.testlog");
}

#[test]
fn test_join() {
    let mut t = TestSuite::adventure_works();
    t.comment("semi_join");
    t.ok(
        "explain select 1 from person where exists (select 1 from customer where customer.person_id = person.person_id)",
    );
    t.comment("anti_join");
    t.ok(
        "explain select 1 from person where not exists (select 1 from customer where customer.person_id = person.person_id)",
    );
    t.comment("single_join");
    t.ok(
        "explain select (select name from store where store.store_id = customer.customer_id and store.name like 'A%'), (select first_name from person where person.person_id = customer.person_id) from customer",
    );
    t.comment("remove_single_join");
    t.ok("explain select (select 1) from customer");
    t.comment("remove_single_join_column");
    t.ok("explain select (select customer_id) from customer");
    t.comment("nested_loop");
    t.ok("explain select customer.customer_id, store.store_id from customer, store");
    t.comment("equi_join");
    t.ok(
        "explain select customer.customer_id, store.store_id from customer, store where customer.store_id = store.store_id",
    );
    t.comment("equi_full_outer_join");
    t.ok(
        "explain select customer.customer_id, store.store_id from customer full outer join store on customer.store_id = store.store_id",
    );
    t.comment("join_left_index_scan");
    t.ok(
        "explain select customer.customer_id, store.store_id from customer, store where customer.customer_id = 1",
    );
    t.comment("join_right_index_scan");
    t.ok(
        "explain select customer.customer_id, store.store_id from customer, store where store.store_id = 1",
    );
    t.comment("equi_join_left_index_scan");
    t.ok(
        "explain select customer.customer_id, store.store_id from customer, store where customer.store_id = store.store_id and customer.customer_id = 1",
    );
    t.comment("equi_join_right_index_scan");
    t.ok(
        "explain select customer.customer_id, store.store_id from customer, store where customer.store_id = store.store_id and store.store_id = 1",
    );
    t.comment("left_equi_join_right_index_scan");
    t.ok(
        "explain select customer.customer_id, store.store_id from customer left join store on customer.store_id = store.store_id where store.store_id = 1",
    );
    t.comment("right_equi_join_right_index_scan");
    t.ok(
        "explain select customer.customer_id, store.store_id from customer right join store on customer.store_id = store.store_id where store.store_id = 1",
    );
    t.comment("two_inner_joins");
    t.ok(
        "explain select * from person join customer using (person_id) join store using (store_id)",
    );
    t.comment("remove_inner_join");
    t.ok("explain select *  from (select 1 as x) join (select 1 as y) on x = y");
    t.finish("examples/optimize_join.testlog");
}

#[test]
fn test_limit() {
    let mut t = TestSuite::adventure_works();
    t.comment("limit_offset");
    t.ok("explain select customer_id from customer limit 100 offset 10");
    t.finish("examples/optimize_limit.testlog");
}

#[test]
fn test_set() {
    let mut t = TestSuite::adventure_works();
    t.comment("union_all");
    t.ok("explain select 1 as a union all select 2 as a");
    t.finish("examples/optimize_set.testlog");
}

#[test]
fn test_sort() {
    let mut t = TestSuite::adventure_works();
    t.comment("order_by");
    t.ok("explain select customer_id from customer order by modified_date");
    t.finish("examples/optimize_sort.testlog");
}

#[test]
fn test_with() {
    let mut t = TestSuite::adventure_works();
    t.comment("redundant_with_clause_with_projection");
    t.ok(
        "explain with foo as (select customer_id, 1 as r from customer) select customer_id, r from foo",
    );
    t.comment("redundant_with_clause");
    t.ok("explain with foo as (select * from customer) select customer_id from foo");
    t.comment("remove_with");
    t.ok("explain with foo as (select * from customer) select * from foo");
    t.comment("unused_with");
    t.ok("explain with foo as (select 1 as a) select 2 as b");
    t.comment("use_with_clause_twice");
    t.ok(
        "explain with foo as (select customer_id, store_id from customer) select f1.customer_id, f2.customer_id from foo f1, foo f2 where f1.store_id = f2.store_id",
    );
    t.comment("use_with_project_twice");
    t.ok(
        "explain with foo as (select *, 1 as r from customer) select f1.customer_id, f2.customer_id from foo f1, foo f2 where f1.r = f2.r",
    );
    t.comment("use_with_select_star_twice");
    t.ok(
        "explain with foo as (select * from customer) select f1.customer_id, f2.customer_id from foo f1, foo f2 where f1.store_id = f2.store_id",
    );
    t.finish("examples/optimize_with.testlog");
}

#[test]
fn test_distinct() {
    let mut t = TestSuite::empty();
    t.setup("create table integers (x int64)");
    t.setup("insert into integers values (1), (1), (2), (3), (null)");
    t.ok("explain select count(distinct x) from integers");
    t.ok("explain select count(distinct x), count(distinct div(x, 2)) from integers");
    t.ok("explain select div(x, 2), count(distinct x) from integers group by 1");
    t.ok("explain select div(x, 2), count(distinct x), count(distinct div(x, 2)) from integers group by 1");
    t.finish("examples/optimize_distinct.testlog");
}

#[test]
fn test_enforcers() {
    let mut t = TestSuite::empty();
    t.setup("create table integers (x int64)");
    t.setup("insert into integers values (1), (1), (2), (3), (null)");
    t.ok("explain select x, count(x) from integers group by 1");
    t.ok("explain select * from integers i1 join integers i2 on i1.x = i2.x + 1");
    t.ok("explain select * from integers i1 join integers i2 on i1.x > i2.x");
    t.finish("examples/optimize_enforcers.testlog");
}