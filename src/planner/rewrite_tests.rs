use crate::rewrite::*;
use fixtures::*;

macro_rules! ok {
    ($path:expr, $sql:expr, $errors:expr) => {
        let mut parser = parser::ParseProvider::new();
        let sql = $sql.to_string();
        let (_, expr) = parser.parse(&sql, 0, &adventure_works()).unwrap();
        let expr = rewrite(expr);
        let found = format!("{}\n\n{}", &sql, expr);
        if !matches_expected(&$path.to_string(), found) {
            $errors.push($path.to_string());
        }
    };
}

#[test]
#[rustfmt::skip]
fn test_rewrite() {
    let mut errors = vec![];
    ok!("examples/rewrite/semi_join.txt", r#"select 1 from person where exists (select 1 from customer where customer.person_id = person.person_id)"#, errors);
    ok!("examples/rewrite/single_join.txt", r#"select (select name from store where store.store_id = customer.customer_id and store.name like "A%"), (select first_name from person where person.person_id = customer.person_id) from customer"#, errors);
    ok!("examples/rewrite/combine_consecutive_filters.txt", r#"select 1 from (select * from person where first_name like "A%") where last_name like "A%""#, errors);
    ok!("examples/rewrite/combine_consecutive_projects.txt", r#"select a + 1 as b from (select 1 as a)"#, errors);
    ok!("examples/rewrite/combine_consecutive_projects_star.txt", r#"select *, a + 1 as b from (select 1 as a)"#, errors);
    ok!("examples/rewrite/pull_filter_through_aggregate.txt", r#"select store_id, (select count(*) from customer where customer.store_id = store.store_id) from store"#, errors);
    ok!("examples/rewrite/remove_single_join.txt", r#"select (select 1) from customer"#, errors);
    ok!("examples/rewrite/remove_single_join_column.txt", r#"select (select customer_id) from customer"#, errors);
    ok!("examples/rewrite/remove_with.txt", r#"with foo as (select * from customer) select * from foo"#, errors);
    ok!("examples/rewrite/unused_with.txt", r#"with foo as (select 1 as a) select 2 as b"#, errors);
    ok!("examples/rewrite/push_filter_through_project.txt", r#"select * from (select *, store_id + 1 from customer) where store_id = 1"#, errors);
    if !errors.is_empty() {
        panic!("{:#?}", errors);
    }
}
