use statistics::Statistics;
use storage::Storage;

use crate::test_suite::*;

#[test]
fn test_join_using() {
    let mut t = TestSuite::new(Storage::default(), Statistics::default());
    t.ok("select * from (select 1 as a) as t1 join (select 1 as a) as t2 using (a)");
    t.finish("examples/execute_join_using.testlog");
}

#[test]
fn test_builtin_function() {
    let mut t = TestSuite::new(Storage::default(), Statistics::default());
    t.ok("with t as (select 1 as a, 1 as b) select a = b from t");
    t.finish("examples/execute_builtin_function.testlog");
}

#[test]
fn test_literals() {
    let mut t = TestSuite::new(Storage::default(), Statistics::default());
    t.ok("select true, 1, 1.0, date '2020-01-01', timestamp '2020-01-01', 'foo'");
    t.finish("examples/execute_literals.testlog");
}

#[test]
fn test_aggregate_functions() {
    let mut t = TestSuite::new(Storage::default(), Statistics::default());
    t.setup("create table booleans (x boolean);");
    t.setup("create table integers (x int64);");
    t.setup("create table floats (x float64);");
    t.setup("create table dates (x date);");
    t.setup("create table timestamps (x timestamp);");
    // TODO strings?
    t.setup("insert into booleans values (true), (true), (false), (null);");
    t.setup("insert into integers values (1), (1), (2), (3), (null);");
    t.setup("insert into floats values (1.0), (2.0), (3.0), (null);");
    t.setup("insert into dates values (date '2000-01-01'), (date '2000-01-02'), (date '2000-01-03'), (null);");
    t.setup("insert into timestamps values (timestamp '2000-01-01'), (timestamp '2000-01-02'), (timestamp '2000-01-03'), (null);");
    t.comment("simple aggregate counts");
    t.ok("select any_value(x), count(x), count(*) from integers");
    t.comment("simple aggregate boolean");
    t.ok("select logical_and(x), logical_or(x), max(x), min(x) from booleans");
    t.comment("simple aggregate int");
    t.ok("select max(x), min(x), sum(x) from integers");
    t.comment("simple aggregate float");
    t.ok("select max(x), min(x), sum(x) from floats");
    t.comment("group by aggregate");
    t.ok("select x, sum(x) from integers group by 1 order by 1");
    t.comment("simple avg");
    t.ok("select avg(x) from integers");
    t.comment("projection");
    t.ok("select x from booleans group by x order by x");
    t.ok("select x from integers group by x order by x");
    t.ok("select x from dates group by x order by x");
    t.ok("select x from timestamps group by x order by x");
    t.ok("select booleans.x, integers.x from booleans, integers group by 1, 2 order by 1, 2");
    t.ok("select distinct booleans.x, integers.x from booleans, integers order by 1, 2");
    t.finish("examples/execute_aggregates.testlog");
}

#[test]
fn test_parameters() {
    let mut t = TestSuite::new(Storage::default(), Statistics::default());
    t.ok("set parameter = 1; select @parameter");
    t.finish("examples/execute_parameters.testlog");
}

#[test]
fn test_metadata() {
    let mut t = TestSuite::new(Storage::default(), Statistics::default());
    t.comment("Catalog queries");
    t.ok("select parent_catalog_id, catalog_id, catalog_name from metadata.catalog");
    t.ok("select catalog_id, table_id, table_name, column_id, column_name, column_type from metadata.table join metadata.column using (table_id) order by catalog_id, table_id, column_id");
    t.ok("select index_id, table_id, column_name from metadata.index join metadata.index_column using (index_id) join metadata.column using (table_id, column_id) order by index_id, index_order");
    t.comment("DDL implementation queries");
    t.ok("select sequence_id from metadata.sequence where sequence_name = 'table'");
    t.finish("examples/execute_metadata.testlog");
}

#[test]
fn test_aggregate_large() {
    let (storage, statistics) = crate::adventure_works();
    let mut t = TestSuite::new(storage, statistics);
    t.ok("select count(*) from person");
    t.ok("select count(*) from customer");
    t.finish("examples/execute_aggregate_large.testlog");
}

#[test]
fn test_ddl() {
    let mut t = TestSuite::new(Storage::default(), Statistics::default());
    t.comment("create table");
    t.setup("create table foo (id int64);");
    t.setup("insert into foo values (1)");
    t.ok("select * from foo;");
    t.comment("drop table");
    t.setup("drop table foo;");
    t.setup("create table foo (id int64);");
    t.ok("select * from foo;");
    t.comment("create index");
    t.ok("create index foo_id_index on foo (id);");
    t.comment("drop index");
    t.ok("drop index foo_id;");
    t.comment("create database");
    t.ok("create database foo;");
    t.comment("create table");
    t.setup("create table foo.bar (id int64);");
    t.setup("insert into foo.bar values (1);");
    t.ok("select * from foo.bar;");
    t.comment("drop database");
    t.setup("drop database foo;");
    t.setup("create database foo;");
    t.setup("create table foo.bar (id int64); create table foo.doh (id int64);");
    t.ok("select * from foo.bar union all select * from foo.doh;");
    t.finish("examples/execute_ddl.testlog");
}

#[test]
fn test_dml() {
    let mut t = TestSuite::new(Storage::default(), Statistics::default());
    t.setup("create table foo (id int64);");
    t.setup("insert into foo values (1);");
    t.ok("select * from foo;");
    t.setup("insert into foo values (1);");
    t.setup("delete from foo where id = 1;");
    t.ok("select * from foo;");
    t.setup("insert into foo values (1);");
    t.setup("delete from foo where id = 1;");
    t.setup("insert into foo values (2);");
    t.ok("select * from foo;");
    t.setup("insert into foo values (1);");
    t.setup("update foo set id = 2 where id = 1;");
    t.ok("select * from foo;");
    t.setup("drop table foo;");
    t.setup("create table foo (id int64, ok bool);");
    t.setup("insert into foo (id, ok) values (1, false);");
    t.setup("insert into foo (ok, id) values (true, 2);");
    t.ok("select * from foo;");
    t.finish("examples/execute_dml.testlog");
}

#[test]
fn test_update_index() {
    let mut t = TestSuite::new(Storage::default(), Statistics::default());
    t.setup("create table foo (id int64);");
    t.setup("create index foo_id_index on foo (id);");
    let values: Vec<_> = (0..1000).map(|i| format!("({})", i)).collect();
    t.setup(format!("insert into foo values {};", values.join(", ")).as_str());
    t.ok("update foo set id = -1 where id = 500");
    t.ok("explain select * from foo where id = -1");
    t.ok("select * from foo where id = -1");
    t.ok("select * from foo where id = 500");
    t.finish("examples/execute_update_index.testlog");
}

#[test]
fn test_index() {
    let (storage, statistics) = crate::adventure_works();
    let mut t = TestSuite::new(storage, statistics);
    t.ok("select * from customer where customer_id = 1");
    t.ok("select * from customer join store using (store_id) where customer_id = 1");
    t.finish("examples/execute_index.testlog");
}

#[test]
fn test_join_nested_loop() {
    let mut t = TestSuite::new(Storage::default(), Statistics::default());
    t.setup("create table foo (id int64); create table bar (id int64);");
    t.setup("insert into foo values (1), (2); insert into bar values (2), (3), (4);");
    t.ok("select foo.id as foo_id, bar.id as bar_id from foo left join bar using (id)");
    t.ok("select foo.id as foo_id, bar.id as bar_id from foo right join bar using (id)");
    t.ok("select foo.id as foo_id, bar.id as bar_id from foo full join bar using (id)");
    t.ok("select id from foo where id in (select id from bar)");
    t.ok("select id from foo where id not in (select id from bar)");
    t.ok("select id, exists(select id from bar where foo.id = bar.id) from foo");
    t.finish("examples/execute_join_nested_loop.testlog");
}

#[test]
fn test_join_hash() {
    let (storage, statistics) = crate::adventure_works();
    let mut t = TestSuite::new(storage, statistics);
    t.comment("hash inner join");
    t.ok(
        "select count(person.person_id), count(customer.person_id) from person join customer using (person_id)"
    );
    t.comment("hash left join");
    t.ok(
        "select count(person.person_id), count(customer.person_id) from person left join customer using (person_id)"
    );
    t.comment("hash right join");
    t.ok(
        "select count(person.person_id), count(customer.person_id) from person right join customer using (person_id)"
    );
    t.comment("hash outer join");
    t.ok(
        "select count(person.person_id), count(customer.person_id) from person full join customer using (person_id)"
    );
    t.comment("hash semi join");
    t.ok("select count(*) from person where person_id in (select person_id from customer)");
    t.comment("hash anti join");
    t.ok("select count(*) from person where person_id not in (select person_id from customer)");
    t.comment("hash mark join");
    t.ok(
        "select exists(select 1 from customer where customer.person_id = person.person_id), count(*) from person group by 1 order by 1"
    );
    t.finish("examples/execute_join_hash.testlog");
}

#[test]
fn test_limit() {
    let mut t = TestSuite::new(Storage::default(), Statistics::default());
    t.setup("create table foo (b bool, i int64, s string);");
    t.setup("insert into foo values (false, 1, 'one'), (true, 2, 'two'), (false, 3, 'three');");
    t.ok("select * from foo limit 1");
    t.ok("select * from foo limit 1 offset 1");
    t.finish("examples/execute_limit.testlog");
}

#[test]
fn test_set() {
    let mut t = TestSuite::new(Storage::default(), Statistics::default());
    t.ok("select 1 as x union all select 2 as x");
    t.finish("examples/execute_set.testlog");
}

#[test]
fn test_with() {
    let mut t = TestSuite::new(Storage::default(), Statistics::default());
    t.ok("with foo as (select 1 as bar) select * from foo union all select * from foo");
    t.finish("examples/execute_with.testlog");
}

#[test]
fn test_correlated_exists() {
    let mut t = TestSuite::new(Storage::default(), Statistics::default());
    t.setup("create table integers (i int64);");
    t.setup("insert into integers values (1), (2), (3), (null);");
    t.comment("correlated EXISTS");
    t.ok("SELECT i, EXISTS(SELECT i FROM integers WHERE i1.i>2) FROM integers i1 ORDER BY i;");
    t.ok("SELECT i, EXISTS(SELECT i FROM integers WHERE i=i1.i) FROM integers i1 ORDER BY i;");
    t.ok("SELECT i, EXISTS(SELECT i FROM integers WHERE i IS NULL OR i>i1.i*10) FROM integers i1 ORDER BY i;");
    t.ok("SELECT i, EXISTS(SELECT i FROM integers WHERE i1.i>i OR i1.i IS NULL) FROM integers i1 ORDER BY i;");
    t.ok("SELECT i FROM integers i1 WHERE EXISTS(SELECT i FROM integers WHERE i=i1.i) ORDER BY i;");
    t.comment("GROUP BY correlated exists");
    t.ok("SELECT EXISTS(SELECT i FROM integers WHERE i=i1.i) AS g, COUNT(*) FROM integers i1 GROUP BY g ORDER BY g;");
    t.comment("SUM on exists");
    t.ok("SELECT SUM(CASE WHEN EXISTS(SELECT i FROM integers WHERE i=i1.i) THEN 1 ELSE 0 END) FROM integers i1;");
    t.comment("aggregates with multiple parameters");
    t.ok("SELECT (SELECT SUM(i1.i * i2.i) FROM integers i2) FROM integers i1 ORDER BY 1");
    t.ok("SELECT (SELECT SUM(i2.i * i1.i) FROM integers i2) FROM integers i1 ORDER BY 1");
    t.ok("SELECT (SELECT SUM((i1.i+i2.i) * (i1.i+i2.i)) FROM integers i2) FROM integers i1 ORDER BY 1");
    t.ok("SELECT (SELECT SUM(i2.i * i2.i) FROM integers i2) FROM integers i1 ORDER BY 1;");
    t.ok("SELECT (SELECT SUM(i1.i * i1.i) FROM integers i2 LIMIT 1) FROM integers i1 ORDER BY 1;");
    t.finish("examples/execute_correlated_exists.testlog");
}

#[test]
fn test_complex_correlated_subquery() {
    let mut t = TestSuite::new(Storage::default(), Statistics::default());
    t.setup("create table integers (i int64);");
    t.setup("insert into integers values (1), (2), (3), (null);");
    t.comment("correlated expression in subquery");
    t.ok("SELECT i, (SELECT s1.i FROM (SELECT * FROM integers WHERE i=i1.i) s1) AS j FROM integers i1 ORDER BY i;");
    t.comment("join on two subqueries that both have a correlated expression in them");
    t.ok("SELECT i, (SELECT s1.i FROM (SELECT i FROM integers WHERE i=i1.i) s1 INNER JOIN (SELECT i FROM integers WHERE i=4-i1.i) s2 ON s1.i>s2.i) AS j FROM integers i1 ORDER BY i;");
    t.comment("implicit join with correlated expression in filter");
    t.ok("SELECT i, (SELECT s1.i FROM integers s1, integers s2 WHERE s1.i=s2.i AND s1.i=4-i1.i) AS j FROM integers i1 ORDER BY i;");
    t.comment("join with a correlated expression in the join condition");
    t.ok("SELECT i, (SELECT s1.i FROM integers s1 INNER JOIN integers s2 ON s1.i=s2.i AND s1.i=4-i1.i) AS j FROM integers i1 ORDER BY i;");
    t.comment("inner join on correlated subquery");
    t.ok("SELECT * FROM integers s1 INNER JOIN integers s2 ON (SELECT 2*SUM(i)*s1.i FROM integers)=(SELECT SUM(i)*s2.i FROM integers) ORDER BY s1.i;");
    t.comment("inner join on non-equality subquery");
    t.ok("SELECT * FROM integers s1 INNER JOIN integers s2 ON (SELECT s1.i=s2.i) ORDER BY s1.i;");
    t.ok("SELECT * FROM integers s1 INNER JOIN integers s2 ON (SELECT s1.i=i FROM integers WHERE s2.i=i) ORDER BY s1.i;");
    // t.comment("left outer join on comparison between correlated subqueries");
    // t.ok("SELECT * FROM integers s1 LEFT OUTER JOIN integers s2 ON (SELECT 2*SUM(i)*s1.i FROM integers)=(SELECT SUM(i)*s2.i FROM integers) ORDER BY s1.i;");
    // t.comment("left outer join on arbitrary correlated subquery: not supported");
    // t.error("SELECT * FROM integers s1 LEFT OUTER JOIN integers s2 ON (SELECT CASE WHEN s1.i+s2.i>10 THEN TRUE ELSE FALSE END) ORDER BY s1.i;");
    // t.comment("left outer join on subquery only involving RHS works");
    // t.ok("SELECT * FROM integers s1 LEFT OUTER JOIN integers s2 ON s1.i=s2.i AND (SELECT CASE WHEN s2.i>2 THEN TRUE ELSE FALSE END) ORDER BY s1.i;");
    // t.comment("left outer join on subquery only involving LHS is not supported");
    // t.error("SELECT * FROM integers s1 LEFT OUTER JOIN integers s2 ON s1.i=s2.i AND (SELECT CASE WHEN s1.i>2 THEN TRUE ELSE FALSE END) ORDER BY s1.i;");
    // t.comment("left outer join in correlated expression");
    // t.error("SELECT i, (SELECT SUM(s1.i) FROM integers s1 LEFT OUTER JOIN integers s2 ON s1.i=s2.i OR s1.i=i1.i-1) AS j FROM integers i1 ORDER BY i;");
    // t.comment("full outer join: both sqlite and postgres actually cannot run this one");
    // t.error("SELECT i, (SELECT SUM(s1.i) FROM integers s1 FULL OUTER JOIN integers s2 ON s1.i=s2.i OR s1.i=i1.i-1) AS j FROM integers i1 ORDER BY i;");
    // t.comment("correlated expression inside window function not supported");
    // t.error("SELECT i, (SELECT row_number() OVER (ORDER BY i)) FROM integers i1 ORDER BY i;");
    t.comment("union with correlated expression");
    t.ok("SELECT i, EXISTS (SELECT i FROM integers WHERE i=i1.i UNION ALL SELECT i FROM integers WHERE i=i1.i) AS j FROM integers i1 ORDER BY i;");
    // t.comment("except with correlated expression");
    // t.ok("SELECT i, (SELECT i FROM integers WHERE i IS NOT NULL EXCEPT ALL SELECT i FROM integers WHERE i<>i1.i) AS j FROM integers i1 WHERE i IS NOT NULL ORDER BY i;");
    // t.comment("intersect with correlated expression");
    // t.ok("SELECT i, (SELECT i FROM integers WHERE i=i1.i INTERSECT ALL SELECT i FROM integers WHERE i=i1.i) AS j FROM integers i1 ORDER BY i;");
    // t.comment("multiple setops");
    // t.ok("SELECT i, (SELECT i FROM integers WHERE i=i1.i UNION ALL SELECT i FROM integers WHERE i<>i1.i EXCEPT ALL SELECT i FROM integers WHERE i<>i1.i) AS j FROM integers i1 ORDER BY i;");
    t.comment("uncorrelated query inside correlated query");
    t.ok("SELECT i, (SELECT (SELECT SUM(i) FROM integers)+42+i1.i) AS j FROM integers i1 ORDER BY i;");
    t.finish("examples/execute_complex_correlated_subquery.testlog");
}

#[test]
fn test_correlated_subquery() {
    let mut t = TestSuite::new(Storage::default(), Statistics::default());
    t.setup("create table integers (i int64);");
    t.setup("insert into integers values (1), (2), (3), (null);");
    t.comment("scalar select with correlation");
    t.ok("SELECT i, (SELECT 42+i1.i) AS j FROM integers i1 ORDER BY i;");
    t.comment("ORDER BY correlated subquery");
    t.ok("SELECT i FROM integers i1 ORDER BY (SELECT 100-i1.i);");
    t.comment("subquery returning multiple results");
    t.ok("SELECT i, (SELECT any_value(42+i1.i) FROM integers) AS j FROM integers i1 ORDER BY i;");
    t.comment("subquery with LIMIT");
    t.ok("SELECT i, (SELECT 42+i1.i FROM integers LIMIT 1) AS j FROM integers i1 ORDER BY i;");
    t.comment("subquery with LIMIT 0");
    t.ok("SELECT i, (SELECT 42+i1.i FROM integers LIMIT 0) AS j FROM integers i1 ORDER BY i;");
    t.comment("subquery with WHERE clause that is always FALSE");
    t.ok(
        "SELECT i, (SELECT i FROM integers WHERE 1=0 AND i1.i=i) AS j FROM integers i1 ORDER BY i;",
    );
    t.comment("correlated EXISTS with WHERE clause that is always FALSE");
    t.ok(
        "SELECT i, EXISTS(SELECT i FROM integers WHERE 1=0 AND i1.i=i) AS j FROM integers i1 ORDER BY i;",
    );
    t.comment("correlated ANY with WHERE clause that is always FALSE");
    t.ok(
        "SELECT i, i IN (SELECT i FROM integers WHERE 1=0 AND i1.i=i) AS j FROM integers i1 ORDER BY i;",
    );
    // t.comment("subquery with OFFSET is not supported");
    // t.error(
    //     "SELECT i, (SELECT i+i1.i FROM integers LIMIT 1 OFFSET 1) AS j FROM integers i1 ORDER BY i;",
    // );
    // t.comment("subquery with ORDER BY is not supported");
    // t.error(
    //     "SELECT i, (SELECT i+i1.i FROM integers ORDER BY 1 LIMIT 1 OFFSET 1) AS j FROM integers i1 ORDER BY i;",
    // );
    // t.comment("correlated filter without FROM clause");
    // t.error("SELECT i, (SELECT 42 WHERE i1.i>2) AS j FROM integers i1 ORDER BY i;");
    // t.comment("correlated filter with matching entry on NULL");
    // t.error("SELECT i, (SELECT 42 WHERE i1.i IS NULL) AS j FROM integers i1 ORDER BY i;");
    t.comment("scalar select with correlation in projection");
    t.ok("SELECT i, (SELECT i+i1.i FROM integers WHERE i=1) AS j FROM integers i1 ORDER BY i;");
    t.comment("scalar select with correlation in filter");
    t.ok("SELECT i, (SELECT i FROM integers WHERE i=i1.i) AS j FROM integers i1 ORDER BY i;");
    t.comment("scalar select with operation in projection");
    t.ok("SELECT i, (SELECT i+1 FROM integers WHERE i=i1.i) AS j FROM integers i1 ORDER BY i;");
    t.comment("correlated scalar select with constant in projection");
    t.ok("SELECT i, (SELECT 42 FROM integers WHERE i=i1.i) AS j FROM integers i1 ORDER BY i;");
    t.finish("examples/execute_correlated_subquery.testlog");
}

#[test]
fn test_subquery_join() {
    let mut t = TestSuite::new(Storage::default(), Statistics::default());
    t.setup("create table integers (i int64);");
    t.setup("insert into integers values (1), (2), (3), (null);");
    t.ok("select i, exists (select * from (select * from integers where i = i3.i + 1) i1 join integers i2 using (i)) from integers i3 order by i");
    t.ok("select i, exists (select * from (select * from integers where i = i3.i + 1) i1 left join integers i2 using (i)) from integers i3 order by i");
    t.ok("select i, exists (select * from (select * from integers where i = i3.i + 1) i1 right join integers i2 using (i)) from integers i3 order by i");
    t.ok("select i, exists (select * from (select * from integers where i = i3.i + 1) i1 full outer join integers i2 using (i)) from integers i3 order by i");
    t.ok("select i, exists (select * from (select * from integers where i = i3.i + 1) i1 full outer join (select * from integers where i = i3.i + 1) i2 using (i)) from integers i3 order by i");
    t.finish("examples/execute_subquery_join.testlog");
}

#[test]
fn test_explain() {
    let mut t = TestSuite::new(Storage::default(), Statistics::default());
    t.ok("explain select 1");
    t.finish("examples/execute_explain.testlog");
}
