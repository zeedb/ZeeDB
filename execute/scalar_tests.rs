use ast::Value;
use chrono::{NaiveDate, TimeZone, Utc};
use kernel::{AnyArray, Array};
use storage::Storage;

#[test]
fn test_math_i64() {
    let cases = vec![
        ("1 + 1", Some(2)),
        ("1 + cast(null as int64)", None),
        ("2 * 3", Some(6)),
        ("2 * cast(null as int64)", None),
        ("1 - 2", Some(-1)),
        ("1 - cast(null as int64)", None),
        ("-1", Some(-1)),
        ("-cast(null as int64)", None),
        ("abs(-1)", Some(1)),
        ("sign(-10)", Some(-1)),
    ];
    for (sql, expect) in cases {
        assert_eq!(Value::I64(expect), eval(sql), "{}", sql);
    }
}

#[test]
fn test_math_f64() {
    let cases = vec![
        ("1.0 + 1.0", Some(2.0)),
        ("1.0 + cast(null as float64)", None),
        ("2.0 * 3.0", Some(6.0)),
        ("2.0 * cast(null as float64)", None),
        ("1.0 - 2.0", Some(-1.0)),
        ("1.0 - cast(null as float64)", None),
        ("-1.0", Some(-1.0)),
        ("-cast(null as float64)", None),
        ("abs(-1.0)", Some(1.0)),
        ("abs(cast(null as float64))", None),
        ("sign(-10.0)", Some(-1.0)),
        ("sign(cast(null as float64))", None),
        ("round(-1.4)", Some(-1.0)),
        ("round(-1.5)", Some(-2.0)),
        ("round(-1.6)", Some(-2.0)),
        ("round(cast(null as float64))", None),
        ("round(-1.14, 1)", Some(-1.1)),
        ("round(-1.15, 1)", Some(-1.2)),
        ("round(-1.16, 1)", Some(-1.2)),
        ("round(cast(null as float64))", None),
        ("trunc(-1.4)", Some(-1.0)),
        ("trunc(-1.5)", Some(-1.0)),
        ("trunc(-1.6)", Some(-1.0)),
        ("trunc(-1.14, 1)", Some(-1.1)),
        ("trunc(-1.15, 1)", Some(-1.1)),
        ("trunc(-1.16, 1)", Some(-1.1)),
        ("trunc(cast(null as float64))", None),
        ("ceil(-1.4)", Some(-1.0)),
        ("ceil(-1.5)", Some(-1.0)),
        ("ceil(-1.6)", Some(-1.0)),
        ("ceil(cast(null as float64))", None),
        ("floor(-1.4)", Some(-2.0)),
        ("floor(-1.5)", Some(-2.0)),
        ("floor(-1.6)", Some(-2.0)),
        ("floor(cast(null as float64))", None),
        ("sqrt(4.0)", Some(2.0)),
        ("sqrt(cast(null as float64))", None),
        ("pow(2.0, 2.0)", Some(4.0)),
        ("pow(cast(null as float64), 2.0)", None),
        ("pow(2.0, cast(null as float64))", None),
        ("exp(0.0)", Some(1.0)),
        ("exp(cast(null as float64))", None),
        ("ln(exp(0.0))", Some(0.0)),
        ("ln(cast(null as float64))", None),
        ("log10(10.0)", Some(1.0)),
        ("log10(cast(null as float64))", None),
        ("log(exp(0.0))", Some(0.0)),
        ("log(cast(null as float64))", None),
        ("log(4.0, 2.0)", Some(2.0)),
        ("log(cast(null as float64), 2.0)", None),
        ("cos(0.5)", Some(f64::cos(0.5))),
        ("cos(cast(null as float64))", None),
        ("cosh(0.5)", Some(f64::cosh(0.5))),
        ("cosh(cast(null as float64))", None),
        ("acos(0.5)", Some(f64::acos(0.5))),
        ("acos(cast(null as float64))", None),
        ("acosh(100.0)", Some(f64::acosh(100.0))),
        ("acosh(cast(null as float64))", None),
        ("sin(0.5)", Some(f64::sin(0.5))),
        ("sin(cast(null as float64))", None),
        ("sinh(0.5)", Some(f64::sinh(0.5))),
        ("sinh(cast(null as float64))", None),
        ("asin(0.5)", Some(f64::asin(0.5))),
        ("asin(cast(null as float64))", None),
        ("asinh(100.0)", Some(f64::asinh(100.0))),
        ("asinh(cast(null as float64))", None),
        ("tan(0.5)", Some(f64::tan(0.5))),
        ("tan(cast(null as float64))", None),
        ("tanh(0.5)", Some(f64::tanh(0.5))),
        ("tanh(cast(null as float64))", None),
        ("atan(0.5)", Some(f64::atan(0.5))),
        ("atan(cast(null as float64))", None),
        ("atanh(0.5)", Some(f64::atanh(0.5))),
        ("atanh(cast(null as float64))", None),
        ("atan2(0.5, 20.0)", Some(f64::atan2(0.5, 20.0))),
        ("atan2(cast(null as float64), 20.0)", None),
    ];
    for (sql, expect) in cases {
        assert_eq!(Value::F64(expect), eval(sql), "{}", sql);
    }
}

#[test]
fn test_string_bool_functions() {
    let cases = vec![
        ("'foobar' like 'foo%'", Some(true)),
        ("'foobar' like 'bar%'", Some(false)),
        ("cast(null as string) like 'foo%'", None),
        ("starts_with('foobar', 'foo')", Some(true)),
        ("starts_with('foobar', 'bar')", Some(false)),
        ("starts_with(null, 'foo')", None),
        ("starts_with('foobar', null)", None),
        ("ends_with('foobar', 'foo')", Some(false)),
        ("ends_with('foobar', 'bar')", Some(true)),
        ("ends_with(null, 'foo')", None),
        ("ends_with('foobar', null)", None),
        (
            r"regexp_contains('foo@example.com', r'@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+')",
            Some(true),
        ),
        (
            r"regexp_contains('www.example.net', r'@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+')",
            Some(false),
        ),
        (
            r"regexp_contains(null, r'@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+')",
            None,
        ),
    ];
    for (sql, expect) in cases {
        assert_eq!(Value::Bool(expect), eval(sql), "{}", sql);
    }
}

#[test]
fn test_string_i64_functions() {
    let cases = vec![
        ("strpos('foobar', 'bar')", Some(4)),
        ("length('foobar')", Some(6)),
        ("length(null)", None),
        ("byte_length('абвгд')", Some(10)),
        ("byte_length('foo')", Some(3)),
        ("byte_length(null)", None),
        ("char_length('абвгд')", Some(5)),
        ("char_length('foo')", Some(3)),
        ("char_length(null)", None),
        ("strpos(null, 'bar')", None),
        ("strpos('foobar', null)", None),
    ];
    for (sql, expect) in cases {
        assert_eq!(Value::I64(expect), eval(sql), "{}", sql);
    }
}

#[test]
fn test_string_string_functions() {
    let cases = vec![
        ("concat('foo', 'bar', 'baz')", Some("foobarbaz")),
        ("concat('foo', null)", None),
        ("lower('FOO')", Some("foo")),
        ("lower('foo')", Some("foo")),
        ("lower(null)", None),
        ("upper('FOO')", Some("FOO")),
        ("upper('foo')", Some("FOO")),
        ("upper(null)", None),
        ("substr('foobar', 4)", Some("bar")),
        ("substr('foobar', 4, 3)", Some("bar")),
        ("substr('foobar', 4, 10)", Some("bar")),
        ("substr('foobar', 10)", Some("")),
        ("substr(null, 3)", None),
        ("trim(' foo ')", Some("foo")),
        ("ltrim(' foo ')", Some("foo ")),
        ("rtrim(' foo ')", Some(" foo")),
        ("trim('xfooy', 'xyz')", Some("foo")),
        ("ltrim('xfooy', 'xyz')", Some("fooy")),
        ("rtrim('xfooy', 'xyz')", Some("xfoo")),
        ("replace('foobar', 'bar', 'baz')", Some("foobaz")),
        (
            r"regexp_extract('foo@bar.com', r'^[a-zA-Z0-9_.+-]+')",
            Some("foo"),
        ),
        (
            r"regexp_replace('# Heading', r'^# ([a-zA-Z0-9\s]+$)', '<h1>\\1</h1>')",
            Some("<h1>Heading</h1>"),
        ),
        ("lpad('foo', 6)", Some("   foo")),
        ("lpad('foo', 6, 'bar')", Some("barfoo")),
        ("rpad('foo', 6)", Some("foo   ")),
        ("rpad('foo', 6, 'bar')", Some("foobar")),
        ("left('foobar', 3)", Some("foo")),
        ("left(null, 3)", None),
        ("left('foobar', null)", None),
        ("right('foobar', 3)", Some("bar")),
        ("right(null, 3)", None),
        ("right('foobar', null)", None),
        ("repeat('foo', 3)", Some("foofoofoo")),
        ("reverse('foo')", Some("oof")),
        ("chr(65)", Some("A")),
        ("chr(255)", Some("ÿ")),
        ("chr(513)", Some("ȁ")),
        ("chr(1024)", Some("Ѐ")),
        ("chr(null)", None),
    ];
    for (sql, expect) in cases {
        assert_eq!(
            Value::String(expect.map(|s| s.to_string())),
            eval(sql),
            "{}",
            sql
        );
    }
}

#[test]
fn test_date_functions() {
    let cases = vec![
        (
            "current_date() > date '2020-01-01'",
            Value::Bool(Some(true)),
        ),
        (
            "date_add(date '2020-01-01', interval 1 day)",
            date(2020, 1, 2),
        ),
        (
            "date_add(date '2020-01-01', interval 1 week)",
            date(2020, 1, 8),
        ),
        (
            "date_add(date '2020-01-01', interval 1 month)",
            date(2020, 2, 1),
        ),
        (
            "date_add(date '2020-01-01', interval 100 month)",
            date(2028, 5, 1),
        ),
        (
            "date_add(date '2020-01-01', interval 1 year)",
            date(2021, 1, 1),
        ),
        (
            "date_sub(date '2020-01-01', interval 1 day)",
            date(2019, 12, 31),
        ),
        (
            "date_sub(date '2020-01-01', interval 1 week)",
            date(2019, 12, 25),
        ),
        (
            "date_sub(date '2020-01-01', interval 1 month)",
            date(2019, 12, 1),
        ),
        (
            "date_sub(date '2020-01-01', interval 100 month)",
            date(2011, 9, 1),
        ),
        (
            "date_sub(date '2020-01-01', interval 1 year)",
            date(2019, 1, 1),
        ),
        (
            "date_diff(date '2020-01-01', date '2020-01-10', day)",
            int64(-9),
        ),
        ("date_trunc(date '2008-12-25', month)", date(2008, 12, 1)),
        ("date_from_unix_date(14238)", date(2008, 12, 25)),
        ("unix_date(date '2008-12-25')", int64(14238)),
        ("date(2020, 1, 1)", date(2020, 1, 1)),
        ("date(timestamp '2020-01-01')", date(2020, 1, 1)),
        ("date(2020, 1, cast(null as int64))", Value::Date(None)),
        ("extract(dayofweek from date '2008-12-25')", int64(5)),
        ("extract(day from date '2008-12-25')", int64(25)),
        ("extract(dayofyear from date '2008-12-25')", int64(360)),
        ("extract(week from date '2008-12-25')", int64(51)),
        ("extract(week(sunday) from date '2008-12-25')", int64(51)),
        ("extract(week(monday) from date '2008-12-25')", int64(51)),
        ("extract(isoweek from date '2008-12-25')", int64(52)),
        ("extract(month from date '2008-12-25')", int64(12)),
        ("extract(quarter from date '2008-12-25')", int64(4)),
        ("extract(year from date '2008-12-25')", int64(2008)),
        ("extract(isoyear from date '2008-12-25')", int64(2008)),
        ("format_date('%x', date '2008-12-25')", string("12/25/08")),
        (
            "format_date('%b-%d-%y', date '2008-12-25')",
            string("Dec-25-08"),
        ),
        ("format_date('%b %y', date '2008-12-25')", string("Dec 08")),
        ("format_date('%b %y', null)", Value::String(None)),
        ("parse_date('%F', '2008-12-25')", date(2008, 12, 25)),
        ("parse_date('%x', '12/25/08')", date(2008, 12, 25)),
        ("parse_date('%Y%m%d', '20081225')", date(2008, 12, 25)),
    ];
    for (sql, expect) in cases {
        assert_eq!(expect, eval(sql), "{}", sql);
    }
}

#[test]
fn test_timestamp_functions() {
    let cases = vec![
        (
            "current_timestamp() > timestamp '2020-01-01'",
            Value::Bool(Some(true)),
        ),
        (
            "timestamp_add(timestamp '2020-01-01', interval 1 day)",
            ts(2020, 01, 02, 00, 00, 00),
        ),
        (
            "timestamp_add(timestamp '2020-01-01', interval 1 hour)",
            ts(2020, 01, 01, 01, 00, 00),
        ),
        (
            "timestamp_add(timestamp '2020-01-01', interval 1 minute)",
            ts(2020, 01, 01, 00, 01, 00),
        ),
        (
            "timestamp_add(timestamp '2020-01-01', interval 1 second)",
            ts(2020, 01, 01, 00, 00, 01),
        ),
        (
            "timestamp_sub(timestamp '2020-01-01', interval 1 day)",
            ts(2019, 12, 31, 00, 00, 00),
        ),
        (
            "timestamp_sub(timestamp '2020-01-01', interval 1 hour)",
            ts(2019, 12, 31, 23, 00, 00),
        ),
        (
            "timestamp_sub(timestamp '2020-01-01', interval 1 minute)",
            ts(2019, 12, 31, 23, 59, 00),
        ),
        (
            "timestamp_sub(timestamp '2020-01-01', interval 1 second)",
            ts(2019, 12, 31, 23, 59, 59),
        ),
        (
            "timestamp_diff(timestamp '2020-01-01', timestamp '2020-01-10', hour)",
            int64(-216),
        ),
        (
            "timestamp_trunc(timestamp '2008-12-25 15:30:00+00', day)",
            ts(2008, 12, 25, 0, 0, 0),
        ),
        (
            "timestamp_from_unix_micros(1230163200000000)",
            ts(2008, 12, 25, 0, 0, 0),
        ),
        (
            "unix_micros(timestamp '2008-12-25 15:30:00+00')",
            int64(1230219000000000),
        ),
        (
            "timestamp(cast('2020-01-01T00:00:00+00:00' as string))",
            ts(2020, 1, 1, 0, 0, 0),
        ),
        ("timestamp(date '2020-01-01')", ts(2020, 1, 1, 0, 0, 0)),
        ("timestamp(cast(null as string))", Value::Timestamp(None)),
        ("extract(microsecond from timestamp '2008-12-25')", int64(0)),
        ("extract(millisecond from timestamp '2008-12-25')", int64(0)),
        ("extract(second from timestamp '2008-12-25')", int64(0)),
        ("extract(minute from timestamp '2008-12-25')", int64(0)),
        ("extract(hour from timestamp '2008-12-25')", int64(0)),
        ("extract(dayofweek from timestamp '2008-12-25')", int64(5)),
        ("extract(day from timestamp '2008-12-25')", int64(25)),
        ("extract(dayofyear from timestamp '2008-12-25')", int64(360)),
        ("extract(week from timestamp '2008-12-25')", int64(51)),
        (
            "extract(week(sunday) from timestamp '2008-12-25')",
            int64(51),
        ),
        (
            "extract(week(monday) from timestamp '2008-12-25')",
            int64(51),
        ),
        ("extract(isoweek from timestamp '2008-12-25')", int64(52)),
        ("extract(month from timestamp '2008-12-25')", int64(12)),
        ("extract(quarter from timestamp '2008-12-25')", int64(4)),
        ("extract(year from timestamp '2008-12-25')", int64(2008)),
        ("extract(isoyear from timestamp '2008-12-25')", int64(2008)),
        (
            "extract(date from timestamp '2008-12-25')",
            date(2008, 12, 25),
        ),
        (
            "string(timestamp '2008-12-25 15:30:00+00')",
            string("2008-12-25T15:30:00+00:00"),
        ),
        ("string(cast(null as timestamp))", Value::String(None)),
        (
            "format_timestamp('%b-%d-%y', timestamp '2008-12-25 15:30:00+00')",
            string("Dec-25-08"),
        ),
        (
            "format_timestamp('%b %y', timestamp '2008-12-25 15:30:00+00')",
            string("Dec 08"),
        ),
        ("format_timestamp('%b %y', null)", Value::String(None)),
        (
            "parse_timestamp('%c', 'Thu Dec 25 07:30:00 2008')",
            ts(2008, 12, 25, 7, 30, 00),
        ),
        ("parse_timestamp('%c', null)", Value::Timestamp(None)),
    ];
    for (sql, expect) in cases {
        assert_eq!(expect, eval(sql), "{}", sql);
    }
}

#[test]
fn test_comparisons() {
    let none = || Value::Bool(None);
    let cases = vec![
        ("true and true", Value::Bool(Some(true))),
        ("true and false", Value::Bool(Some(false))),
        ("true and cast(null as bool)", none()),
        ("1.0 / 2.0", float64(0.5)),
        ("2 > 1", Value::Bool(Some(true))),
        ("1 > 2", Value::Bool(Some(false))),
        ("1 > cast(null as int64)", none()),
        ("2 >= 1", Value::Bool(Some(true))),
        ("1 >= 2", Value::Bool(Some(false))),
        ("1 >= cast(null as int64)", none()),
        ("2 < 1", Value::Bool(Some(false))),
        ("1 < 2", Value::Bool(Some(true))),
        ("1 < cast(null as int64)", none()),
        ("2 <= 1", Value::Bool(Some(false))),
        ("1 <= 2", Value::Bool(Some(true))),
        ("1 <= cast(null as int64)", none()),
        ("1 = 1", Value::Bool(Some(true))),
        ("1 = 2", Value::Bool(Some(false))),
        ("1 = cast(null as int64)", none()),
        ("2 in (1, 2)", Value::Bool(Some(true))),
        ("10 in (1, 2)", Value::Bool(Some(false))),
        ("null in (1, 2)", none()),
        ("1 between 1 and 2", Value::Bool(Some(true))),
        ("2 between 1 and 2", Value::Bool(Some(true))),
        ("3 between 1 and 2", Value::Bool(Some(false))),
        ("null between 1 and 2", none()),
        ("1 is null", Value::Bool(Some(false))),
        ("cast(null as int64) is null", Value::Bool(Some(true))),
        ("true is true", Value::Bool(Some(true))),
        ("false is true", Value::Bool(Some(false))),
        ("cast(null as bool) is true", Value::Bool(Some(false))),
        ("true is false", Value::Bool(Some(false))),
        ("false is false", Value::Bool(Some(true))),
        ("cast(null as bool) is false", Value::Bool(Some(false))),
        ("not(true)", Value::Bool(Some(false))),
        ("not(false)", Value::Bool(Some(true))),
        ("not(cast(null as bool))", none()),
        ("1 <> 1", Value::Bool(Some(false))),
        ("1 <> 2", Value::Bool(Some(true))),
        ("1 <> cast(null as int64)", none()),
        ("false or true", Value::Bool(Some(true))),
        ("false or false", Value::Bool(Some(false))),
        ("false or cast(null as bool)", none()),
        ("true or cast(null as bool)", Value::Bool(Some(true))),
        ("is_inf(1.0)", Value::Bool(Some(false))),
        ("is_inf(cast('inf' as float64))", Value::Bool(Some(true))),
        ("is_inf(cast(null as float64))", none()),
        ("is_nan(1.0)", Value::Bool(Some(false))),
        ("is_nan(cast('NaN' as float64))", Value::Bool(Some(true))),
        ("is_nan(cast('inf' as float64))", Value::Bool(Some(false))),
        ("is_nan(cast(null as float64))", none()),
        ("greatest(true, false, null)", none()),
        ("greatest(true, false)", Value::Bool(Some(true))),
        ("greatest(1, 2, null)", Value::I64(None)),
        ("greatest(1, 2)", int64(2)),
        ("greatest(1.0, 2.0, null)", Value::F64(None)),
        ("greatest(1.0, 2.0)", float64(2.0)),
        (
            "greatest(date '2020-01-01', date '2020-01-02', null)",
            Value::Date(None),
        ),
        (
            "greatest(date '2020-01-01', date '2020-01-02')",
            date(2020, 1, 2),
        ),
        (
            "greatest(timestamp '2020-01-01', timestamp '2020-01-02', null)",
            Value::Timestamp(None),
        ),
        (
            "greatest(timestamp '2020-01-01', timestamp '2020-01-02')",
            ts(2020, 1, 2, 0, 0, 0),
        ),
        ("greatest('foo', 'bar', null)", Value::String(None)),
        ("greatest('foo', 'bar')", string("foo")),
        ("least(true, false, null)", none()),
        ("least(true, false)", Value::Bool(Some(false))),
        ("least(1, 2, null)", Value::I64(None)),
        ("least(1, 2)", int64(1)),
        ("least(1.0, 2.0, null)", Value::F64(None)),
        ("least(1.0, 2.0)", float64(1.0)),
        (
            "least(date '2020-01-01', date '2020-01-02', null)",
            Value::Date(None),
        ),
        (
            "least(date '2020-01-01', date '2020-01-02')",
            date(2020, 1, 1),
        ),
        (
            "least(timestamp '2020-01-01', timestamp '2020-01-02', null)",
            Value::Timestamp(None),
        ),
        (
            "least(timestamp '2020-01-01', timestamp '2020-01-02')",
            ts(2020, 1, 1, 0, 0, 0),
        ),
        ("least('foo', 'bar', null)", Value::String(None)),
        ("least('foo', 'bar')", string("bar")),
    ];
    for (sql, expect) in cases {
        assert_eq!(expect, eval(sql), "{}", sql);
    }
}

#[test]
fn test_control_flow() {
    let cases = vec![
        ("case when true then 1 else 2 end", int64(1)),
        ("case when false then 1 else 2 end", int64(2)),
        ("case when null then 1 else 2 end", int64(2)),
        (
            "case when false then 1 when false then 2 else 3 end",
            int64(3),
        ),
        ("case 1 when 1 then 'a' else 'b' end", string("a")),
        ("case 2 when 1 then 'a' else 'b' end", string("b")),
        ("case null when 1 then 'a' else 'b' end", string("b")),
        (
            "case 3 when 1 then 'a' when 2 then 'b' when 3 then 'c' else 'd' end",
            string("c"),
        ),
        ("if(true, 1, 2)", int64(1)),
        ("if(false, 1, 2)", int64(2)),
        ("if(null, 1, 2)", int64(2)),
        ("coalesce(1, 2, null)", int64(2)),
        ("coalesce(null, 2, null)", int64(2)),
        ("ifnull(1, 2)", int64(1)),
        ("ifnull(null, 2)", int64(2)),
        ("ifnull(1, null)", int64(1)),
        ("nullif('a', 'b')", string("a")),
        ("nullif('a', 'a')", Value::String(None)),
        ("nullif(null, 'a')", Value::String(None)),
        ("nullif('a', null)", string("a")),
    ];
    for (sql, expect) in cases {
        assert_eq!(expect, eval(sql), "{}", sql);
    }
}

#[test]
fn test_casts() {
    let cases = vec![
        (
            "cast(t as int64) from (select true as t)",
            Value::I64(Some(1)),
        ),
        ("cast(t as string) from (select true as t)", string("true")),
        (
            "cast(i1 as bool) from (select 1 as i1)",
            Value::Bool(Some(true)),
        ),
        (
            "cast(i1 as float64) from (select 1 as i1)",
            Value::F64(Some(1.0)),
        ),
        ("cast(i1 as string) from (select 1 as i1)", string("1")),
        (
            "cast(f1 as int64) from (select 1.0 as f1)",
            Value::I64(Some(1)),
        ),
        ("cast(f1 as string) from (select 1.0 as f1)", string("1")),
        (
            "cast(d as timestamp) from (select date '2020-01-01' as d)",
            ts(2020, 1, 1, 0, 0, 0),
        ),
        (
            "cast(d as string) from (select date '2020-01-01' as d)",
            string("2020-01-01"),
        ),
        (
            "cast(ts as date) from (select timestamp '2020-01-01' as ts)",
            date(2020, 1, 1),
        ),
        (
            "cast(ts as string) from (select timestamp '2020-01-01' as ts)",
            string("2020-01-01T00:00:00+00:00"),
        ),
        (
            "cast(t as bool) from (select 'true' as t)",
            Value::Bool(Some(true)),
        ),
        (
            "cast(i1 as int64) from (select '1' as i1)",
            Value::I64(Some(1)),
        ),
        (
            "cast(f1 as float64) from (select '1.0' as f1)",
            Value::F64(Some(1.0)),
        ),
        (
            "cast(d as date) from (select '2020-01-01' as d)",
            date(2020, 1, 1),
        ),
        (
            "cast(ts as timestamp) from (select '2020-01-01T00:00:00+00:00' as ts)",
            ts(2020, 1, 1, 0, 0, 0),
        ),
    ];
    for (sql, expect) in cases {
        assert_eq!(expect, eval(sql), "{}", sql);
    }
}

fn eval(sql: &str) -> Value {
    let sql = format!("select {}", sql);
    let mut storage = Storage::new();
    let catalog = crate::catalog::catalog(&mut storage, 100);
    let indexes = crate::catalog::indexes(&mut storage, 100);
    let parse = parser::analyze(catalog::ROOT_CATALOG_ID, &catalog, &sql).expect(&sql);
    let plan = planner::optimize(
        catalog::ROOT_CATALOG_ID,
        &catalog,
        &indexes,
        &mut storage,
        parse,
    );
    let program = crate::execute::compile(plan);
    let mut batch = program.execute(&mut storage, 100).last().unwrap();
    assert_eq!(1, batch.len(), "{}", &sql);
    match batch.columns.remove(0).1 {
        AnyArray::Bool(array) => Value::Bool(array.get(0)),
        AnyArray::I64(array) => Value::I64(array.get(0)),
        AnyArray::F64(array) => Value::F64(array.get(0)),
        AnyArray::Date(array) => Value::Date(array.get(0)),
        AnyArray::Timestamp(array) => Value::Timestamp(array.get(0)),
        AnyArray::String(array) => Value::String(array.get(0).map(|s| s.to_string())),
    }
}

fn date(y: i32, m: u32, d: u32) -> Value {
    let date = Utc.from_utc_date(&NaiveDate::from_ymd(y, m, d));
    let epoch = Utc.from_utc_date(&NaiveDate::from_ymd(1970, 1, 1));
    Value::Date(Some((date - epoch).num_days() as i32))
}
fn ts(y: i32, m: u32, d: u32, hour: u32, min: u32, sec: u32) -> Value {
    let ts = Utc
        .from_utc_date(&NaiveDate::from_ymd(y, m, d))
        .and_hms(hour, min, sec);
    let epoch = Utc
        .from_utc_date(&NaiveDate::from_ymd(1970, 1, 1))
        .and_hms(0, 0, 0);
    Value::Timestamp(Some((ts - epoch).num_microseconds().unwrap() as i64))
}

fn int64(value: i64) -> Value {
    Value::I64(Some(value))
}

fn float64(value: f64) -> Value {
    Value::F64(Some(value))
}

fn string(value: &str) -> Value {
    Value::String(Some(value.to_string()))
}
