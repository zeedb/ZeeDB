use crate::eval::*;
use ast::DatePart;
use chrono::Weekday;

#[test]
fn test_extract_from_date() {
    let cases = vec![
        (2020, 1, 1, DatePart::DayOfWeek, 4),
        (2020, 1, 1, DatePart::Day, 1),
        (2020, 1, 1, DatePart::DayOfYear, 1),
        (2020, 1, 1, DatePart::Week(Weekday::Sun), 0),
        (2020, 1, 1, DatePart::Week(Weekday::Mon), 0),
        (2020, 1, 1, DatePart::Week(Weekday::Tue), 0),
        (2020, 1, 1, DatePart::Week(Weekday::Wed), 1),
        (2020, 1, 1, DatePart::Week(Weekday::Thu), 0),
        (2020, 1, 1, DatePart::Week(Weekday::Fri), 0),
        (2020, 1, 1, DatePart::Week(Weekday::Sat), 0),
        (2020, 1, 1, DatePart::IsoWeek, 1),
        (2020, 1, 1, DatePart::Month, 1),
        (2020, 1, 1, DatePart::Quarter, 1),
        (2020, 1, 1, DatePart::Year, 2020),
        (2020, 1, 1, DatePart::IsoYear, 2020),
        (2019, 12, 31, DatePart::DayOfWeek, 3),
        (2019, 12, 31, DatePart::Day, 31),
        (2019, 12, 31, DatePart::DayOfYear, 365),
        (2019, 12, 31, DatePart::Week(Weekday::Sun), 52),
        (2019, 12, 31, DatePart::Week(Weekday::Mon), 52),
        (2019, 12, 31, DatePart::Week(Weekday::Tue), 53),
        (2019, 12, 31, DatePart::Week(Weekday::Wed), 52),
        (2019, 12, 31, DatePart::Week(Weekday::Thu), 52),
        (2019, 12, 31, DatePart::Week(Weekday::Fri), 52),
        (2019, 12, 31, DatePart::Week(Weekday::Sat), 52),
        (2019, 12, 31, DatePart::IsoWeek, 1),
        (2019, 12, 31, DatePart::Month, 12),
        (2019, 12, 31, DatePart::Quarter, 4),
        (2019, 12, 31, DatePart::Year, 2019),
        (2019, 12, 31, DatePart::IsoYear, 2020),
    ];
    for (year, month, day, part, expect) in cases {
        let date = date_from_ymd(year, month, day);
        let found = extract_from_date(date, part);
        assert_eq!(
            expect, found,
            "extract({:?} from '{}-{}-{}')",
            part, year, month, day
        );
    }
}

#[test]
fn test_date_diff() {
    let cases = vec![
        (2020, 1, 1, 2001, 1, 1, DatePart::Year, 19),
        (2020, 1, 1, 2001, 1, 1, DatePart::Month, 228),
        (2020, 1, 1, 2001, 1, 1, DatePart::Day, 6939),
        (2020, 1, 1, 2001, 1, 1, DatePart::Quarter, 76),
        (2020, 1, 1, 2001, 1, 1, DatePart::IsoYear, 19),
        (2020, 1, 1, 2001, 1, 1, DatePart::IsoWeek, 991),
        (2020, 1, 1, 2001, 1, 1, DatePart::Week(Weekday::Sun), 991),
        (2020, 1, 1, 2001, 1, 1, DatePart::Week(Weekday::Mon), 991),
        (2020, 1, 1, 2001, 1, 1, DatePart::Week(Weekday::Tue), 992),
        (2020, 1, 1, 2001, 1, 1, DatePart::Week(Weekday::Wed), 992),
        (2020, 1, 1, 2001, 1, 1, DatePart::Week(Weekday::Thu), 991),
        (2020, 1, 1, 2001, 1, 1, DatePart::Week(Weekday::Fri), 991),
        (2020, 1, 1, 2001, 1, 1, DatePart::Week(Weekday::Sat), 991),
    ];
    for (y1, m1, d1, y2, m2, d2, part, expect) in cases {
        let d1 = date_from_ymd(y1, m1, d1);
        let d2 = date_from_ymd(y2, m2, d2);
        let found = date_diff(d1, d2, part);
        assert_eq!(expect, found, "date_diff('{}', '{}', {:?})", d1, d2, part);
    }
}

#[test]
fn test_date_add_sub() {
    let cases = vec![
        (2020, 1, 1, 1, DatePart::Day, 2020, 1, 2),
        (2020, 1, 1, 1, DatePart::Week(Weekday::Sun), 2020, 1, 8),
        (2020, 1, 1, 1, DatePart::Month, 2020, 2, 1),
        (2020, 1, 1, 1, DatePart::Quarter, 2020, 4, 1),
        (2020, 1, 1, 1, DatePart::Year, 2021, 1, 1),
    ];
    for (y1, m1, d1, amount, part, y2, m2, d2) in cases {
        let date = date_from_ymd(y1, m1, d1);
        let expect = date_from_ymd(y2, m2, d2);
        let found = date_add(date, amount, part);
        assert_eq!(
            expect, found,
            "date_add('{}', {}, {:?})",
            date, amount, part
        );
        let reverse = date_sub(found, amount, part);
        assert_eq!(
            date, reverse,
            "date_sub('{}', {}, {:?})",
            found, amount, part
        );
    }
}

#[test]
fn test_substr() {
    let cases = vec![
        ("", 0, ""),
        ("", 1, ""),
        ("", -1, ""),
        ("abc", 1, "abc"),
        ("abc", 3, "c"),
        ("abc", 4, ""),
        ("abc", 0, "abc"),
        ("abc", -2, "bc"),
        ("abc", -5, "abc"),
        ("abc", i64::MIN, "abc"),
        ("abc", i64::MAX, ""),
        ("ЩФБШ", 0, "ЩФБШ"),
        ("ЩФБШ", 1, "ЩФБШ"),
        ("ЩФБШ", 3, "БШ"),
        ("ЩФБШ", 5, ""),
        ("ЩФБШ", -2, "БШ"),
        ("ЩФБШ", -4, "ЩФБШ"),
        ("ЩФБШ", -5, "ЩФБШ"),
        ("ЩФБШ", i64::MIN, "ЩФБШ"),
        ("ЩФБШ", i64::MAX, ""),
    ];
    for (test, position, expect) in cases {
        assert_eq!(expect, substr(test, position, i64::MAX));
    }
}

#[test]
fn test_substr3() {
    let cases = vec![
        ("", 0, 1, ""),
        ("", 0, 0, ""),
        ("", 1, 1, ""),
        ("", 1, 0, ""),
        ("", -1, 1, ""),
        ("", -1, 0, ""),
        ("abc", 0, 0, ""),
        ("abc", 0, 1, "a"),
        ("abc", -1, 0, ""),
        ("abc", -1, 1, "c"),
        ("abc", 1, 2, "ab"),
        ("abc", 3, 1, "c"),
        ("abc", 3, 0, ""),
        ("abc", 2, 1, "b"),
        ("abc", 4, 5, ""),
        ("abc", 1, i64::MAX, "abc"),
        ("abc", -2, 1, "b"),
        ("abc", -5, 2, "ab"),
        ("abc", -5, 0, ""),
        ("abc", i64::MIN, 1, "a"),
        ("abc", i64::MAX, i64::MAX, ""),
        ("ЩФБШ", 0, 2, "ЩФ"),
        ("ЩФБШ", 0, 5, "ЩФБШ"),
        ("ЩФБШ", 2, 1, "Ф"),
        ("ЩФБШ", 2, 5, "ФБШ"),
        ("ЩФБШ", 3, 2, "БШ"),
        ("ЩФБШ", 5, 2, ""),
        ("ЩФБШ", -2, 3, "БШ"),
        ("ЩФБШ", -2, 1, "Б"),
        ("ЩФБШ", -4, 5, "ЩФБШ"),
        ("ЩФБШ", -3, 2, "ФБ"),
        ("ЩФБШ", -5, 3, "ЩФБ"),
        ("ЩФБШ", i64::MIN, 1, "Щ"),
        ("ЩФБШ", i64::MAX, 1, ""),
    ];
    for (test, position, length, expect) in cases {
        assert_eq!(expect, substr(test, position, length));
    }
}

#[test]
fn test_regexp_extract() {
    let cases = vec![
        ("", "", Some("")),
        ("", "abc", None),
        ("abc", "abc", Some("abc")),
        ("abc", "^a", Some("a")),
        ("abc", "^b", None),
        ("abcdef", "a.c.*f", Some("abcdef")),
        ("abcdef", "ac.*e.", None),
        ("abcdef", "bcde", Some("bcde")),
        ("щцф", ".{3}", Some("щцф")),
        ("щцф", ".{6}", None),
        ("", "()", Some("")),
        ("", "(abc)", None),
        ("", "(abc)?", None),
        ("abc", "a(b)c", Some("b")),
        ("abcdef", "a(.c.*)f", Some("bcde")),
        ("abcdef", "(bcde)", Some("bcde")),
        ("щцф", "щ(.).", Some("ц")),
        ("щцф", "(?:щ|ц)(ц|ф)(?:щ|ф)", Some("ц")),
        ("щцф", "(.{6})", None),
    ];
    for (value, pattern, expect) in cases {
        let expect = expect.map(|s| s.to_string());
        assert_eq!(expect, regexp_extract(value, pattern));
    }
}

#[test]
fn test_left_pad() {
    let cases = vec![
        ("abcdef", 0, "defgh", ""),
        ("abcdef", 6, "defgh", "abcdef"),
        ("abcdef", 4, "defgh", "abcd"),
        ("abcdef", 3, "defgh", "abc"),
        ("abcde", 4, "defgh", "abcd"),
        ("abcde", 3, "defgh", "abc"),
        ("abc", 5, "defgh", "deabc"),
        ("abc", 7, "defg", "defgabc"),
        ("abc", 4, "def", "dabc"),
        ("abc", 10, "defg", "defgdefabc"),
        ("abcx", 5, "defgh", "dabcx"),
        ("abcx", 7, "defg", "defabcx"),
        ("abcx", 10, "defg", "defgdeabcx"),
        ("abc", 7, "-", "----abc"),
        ("", 7, "-", "-------"),
        ("", 7, "def", "defdefd"),
        ("¼¼¼𠈓𠈓𠈓", 6, "智者不終朝", "¼¼¼𠈓𠈓𠈓"),
        ("¼¼¼𠈓𠈓𠈓", 5, "智者不終朝", "¼¼¼𠈓𠈓"),
        ("¼¼¼𠈓𠈓𠈓", 7, "智者不終朝", "智¼¼¼𠈓𠈓𠈓"),
        ("¼¼¼𠈓𠈓𠈓", 10, "智者朝", "智者朝智¼¼¼𠈓𠈓𠈓"),
        ("¼¼𠈓𠈓", 10, "智者朝", "智者朝智者朝¼¼𠈓𠈓"),
        ("¼¼¼𠈓𠈓𠈓", 7, " ", " ¼¼¼𠈓𠈓𠈓"),
        ("¼¼¼𠈓𠈓𠈓", 10, " ", "    ¼¼¼𠈓𠈓𠈓"),
        ("", 5, " ", "     "),
    ];
    for (value, length, padding, expect) in cases {
        assert_eq!(expect, lpad(value, length, padding));
    }
}

#[test]
fn test_right_pad() {
    let cases = vec![
        ("abcdef", 0, "defgh", ""),
        ("abcdef", 6, "defgh", "abcdef"),
        ("abcdef", 4, "defgh", "abcd"),
        ("abcdef", 3, "defgh", "abc"),
        ("abc", 5, "defgh", "abcde"),
        ("abc", 7, "defg", "abcdefg"),
        ("abc", 4, "def", "abcd"),
        ("abc", 10, "defg", "abcdefgdef"),
        ("abcx", 5, "defgh", "abcxd"),
        ("abcx", 7, "defg", "abcxdef"),
        ("abcx", 8, "def", "abcxdefd"),
        ("abcx", 10, "defg", "abcxdefgde"),
        ("abc", 7, "-", "abc----"),
        ("", 7, "-", "-------"),
        ("", 7, "def", "defdefd"),
        ("¼¼¼𠈓𠈓𠈓", 6, "智者不終朝", "¼¼¼𠈓𠈓𠈓"),
        ("¼¼¼𠈓𠈓𠈓", 5, "智者不終朝", "¼¼¼𠈓𠈓"),
        ("¼¼¼𠈓𠈓𠈓", 7, "智者不終朝", "¼¼¼𠈓𠈓𠈓智"),
        ("¼¼¼𠈓𠈓𠈓", 10, "智者朝", "¼¼¼𠈓𠈓𠈓智者朝智"),
        ("¼¼¼𠈓𠈓𠈓", 7, " ", "¼¼¼𠈓𠈓𠈓 "),
        ("¼¼¼𠈓𠈓𠈓", 10, " ", "¼¼¼𠈓𠈓𠈓    "),
        ("", 5, " ", "     "),
    ];
    for (value, length, padding, expect) in cases {
        assert_eq!(expect, rpad(value, length, padding));
    }
}

#[test]
fn test_like() {
    let cases = vec![
        // '_' matches single character...
        ("_", "", false),
        ("_", "a", true),
        ("_", "ab", false),
        // ... any Unicode character actually.
        ("_", "ф", true),
        // Escaped '_' matches itself
        ("\\_", "_", true),
        ("\\_", "a", false),
        // '_' matches CR and LF
        ("_", "\n", true),
        ("_", "\r", true),
        // '%' should match any std::string
        ("%", "", true),
        ("%", "abc", true),
        ("%", "фюы", true),
        (
            "% matches LF, CR, and %",
            "\r\n matches LF, CR, and \t\r\n anything",
            true,
        ),
        // A few more more complex expressions
        ("a(%)b", "a()b", true),
        ("a(%)b", "a(z)b", true),
        ("a(_%)b", "a()b", false),
        ("a(_%)b", "a(z)b", true),
        ("a(_\\%)b", "a(z)b", false),
        ("\\a\\(_%\\)\\b", "a(z)b", true),
        ("a%b%c", "abc", true),
        ("a%b%c", "axyzbxyzc", true),
        ("a%xyz%c", "abxybyzbc", false),
        ("a%xyz%c", "abxybyzbxyzbc", true),
    ];
    for (pattern, test, expect) in cases {
        assert_eq!(
            expect,
            like(test, pattern),
            "like('{}', '{}')",
            test,
            pattern
        );
    }
}

#[test]
fn test_regexp_replace() {
    let cases = vec![
        ("", "", "", ""),
        ("abc", "", "x", "xaxbxcx"),
        ("abc", "x?", "x", "xaxbxcx"),
        ("abc", "b", "xyz", "axyzc"),
        ("abcabc", "bc", "xyz", "axyzaxyz"),
        ("abc", "abc", "xyz", "xyz"),
        ("banana", "ana", "xyz", "bxyzna"),
        ("banana", "ana", "", "bna"),
        ("banana", "a", "z", "bznznz"),
        ("banana", "(.)a(.)", "\\1\\2", "bnana"),
        ("banana", ".", "x", "xxxxxx"),
        ("щцфшцф", ".", "ы", "ыыыыыы"),
        ("T€T", "", "K", "KTK€KTK"),
        ("abc", "b*", "x", "xaxcx"),
        ("ab", "b*", "x", "xax"),
        ("bc", "b*", "x", "xcx"),
        ("b", "b*", "x", "x"),
        ("bb", "^b", "x", "xb"),
        ("one", "\\b", "-", "-one-"),
        ("one two  many", "\\b", "-", "-one- -two-  -many-"),
        // non-capturing group
        (
            "http://www.google.com",
            "(?:http|ftp)://(.*?)\\.(.*?)\\.(com|org|net)",
            "\\3.\\2.\\1",
            "com.google.www",
        ),
        // nested non-capturing group
        (
            "www3.archive.org",
            "(?:(?:http|ftp)://)?(.*?)\\.(.*?)\\.(com|org|net)",
            "\\3.\\2.\\1",
            "org.archive.www3",
        ),
    ];
    for (value, regexp, replacement, expect) in cases {
        let found = regexp_replace(value, regexp, replacement);
        assert_eq!(
            expect, found,
            "regexp_replace({}, {}, {})",
            value, regexp, replacement
        );
    }
}
