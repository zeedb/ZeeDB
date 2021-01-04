use crate::execute::Session;
use ast::*;
use chrono::*;
use kernel::*;
use regex::{Captures, Regex};

pub fn all(predicates: &Vec<Scalar>, input: &RecordBatch, state: &mut Session) -> BoolArray {
    let mut mask = BoolArray::trues(input.len());
    for p in predicates {
        mask = eval(p, &input, state).as_bool().and(&mask);
    }
    mask
}

pub fn eval(scalar: &Scalar, input: &RecordBatch, state: &mut Session) -> Array {
    match scalar {
        Scalar::Literal(value) => value.repeat(input.len()),
        Scalar::Column(column) => {
            let find = column.canonical_name();
            input.find(&find).expect(&find).clone()
        }
        Scalar::Parameter(name, _) => {
            let value: &Array = state.variables.get(name).as_ref().expect(name);
            assert_eq!(value.len(), 1, "@{} has length {}", name, value.len());
            value.repeat(input.len())
        }
        Scalar::Call(function) => eval_function(function.as_ref(), input, state),
        Scalar::Cast(scalar, data_type) => eval(scalar, input, state).cast(*data_type),
    }
}

fn eval_function(function: &F, input: &RecordBatch, state: &mut Session) -> Array {
    let mut e = |a| eval(a, input, state);
    match function {
        F::CurrentDate | F::CurrentTimestamp => panic!(
            "{} should have been eliminated in the rewrite phase",
            function.name()
        ),
        F::Xid => I64Array::from(vec![state.txn])
            .repeat(input.len())
            .as_array(),
        F::Coalesce(varargs) => {
            let mut tail = e(varargs.last().unwrap());
            for head in &varargs[..varargs.len() - 1] {
                tail = e(head).coalesce(&tail);
            }
            tail
        }
        F::ConcatString(varargs) => {
            StringArray::concat(varargs.iter().map(|a| e(a).as_string()).collect()).as_array()
        }
        F::Greatest(varargs) => Array::greatest(varargs.iter().map(e).collect()),
        F::Least(varargs) => Array::least(varargs.iter().map(e).collect()),
        F::AbsDouble(a) => e(a).as_f64().unary(f64::abs),
        F::AbsInt64(a) => e(a).as_i64().unary(i64::abs),
        F::AcosDouble(a) => e(a).as_f64().unary(f64::acos),
        F::AcoshDouble(a) => e(a).as_f64().unary(f64::acosh),
        F::AsinDouble(a) => e(a).as_f64().unary(f64::asin),
        F::AsinhDouble(a) => e(a).as_f64().unary(f64::asinh),
        F::AtanDouble(a) => e(a).as_f64().unary(f64::atan),
        F::AtanhDouble(a) => e(a).as_f64().unary(f64::atanh),
        F::ByteLengthString(a) => e(a).as_string().unary(|a| a.len() as i64),
        F::CeilDouble(a) => e(a).as_f64().unary(f64::ceil),
        F::CharLengthString(a) => e(a).as_string().unary(|a| a.chars().count() as i64),
        F::ChrString(a) => e(a).as_i64().unary(chr),
        F::CosDouble(a) => e(a).as_f64().unary(f64::cos),
        F::CoshDouble(a) => e(a).as_f64().unary(f64::cosh),
        F::DateFromTimestamp(a) => e(a).as_timestamp().unary(date_from_timestamp),
        F::DateFromUnixDate(a) => e(a).as_i64().unary(date_from_unix_date),
        F::DecimalLogarithmDouble(a) => e(a).as_f64().unary(f64::log10),
        F::ExpDouble(a) => e(a).as_f64().unary(f64::exp),
        F::ExtractDateFromTimestamp(a) => e(a).as_timestamp().unary(date_from_timestamp),
        F::FloorDouble(a) => e(a).as_f64().unary(f64::floor),
        F::IsFalse(a) => e(a).as_bool().unary_is(|a| a == Some(false)),
        F::IsInf(a) => e(a).as_f64().unary(f64::is_infinite),
        F::IsNan(a) => e(a).as_f64().unary(f64::is_nan),
        F::IsNull(a) => e(a).is_null().as_array(),
        F::IsTrue(a) => e(a).as_bool().unary_is(|a| a == Some(true)),
        F::LengthString(a) => e(a).as_string().unary(|a| a.chars().count() as i64),
        F::LowerString(a) => e(a).as_string().unary(|a| a.to_lowercase()),
        F::NaturalLogarithmDouble(a) => e(a).as_f64().unary(f64::ln),
        F::NextVal(a) => e(a).as_i64().unary(|a| state.storage.next_val(a)),
        F::Not(a) => e(a).as_bool().unary(|a| !a),
        F::ReverseString(a) => e(a)
            .as_string()
            .unary(|a| a.chars().rev().collect::<String>()),
        F::RoundDouble(a) => e(a).as_f64().unary(f64::round),
        F::SignDouble(a) => e(a).as_f64().unary(f64::signum),
        F::SignInt64(a) => e(a).as_i64().unary(i64::signum),
        F::SinDouble(a) => e(a).as_f64().unary(f64::sin),
        F::SinhDouble(a) => e(a).as_f64().unary(f64::sinh),
        F::SqrtDouble(a) => e(a).as_f64().unary(f64::sqrt),
        F::StringFromDate(a) => e(a).as_date().unary(string_from_date),
        F::StringFromTimestamp(a) => e(a).as_timestamp().unary(string_from_timestamp),
        F::TanDouble(a) => e(a).as_f64().unary(f64::tan),
        F::TanhDouble(a) => e(a).as_f64().unary(f64::tanh),
        F::TimestampFromDate(a) => e(a).as_date().unary(timestamp_from_date),
        F::TimestampFromString(a) => e(a).as_string().unary(timestamp_from_string),
        F::TimestampFromUnixMicrosInt64(a) => e(a).as_i64().unary(timestamp),
        F::TruncDouble(a) => e(a).as_f64().unary(f64::trunc),
        F::UnaryMinusDouble(a) => e(a).as_f64().unary(|a| -a),
        F::UnaryMinusInt64(a) => e(a).as_i64().unary(|a| -a),
        F::UnixDate(a) => e(a).as_date().unary(|a| a as i64),
        F::UnixMicrosFromTimestamp(a) => e(a).as_timestamp().unary(|a| a),
        F::UpperString(a) => e(a).as_string().unary(|a| a.to_uppercase()),
        F::DateTruncDate(a, date_part) => e(a).as_date().unary(|a| date_trunc(date(a), *date_part)),
        F::ExtractFromDate(a, date_part) => e(a)
            .as_date()
            .unary(|a| extract_from_date(date(a), *date_part)),
        F::ExtractFromTimestamp(a, date_part) => e(a)
            .as_timestamp()
            .unary(|a| extract_from_timestamp(timestamp(a), *date_part)),
        F::TimestampTrunc(a, date_part) => e(a)
            .as_timestamp()
            .unary(|a| timestamp_trunc(timestamp(a), *date_part)),
        F::In(a, varargs) => e(a)
            .equal_any(varargs.iter().map(|a| e(a)).collect())
            .as_array(),
        F::AddDouble(a, b) => (e(a), e(b)).as_f64().binary(|a, b| a + b),
        F::AddInt64(a, b) => (e(a), e(b)).as_i64().binary(|a, b| a + b),
        F::And(a, b) => e(a).as_bool().and(&e(b).as_bool()).as_array(),
        F::Atan2Double(a, b) => (e(a), e(b)).as_f64().binary(|a, b| a.atan2(b)),
        F::DivideDouble(a, b) => (e(a), e(b)).as_f64().binary(|a, b| a / b),
        F::DivInt64(a, b) => (e(a), e(b)).as_i64().binary(|a, b| a / b),
        F::EndsWithString(a, b) => (e(a), e(b)).as_string().binary(|a, b| a.ends_with(b)),
        F::Equal(a, b) => e(a).equal(&e(b)).as_array(),
        F::FormatDate(a, b) => (e(a).as_string(), e(b).as_date()).binary(|a, b| format_date(a, b)),
        F::FormatTimestamp(a, b) => {
            (e(a).as_string(), e(b).as_timestamp()).binary(|a, b| format_timestamp(a, b))
        }
        F::Greater(a, b) => e(a).greater(&e(b)).as_array(),
        F::GreaterOrEqual(a, b) => e(a).greater_equal(&e(b)).as_array(),
        F::Ifnull(a, b) => e(a).coalesce(&e(b)),
        F::Is(a, b) => e(a).is(&e(b)).as_array(),
        F::LeftString(a, b) => (e(a).as_string(), e(b).as_i64())
            .binary(|a, b| a.chars().take(b as usize).collect::<String>()),
        F::Less(a, b) => e(a).less(&e(b)).as_array(),
        F::LessOrEqual(a, b) => e(a).less_equal(&e(b)).as_array(),
        F::LogarithmDouble(a, b) => (e(a), e(b)).as_f64().binary(|a, b| a.log(b)),
        F::LtrimString(a, None) => e(a).as_string().unary(|a| a.trim_start()),
        F::LtrimString(a, Some(b)) => (e(a), e(b)).as_string().binary(ltrim),
        F::ModInt64(a, b) => (e(a), e(b)).as_i64().binary(|a, b| a % b),
        F::MultiplyDouble(a, b) => (e(a), e(b)).as_f64().binary(|a, b| a * b),
        F::MultiplyInt64(a, b) => (e(a), e(b)).as_i64().binary(|a, b| a * b),
        F::NotEqual(a, b) => e(a).not_equal(&e(b)).as_array(),
        F::Nullif(a, b) => e(a).null_if(&e(b)),
        F::Or(a, b) => e(a).as_bool().or(&e(b).as_bool()).as_array(),
        F::ParseDate(a, b) => (e(a), e(b)).as_string().binary(|a, b| parse_date(a, b)),
        F::ParseTimestamp(a, b) => (e(a), e(b))
            .as_string()
            .binary(|a, b| parse_timestamp(a, b)),
        F::PowDouble(a, b) => (e(a), e(b)).as_f64().binary(f64::powf),
        F::RegexpContainsString(a, b) => (e(a), e(b))
            .as_string()
            .binary(|a, b| regexp_contains(a, b)),
        F::RegexpExtractString(a, b) => (e(a), e(b))
            .as_string()
            .binary_nullable(|a, b| regexp_extract(a, b)),
        F::RepeatString(a, b) => {
            (e(a).as_string(), e(b).as_i64()).binary(|a, b| a.repeat(b as usize))
        }
        F::RightString(a, b) => (e(a).as_string(), e(b).as_i64()).binary(right),
        F::RoundWithDigitsDouble(a, b) => (e(a).as_f64(), e(b).as_i64()).binary(round),
        F::RtrimString(a, None) => e(a).as_string().unary(|a| a.trim_end()),
        F::RtrimString(a, Some(b)) => (e(a), e(b)).as_string().binary(rtrim),
        F::StartsWithString(a, b) => (e(a), e(b)).as_string().binary(|a, b| a.starts_with(b)),
        F::StringLike(a, b) => (e(a), e(b)).as_string().binary(like),
        F::StrposString(a, b) => (e(a), e(b)).as_string().binary_nullable(strpos),
        F::SubtractDouble(a, b) => (e(a), e(b)).as_f64().binary(|a, b| a - b),
        F::SubtractInt64(a, b) => (e(a), e(b)).as_i64().binary(|a, b| a - b),
        F::TrimString(a, None) => e(a).as_string().unary(&str::trim),
        F::TrimString(a, Some(b)) => (e(a), e(b)).as_string().binary(trim),
        F::TruncWithDigitsDouble(a, b) => (e(a).as_f64(), e(b).as_i64()).binary(trunc),
        F::DateAddDate(a, b, date_part) => {
            (e(a).as_date(), e(b).as_i64()).binary(|a, b| date_add(date(a), b, *date_part))
        }
        F::DateDiffDate(a, b, date_part) => (e(a), e(b))
            .as_date()
            .binary(|a, b| date_diff(date(a), date(b), *date_part)),
        F::DateSubDate(a, b, date_part) => {
            (e(a).as_date(), e(b).as_i64()).binary(|a, b| date_sub(date(a), b, *date_part))
        }
        F::TimestampAdd(a, b, date_part) => (e(a).as_timestamp(), e(b).as_i64())
            .binary(|a, b| timestamp_add(timestamp(a), b, *date_part)),
        F::TimestampDiff(a, b, date_part) => (e(a), e(b))
            .as_timestamp()
            .binary(|a, b| timestamp_diff(timestamp(a), timestamp(b), *date_part)),
        F::TimestampSub(a, b, date_part) => (e(a).as_timestamp(), e(b).as_i64())
            .binary(|a, b| timestamp_sub(timestamp(a), b, *date_part)),
        F::Between(a, b, c) => {
            let a = e(a);
            let b = e(b);
            let c = e(c);
            let left = a.greater_equal(&b);
            let right = a.less_equal(&c);
            left.and(&right).as_array()
        }
        F::DateFromYearMonthDay(a, b, c) => {
            (e(a).as_i64(), e(b).as_i64(), e(c).as_i64()).ternary(|a, b, c| date_from_ymd(a, b, c))
        }
        F::If(a, b, c) => e(a).as_bool().blend(&e(b), &e(c)),
        F::LpadString(a, b, c) => {
            (e(a).as_string(), e(b).as_i64(), e(c).as_string()).ternary(|a, b, c| lpad(a, b, c))
        }
        F::RegexpReplaceString(a, b, c) => (e(a).as_string(), e(b).as_string(), e(c).as_string())
            .ternary(|a, b, c| regexp_replace(a, b, c)),
        F::ReplaceString(a, b, c) => (e(a).as_string(), e(b).as_string(), e(c).as_string())
            .ternary(|a, b, c| a.replace(b, c)),
        F::RpadString(a, b, c) => {
            (e(a).as_string(), e(b).as_i64(), e(c).as_string()).ternary(|a, b, c| rpad(a, b, c))
        }
        F::SubstrString(a, b, None) => {
            (e(a).as_string(), e(b).as_i64()).binary(|a, b| substr(a, b, i64::MAX))
        }
        F::SubstrString(a, b, Some(c)) => {
            (e(a).as_string(), e(b).as_i64(), e(c).as_i64()).ternary(|a, b, c| substr(a, b, c))
        }
        F::CaseNoValue(cases, default) => {
            let mut acc = e(default);
            for (test, value) in cases.iter().rev() {
                acc = e(test).as_bool().blend(&e(value), &acc);
            }
            acc
        }
        F::CaseWithValue(head, cases, default) => {
            let mut acc = e(default);
            let expect = e(head);
            for (found, value) in cases.iter().rev() {
                acc = expect.equal(&e(found)).blend(&e(value), &acc);
            }
            acc
        }
    }
}

// Math functions.

fn round(value: f64, digits: i64) -> f64 {
    let mul = f64::powi(10.0, digits as i32);
    (value * mul).round() / mul
}

fn trunc(value: f64, digits: i64) -> f64 {
    let mul = f64::powi(10.0, digits as i32);
    (value * mul).trunc() / mul
}

// String functions.

fn chr(code_point: i64) -> String {
    char::from_u32(code_point as u32)
        .map(|c| c.to_string())
        .unwrap_or("".to_string())
}

fn right(value: &str, len: i64) -> String {
    let mut result = vec![];
    for c in value.chars().rev().take(len as usize) {
        result.push(c);
    }
    result.iter().rev().collect::<String>()
}

fn ltrim(value: &str, pattern: &str) -> String {
    value
        .trim_start_matches(|c| pattern.contains(c))
        .to_string()
}

fn rtrim(value: &str, pattern: &str) -> String {
    value.trim_end_matches(|c| pattern.contains(c)).to_string()
}

fn trim(value: &str, pattern: &str) -> String {
    value.trim_matches(|c| pattern.contains(c)).to_string()
}

pub(crate) fn substr(value: &str, mut position: i64, mut length: i64) -> String {
    assert!(length >= 0, "SUBSTR length cannot be negative");
    let chars: Vec<char> = value.chars().collect();
    if position > chars.len() as i64 {
        return "".to_string();
    }
    if position < 0 {
        position = chars.len() as i64 + position + 1
    }
    if position <= 0 {
        position = 1
    }
    if length > chars.len() as i64 - position + 1 {
        length = chars.len() as i64 - position + 1
    }
    chars[position as usize - 1..position as usize - 1 + length as usize]
        .iter()
        .collect()
}

fn strpos(string: &str, substring: &str) -> Option<i64> {
    string.find(substring).map(|i| i as i64 + 1)
}

pub(crate) fn lpad(original_value: &str, return_length: i64, pattern: &str) -> String {
    assert!(return_length >= 0, "LPAD return_length cannot be negative");
    assert!(pattern.len() != 0, "LPAD pattern cannot be empty");
    let return_length = return_length as usize;
    let original_chars: Vec<char> = original_value.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();
    if return_length <= original_chars.len() {
        return original_chars[..return_length].iter().collect();
    }
    let pad_len = return_length - original_chars.len();
    let mut padded_value = Vec::with_capacity(return_length);
    for i in 0..pad_len {
        padded_value.push(pattern_chars[i % pattern_chars.len()]);
    }
    padded_value.extend_from_slice(&original_chars);
    padded_value.iter().collect()
}

pub(crate) fn rpad(original_value: &str, return_length: i64, pattern: &str) -> String {
    assert!(return_length >= 0, "RPAD return_length cannot be negative");
    assert!(pattern.len() != 0, "RPAD pattern cannot be empty");
    let return_length = return_length as usize;
    let original_chars: Vec<char> = original_value.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();
    if return_length <= original_chars.len() {
        return original_chars[..return_length].iter().collect();
    }
    let pad_len = return_length - original_chars.len();
    let mut padded_value = Vec::with_capacity(return_length);
    padded_value.extend_from_slice(&original_chars);
    for i in 0..pad_len {
        padded_value.push(pattern_chars[i % pattern_chars.len()]);
    }
    padded_value.iter().collect()
}

pub(crate) fn regexp_extract(value: &str, regexp: &str) -> Option<String> {
    let re = Regex::new(regexp).expect(regexp);
    match re.captures_len() {
        1 => re.find(value).map(|m| m.as_str().to_string()),
        2 => re
            .captures(value)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().to_string()),
        _ => panic!(
            "Regular expression r'{}' has more than 1 capturing group",
            re
        ),
    }
}

fn regexp_contains(value: &str, regexp: &str) -> bool {
    Regex::new(regexp).expect(regexp).is_match(value)
}

pub(crate) fn regexp_replace(value: &str, regexp: &str, replacement: &str) -> String {
    let rewrite = |captures: &Captures| -> String {
        let mut buffer = String::new();
        let mut i = 0;
        let chars: Vec<_> = replacement.chars().collect();
        while i < chars.len() {
            while i < chars.len() && chars[i] != '\\' {
                buffer.push(chars[i]);
                i += 1;
            }
            if i < chars.len() {
                i += 1;
                if i < chars.len() {
                    let c = chars[i];
                    if let Some(n) = c.to_digit(10) {
                        buffer.push_str(&captures[n as usize]);
                    } else if c == '\\' {
                        buffer.push('\\');
                    } else {
                        panic!("Invalid REGEXP_REPLACE pattern");
                    }
                    i += 1;
                } else {
                    panic!("REGEXP_REPLACE pattern ends with \\");
                }
            }
        }
        buffer
    };
    Regex::new(regexp)
        .expect(regexp)
        .replace_all(value, rewrite)
        .to_string()
}

pub(crate) fn like(value: &str, pattern: &str) -> bool {
    like_pattern(pattern).is_match(value)
}

fn like_pattern(pattern: &str) -> Regex {
    let mut re = "(?s)^".to_string();
    let mut i = 0;
    let chars: Vec<_> = pattern.chars().collect();
    while i < chars.len() {
        match chars[i] {
            '\\' => {
                if i + 1 >= chars.len() {
                    panic!("LIKE pattern ends with backslash");
                }
                if is_meta_character(chars[i + 1]) {
                    re.push('\\');
                }
                re.push(chars[i + 1]);
                i += 1;
            }
            '_' => re.push('.'),
            '%' => re.push_str(".*"),
            c => {
                if is_meta_character(c) {
                    re.push('\\');
                }
                re.push(c);
            }
        }
        i += 1;
    }
    re.push('$');
    Regex::new(&re).unwrap()
}

fn is_meta_character(c: char) -> bool {
    match c {
        '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$'
        | '#' | '&' | '-' | '~' => true,
        _ => false,
    }
}

// Datetime functions.

pub(crate) fn date_from_ymd(year: i64, month: i64, day: i64) -> Date<Utc> {
    Utc.from_utc_date(&NaiveDate::from_ymd(year as i32, month as u32, day as u32))
}

fn date_from_timestamp(value: i64) -> Date<Utc> {
    timestamp(value).date()
}

fn date_from_unix_date(value: i64) -> Date<Utc> {
    let naive = NaiveDate::from_ymd(1970, 1, 1) + Duration::days(value as i64);
    Utc.from_utc_date(&naive)
}

fn timestamp_from_date(value: i32) -> DateTime<Utc> {
    date(value as i32).and_hms(0, 0, 0)
}

fn timestamp_from_string(value: &str) -> DateTime<Utc> {
    parse_timestamp("%+", value)
}

fn parse_date(format: &str, value: &str) -> Date<Utc> {
    Utc.from_utc_date(&NaiveDate::parse_from_str(value, format).expect(value))
}

fn parse_timestamp(format: &str, value: &str) -> DateTime<Utc> {
    Utc.from_utc_datetime(&NaiveDateTime::parse_from_str(value, format).expect(value))
}

fn string_from_date(value: i32) -> String {
    date(value).format("%F").to_string()
}

fn string_from_timestamp(value: i64) -> String {
    format_timestamp("%+", value)
}

fn format_date(format: &str, value: i32) -> String {
    date(value).format(format).to_string()
}

fn format_timestamp(format: &str, value: i64) -> String {
    timestamp(value).format(format).to_string()
}

fn date_trunc(d: Date<Utc>, date_part: DatePart) -> Date<Utc> {
    match date_part {
        DatePart::Day => d,
        DatePart::Week(weekday) => prev_weekday_or_today(d, weekday),
        DatePart::IsoWeek => prev_weekday_or_today(d, Weekday::Mon),
        DatePart::Month => d.with_day(1).unwrap(),
        DatePart::Quarter => d
            .with_month0(d.month0() / 4 * 4)
            .unwrap()
            .with_day(1)
            .unwrap(),
        DatePart::Year => d.with_month(1).unwrap().with_day(1).unwrap(),
        DatePart::IsoYear => {
            let iso = d.iso_week();
            let naive = NaiveDate::from_isoywd(iso.year(), 1, Weekday::Mon);
            Utc.from_utc_date(&naive)
        }
        DatePart::Microsecond
        | DatePart::Millisecond
        | DatePart::Second
        | DatePart::Minute
        | DatePart::Hour
        | DatePart::DayOfWeek
        | DatePart::DayOfYear => panic!("date_trunc(_, {:?}) is not supported", date_part),
    }
}

fn timestamp_trunc(ts: DateTime<Utc>, date_part: DatePart) -> DateTime<Utc> {
    match date_part {
        DatePart::Microsecond => ts,
        DatePart::Millisecond => ts.duration_trunc(Duration::milliseconds(1)).unwrap(),
        DatePart::Second => ts.duration_trunc(Duration::seconds(1)).unwrap(),
        DatePart::Minute => ts.duration_trunc(Duration::minutes(1)).unwrap(),
        DatePart::Hour => ts.duration_trunc(Duration::hours(1)).unwrap(),
        DatePart::Day
        | DatePart::Week(_)
        | DatePart::IsoWeek
        | DatePart::Month
        | DatePart::Quarter
        | DatePart::Year
        | DatePart::IsoYear => date_trunc(ts.date(), date_part).and_hms(0, 0, 0),
        DatePart::DayOfWeek | DatePart::DayOfYear => {
            panic!("timestamp_trunc(_, {:?}) is not supported", date_part)
        }
    }
}

pub(crate) fn extract_from_date(d: Date<Utc>, date_part: DatePart) -> i64 {
    match date_part {
        DatePart::DayOfWeek => d.weekday().num_days_from_sunday() as i64 + 1,
        DatePart::Day => d.day() as i64,
        DatePart::DayOfYear => d.ordinal() as i64,
        DatePart::Week(weekday) => {
            let first_calendar_day_of_year = date_from_ymd(d.year() as i64, 1, 1);
            let effective_first_day_of_year =
                next_weekday_or_today(first_calendar_day_of_year, weekday);
            if d < effective_first_day_of_year {
                0
            } else {
                (d - effective_first_day_of_year).num_days() / 7 + 1 as i64
            }
        }
        DatePart::IsoWeek => d.iso_week().week() as i64,
        DatePart::Month => d.month() as i64,
        DatePart::Quarter => d.month0() as i64 / 3 + 1,
        DatePart::Year => d.year() as i64,
        DatePart::IsoYear => d.iso_week().year() as i64,
        DatePart::Microsecond
        | DatePart::Millisecond
        | DatePart::Second
        | DatePart::Minute
        | DatePart::Hour => panic!("extract {:?} from DATE is not supported", date_part),
    }
}

/// If `day` is a `weekday`, return `day`, otherwise return the next `weekday`.
fn next_weekday_or_today(mut d: Date<Utc>, weekday: Weekday) -> Date<Utc> {
    while d.weekday() != weekday {
        d = d.succ()
    }
    d
}
/// If `day` is a `weekday`, return `day`, otherwise return the previous `weekday`.
fn prev_weekday_or_today(mut d: Date<Utc>, weekday: Weekday) -> Date<Utc> {
    while d.weekday() != weekday {
        d = d.pred()
    }
    d
}

fn extract_from_timestamp(ts: DateTime<Utc>, date_part: DatePart) -> i64 {
    match date_part {
        DatePart::Microsecond => ts.timestamp_subsec_micros() as i64,
        DatePart::Millisecond => ts.timestamp_subsec_millis() as i64,
        DatePart::Second => ts.second() as i64,
        DatePart::Minute => ts.minute() as i64,
        DatePart::Hour => ts.hour() as i64,
        DatePart::Day
        | DatePart::Week(_)
        | DatePart::IsoWeek
        | DatePart::Month
        | DatePart::Quarter
        | DatePart::Year
        | DatePart::IsoYear
        | DatePart::DayOfWeek
        | DatePart::DayOfYear => extract_from_date(ts.date(), date_part),
    }
}

pub(crate) fn date_add(date: Date<Utc>, amount: i64, date_part: DatePart) -> Date<Utc> {
    match date_part {
        DatePart::Day => date + Duration::days(amount),
        DatePart::Week(_) => date + Duration::days(amount * 7),
        DatePart::Month => {
            let d = date.day0(); // This can exceed the last day of the month, but chrono seems to fix it.
            let m = (date.month0() as i64 + amount).rem_euclid(12);
            let y = (date.year() as i64 * 12 + date.month0() as i64 + amount).div_euclid(12);
            Utc.from_utc_date(&NaiveDate::from_ymd(y as i32, (m + 1) as u32, d + 1))
        }
        DatePart::Quarter => date_add(date, amount * 3, DatePart::Month),
        DatePart::Year => date_add(date, amount * 12, DatePart::Month),
        DatePart::Microsecond
        | DatePart::Millisecond
        | DatePart::Second
        | DatePart::Minute
        | DatePart::Hour
        | DatePart::DayOfWeek
        | DatePart::DayOfYear
        | DatePart::IsoWeek
        | DatePart::IsoYear => panic!("date_add(_, {:?}) is not supported", date_part),
    }
}

pub(crate) fn date_sub(date: Date<Utc>, amount: i64, date_part: DatePart) -> Date<Utc> {
    date_add(date, -amount, date_part)
}

pub(crate) fn date_diff(later: Date<Utc>, earlier: Date<Utc>, date_part: DatePart) -> i64 {
    match date_part {
        DatePart::Day => (later - earlier).num_days() as i64,
        DatePart::Week(_) | DatePart::IsoWeek => {
            let later = date_trunc(later, date_part);
            let earlier = date_trunc(earlier, date_part);
            (later - earlier).num_weeks() as i64
        }
        DatePart::Month => {
            let years = later.year() - earlier.year();
            let months = later.month() - earlier.month();
            years as i64 * 12 + months as i64
        }
        DatePart::Quarter => {
            let years = later.year() - earlier.year();
            let months = later.month() - earlier.month();
            years as i64 * 4 + months as i64 / 3
        }
        DatePart::Year => (later.year() - earlier.year()) as i64,
        DatePart::IsoYear => (later.iso_week().year() - earlier.iso_week().year()) as i64,
        DatePart::Microsecond
        | DatePart::Millisecond
        | DatePart::Second
        | DatePart::Minute
        | DatePart::Hour
        | DatePart::DayOfWeek
        | DatePart::DayOfYear => panic!("date_diff(_, _, {:?}) is not supported", date_part),
    }
}

fn timestamp_add(ts: DateTime<Utc>, amount: i64, date_part: DatePart) -> DateTime<Utc> {
    ts + timestamp_duration(amount, date_part)
}

fn timestamp_sub(ts: DateTime<Utc>, amount: i64, date_part: DatePart) -> DateTime<Utc> {
    ts - timestamp_duration(amount, date_part)
}

fn timestamp_duration(amount: i64, date_part: DatePart) -> Duration {
    match date_part {
        DatePart::Microsecond => Duration::microseconds(amount),
        DatePart::Millisecond => Duration::milliseconds(amount),
        DatePart::Second => Duration::seconds(amount),
        DatePart::Minute => Duration::minutes(amount),
        DatePart::Hour => Duration::hours(amount),
        DatePart::Day => Duration::days(amount),
        DatePart::DayOfWeek
        | DatePart::DayOfYear
        | DatePart::Week(_)
        | DatePart::IsoWeek
        | DatePart::Month
        | DatePart::Quarter
        | DatePart::Year
        | DatePart::IsoYear => panic!(
            "timestamp_add/subtract(_, {:?}) is not supported",
            date_part
        ),
    }
}

fn timestamp_diff(later: DateTime<Utc>, earlier: DateTime<Utc>, date_part: DatePart) -> i64 {
    match date_part {
        DatePart::Microsecond => (later - earlier).num_microseconds().unwrap(),
        DatePart::Millisecond => (later - earlier).num_milliseconds() as i64,
        DatePart::Second => (later - earlier).num_seconds() as i64,
        DatePart::Minute => (later - earlier).num_minutes() as i64,
        DatePart::Hour => (later - earlier).num_hours() as i64,
        DatePart::Day
        | DatePart::Week(_)
        | DatePart::IsoWeek
        | DatePart::Month
        | DatePart::Quarter
        | DatePart::Year
        | DatePart::IsoYear => date_diff(later.date(), earlier.date(), date_part),
        DatePart::DayOfWeek | DatePart::DayOfYear => {
            panic!("date_diff(_, _, {:?}) is not supported", date_part)
        }
    }
}

fn epoch_date(d: Date<Utc>) -> i32 {
    (d - date(0)).num_days() as i32
}

fn epoch_micros(ts: DateTime<Utc>) -> i64 {
    ts.timestamp() * MICROSECONDS + ts.timestamp_subsec_micros() as i64
}

fn date(value: i32) -> Date<Utc> {
    let naive = NaiveDate::from_ymd(1970, 1, 1) + Duration::days(value as i64);
    Utc.from_utc_date(&naive)
}

fn timestamp(value: i64) -> DateTime<Utc> {
    Utc.timestamp(
        value / MICROSECONDS,
        (value % MICROSECONDS * MILLISECONDS) as u32,
    )
}

/// Number of milliseconds in a second
const MILLISECONDS: i64 = 1_000;
/// Number of microseconds in a second
const MICROSECONDS: i64 = 1_000_000;

trait UnaryOperatorSupport<'a> {
    type Element;

    fn len(&'a self) -> usize;

    fn get(&'a self, i: usize) -> Option<Self::Element>;

    fn unary<T: ArrayBuilderSupport>(&'a self, f: impl Fn(Self::Element) -> T) -> Array {
        let mut builder = T::with_capacity(self.len());
        for i in 0..self.len() {
            if let Some(a) = self.get(i) {
                T::push_some(&mut builder, f(a));
            } else {
                T::push_none(&mut builder);
            }
        }
        T::as_array(builder)
    }

    fn unary_is<T: ArrayBuilderSupport>(&'a self, f: impl Fn(Option<Self::Element>) -> T) -> Array {
        let mut builder = T::with_capacity(self.len());
        for i in 0..self.len() {
            T::push_some(&mut builder, f(self.get(i)));
        }
        T::as_array(builder)
    }
}

impl<'a> UnaryOperatorSupport<'a> for BoolArray {
    type Element = bool;

    fn len(&'a self) -> usize {
        self.len()
    }

    fn get(&'a self, i: usize) -> Option<Self::Element> {
        self.get(i)
    }
}

impl<'a> UnaryOperatorSupport<'a> for I64Array {
    type Element = i64;

    fn len(&'a self) -> usize {
        self.len()
    }

    fn get(&'a self, i: usize) -> Option<Self::Element> {
        self.get(i)
    }
}

impl<'a> UnaryOperatorSupport<'a> for F64Array {
    type Element = f64;

    fn len(&'a self) -> usize {
        self.len()
    }

    fn get(&'a self, i: usize) -> Option<Self::Element> {
        self.get(i)
    }
}

impl<'a> UnaryOperatorSupport<'a> for DateArray {
    type Element = i32;

    fn len(&'a self) -> usize {
        self.len()
    }

    fn get(&'a self, i: usize) -> Option<Self::Element> {
        self.get(i)
    }
}

impl<'a> UnaryOperatorSupport<'a> for TimestampArray {
    type Element = i64;

    fn len(&'a self) -> usize {
        self.len()
    }

    fn get(&'a self, i: usize) -> Option<Self::Element> {
        self.get(i)
    }
}

impl<'a> UnaryOperatorSupport<'a> for StringArray {
    type Element = &'a str;

    fn len(&'a self) -> usize {
        self.len()
    }

    fn get(&'a self, i: usize) -> Option<Self::Element> {
        self.get(i)
    }
}

trait ArrayBuilderSupport {
    type Container;

    fn with_capacity(capacity: usize) -> Self::Container;
    fn push_some(array: &mut Self::Container, next: Self);
    fn push_none(array: &mut Self::Container);
    fn as_array(array: Self::Container) -> Array;
}

impl ArrayBuilderSupport for bool {
    type Container = BoolArray;

    fn with_capacity(capacity: usize) -> Self::Container {
        Self::Container::with_capacity(capacity)
    }

    fn push_some(array: &mut Self::Container, next: Self) {
        array.push(Some(next))
    }

    fn push_none(array: &mut Self::Container) {
        array.push(None)
    }

    fn as_array(array: Self::Container) -> Array {
        array.as_array()
    }
}

impl ArrayBuilderSupport for i64 {
    type Container = I64Array;

    fn with_capacity(capacity: usize) -> Self::Container {
        Self::Container::with_capacity(capacity)
    }

    fn push_some(array: &mut Self::Container, next: Self) {
        array.push(Some(next))
    }

    fn push_none(array: &mut Self::Container) {
        array.push(None)
    }

    fn as_array(array: Self::Container) -> Array {
        array.as_array()
    }
}

impl ArrayBuilderSupport for f64 {
    type Container = F64Array;

    fn with_capacity(capacity: usize) -> Self::Container {
        Self::Container::with_capacity(capacity)
    }

    fn push_some(array: &mut Self::Container, next: Self) {
        array.push(Some(next))
    }

    fn push_none(array: &mut Self::Container) {
        array.push(None)
    }

    fn as_array(array: Self::Container) -> Array {
        array.as_array()
    }
}

impl ArrayBuilderSupport for &str {
    type Container = StringArray;

    fn with_capacity(capacity: usize) -> Self::Container {
        Self::Container::with_capacity(capacity)
    }

    fn push_some(array: &mut Self::Container, next: Self) {
        array.push(Some(next))
    }

    fn push_none(array: &mut Self::Container) {
        array.push(None)
    }

    fn as_array(array: Self::Container) -> Array {
        array.as_array()
    }
}

impl ArrayBuilderSupport for String {
    type Container = StringArray;

    fn with_capacity(capacity: usize) -> Self::Container {
        Self::Container::with_capacity(capacity)
    }

    fn push_some(array: &mut Self::Container, next: Self) {
        array.push(Some(next.as_str()))
    }

    fn push_none(array: &mut Self::Container) {
        array.push(None)
    }

    fn as_array(array: Self::Container) -> Array {
        array.as_array()
    }
}

impl ArrayBuilderSupport for Date<Utc> {
    type Container = DateArray;

    fn with_capacity(capacity: usize) -> Self::Container {
        Self::Container::with_capacity(capacity)
    }

    fn push_some(array: &mut Self::Container, next: Self) {
        array.push(Some(epoch_date(next)))
    }

    fn push_none(array: &mut Self::Container) {
        array.push(None)
    }

    fn as_array(array: Self::Container) -> Array {
        array.as_array()
    }
}

impl ArrayBuilderSupport for DateTime<Utc> {
    type Container = TimestampArray;

    fn with_capacity(capacity: usize) -> Self::Container {
        Self::Container::with_capacity(capacity)
    }

    fn push_some(array: &mut Self::Container, next: Self) {
        array.push(Some(epoch_micros(next)))
    }

    fn push_none(array: &mut Self::Container) {
        array.push(None)
    }

    fn as_array(array: Self::Container) -> Array {
        array.as_array()
    }
}

trait BinaryCastSupport {
    fn as_bool(self) -> (BoolArray, BoolArray);
    fn as_i64(self) -> (I64Array, I64Array);
    fn as_f64(self) -> (F64Array, F64Array);
    fn as_date(self) -> (DateArray, DateArray);
    fn as_timestamp(self) -> (TimestampArray, TimestampArray);
    fn as_string(self) -> (StringArray, StringArray);
}

impl BinaryCastSupport for (Array, Array) {
    fn as_bool(self) -> (BoolArray, BoolArray) {
        (self.0.as_bool(), self.1.as_bool())
    }
    fn as_i64(self) -> (I64Array, I64Array) {
        (self.0.as_i64(), self.1.as_i64())
    }
    fn as_f64(self) -> (F64Array, F64Array) {
        (self.0.as_f64(), self.1.as_f64())
    }
    fn as_date(self) -> (DateArray, DateArray) {
        (self.0.as_date(), self.1.as_date())
    }
    fn as_timestamp(self) -> (TimestampArray, TimestampArray) {
        (self.0.as_timestamp(), self.1.as_timestamp())
    }
    fn as_string(self) -> (StringArray, StringArray) {
        (self.0.as_string(), self.1.as_string())
    }
}

trait BinaryOperatorSupport<'a> {
    type Left;
    type Right;

    fn len(&'a self) -> usize;

    fn get(&'a self, i: usize) -> Option<(Self::Left, Self::Right)>;

    fn binary<T: ArrayBuilderSupport>(&'a self, f: impl Fn(Self::Left, Self::Right) -> T) -> Array {
        let mut builder = T::with_capacity(self.len());
        for i in 0..self.len() {
            if let Some((a, b)) = self.get(i) {
                T::push_some(&mut builder, f(a, b));
            } else {
                T::push_none(&mut builder);
            }
        }
        T::as_array(builder)
    }

    fn binary_nullable<T: ArrayBuilderSupport>(
        &'a self,
        f: impl Fn(Self::Left, Self::Right) -> Option<T>,
    ) -> Array {
        let mut builder = T::with_capacity(self.len());
        for i in 0..self.len() {
            if let Some((a, b)) = self.get(i) {
                if let Some(c) = f(a, b) {
                    T::push_some(&mut builder, c);
                } else {
                    T::push_none(&mut builder);
                }
            } else {
                T::push_none(&mut builder);
            }
        }
        T::as_array(builder)
    }
}

impl<'a> BinaryOperatorSupport<'a> for (I64Array, I64Array) {
    type Left = i64;
    type Right = i64;

    fn len(&'a self) -> usize {
        self.0.len()
    }

    fn get(&'a self, i: usize) -> Option<(Self::Left, Self::Right)> {
        let a = self.0.get(i);
        let b = self.1.get(i);
        a.zip(b)
    }
}

impl<'a> BinaryOperatorSupport<'a> for (F64Array, F64Array) {
    type Left = f64;
    type Right = f64;

    fn len(&'a self) -> usize {
        self.0.len()
    }

    fn get(&'a self, i: usize) -> Option<(Self::Left, Self::Right)> {
        let a = self.0.get(i);
        let b = self.1.get(i);
        a.zip(b)
    }
}

impl<'a> BinaryOperatorSupport<'a> for (F64Array, I64Array) {
    type Left = f64;
    type Right = i64;

    fn len(&'a self) -> usize {
        self.0.len()
    }

    fn get(&'a self, i: usize) -> Option<(Self::Left, Self::Right)> {
        let a = self.0.get(i);
        let b = self.1.get(i);
        a.zip(b)
    }
}

impl<'a> BinaryOperatorSupport<'a> for (DateArray, DateArray) {
    type Left = i32;
    type Right = i32;

    fn len(&'a self) -> usize {
        self.0.len()
    }

    fn get(&'a self, i: usize) -> Option<(Self::Left, Self::Right)> {
        let a = self.0.get(i);
        let b = self.1.get(i);
        a.zip(b)
    }
}

impl<'a> BinaryOperatorSupport<'a> for (DateArray, I64Array) {
    type Left = i32;
    type Right = i64;

    fn len(&'a self) -> usize {
        self.0.len()
    }

    fn get(&'a self, i: usize) -> Option<(Self::Left, Self::Right)> {
        let a = self.0.get(i);
        let b = self.1.get(i);
        a.zip(b)
    }
}

impl<'a> BinaryOperatorSupport<'a> for (DateArray, StringArray) {
    type Left = i32;
    type Right = &'a str;

    fn len(&'a self) -> usize {
        self.0.len()
    }

    fn get(&'a self, i: usize) -> Option<(Self::Left, Self::Right)> {
        let a = self.0.get(i);
        let b = self.1.get(i);
        a.zip(b)
    }
}

impl<'a> BinaryOperatorSupport<'a> for (TimestampArray, TimestampArray) {
    type Left = i64;
    type Right = i64;

    fn len(&'a self) -> usize {
        self.0.len()
    }

    fn get(&'a self, i: usize) -> Option<(Self::Left, Self::Right)> {
        let a = self.0.get(i);
        let b = self.1.get(i);
        a.zip(b)
    }
}

impl<'a> BinaryOperatorSupport<'a> for (TimestampArray, I64Array) {
    type Left = i64;
    type Right = i64;

    fn len(&'a self) -> usize {
        self.0.len()
    }

    fn get(&'a self, i: usize) -> Option<(Self::Left, Self::Right)> {
        let a = self.0.get(i);
        let b = self.1.get(i);
        a.zip(b)
    }
}

impl<'a> BinaryOperatorSupport<'a> for (TimestampArray, StringArray) {
    type Left = i64;
    type Right = &'a str;

    fn len(&'a self) -> usize {
        self.0.len()
    }

    fn get(&'a self, i: usize) -> Option<(Self::Left, Self::Right)> {
        let a = self.0.get(i);
        let b = self.1.get(i);
        a.zip(b)
    }
}

impl<'a> BinaryOperatorSupport<'a> for (StringArray, StringArray) {
    type Left = &'a str;
    type Right = &'a str;

    fn len(&'a self) -> usize {
        self.0.len()
    }

    fn get(&'a self, i: usize) -> Option<(Self::Left, Self::Right)> {
        let a = self.0.get(i);
        let b = self.1.get(i);
        a.zip(b)
    }
}

impl<'a> BinaryOperatorSupport<'a> for (StringArray, I64Array) {
    type Left = &'a str;
    type Right = i64;

    fn len(&'a self) -> usize {
        self.0.len()
    }

    fn get(&'a self, i: usize) -> Option<(Self::Left, Self::Right)> {
        let a = self.0.get(i);
        let b = self.1.get(i);
        a.zip(b)
    }
}

impl<'a> BinaryOperatorSupport<'a> for (StringArray, DateArray) {
    type Left = &'a str;
    type Right = i32;

    fn len(&'a self) -> usize {
        self.0.len()
    }

    fn get(&'a self, i: usize) -> Option<(Self::Left, Self::Right)> {
        let a = self.0.get(i);
        let b = self.1.get(i);
        a.zip(b)
    }
}

impl<'a> BinaryOperatorSupport<'a> for (StringArray, TimestampArray) {
    type Left = &'a str;
    type Right = i64;

    fn len(&'a self) -> usize {
        self.0.len()
    }

    fn get(&'a self, i: usize) -> Option<(Self::Left, Self::Right)> {
        let a = self.0.get(i);
        let b = self.1.get(i);
        a.zip(b)
    }
}

trait TernaryOperatorSupport<'a> {
    type A;
    type B;
    type C;

    fn len(&'a self) -> usize;

    fn get(&'a self, i: usize) -> Option<(Self::A, Self::B, Self::C)>;

    fn ternary<T: ArrayBuilderSupport>(
        &'a self,
        f: impl Fn(Self::A, Self::B, Self::C) -> T,
    ) -> Array {
        let mut builder = T::with_capacity(self.len());
        for i in 0..self.len() {
            if let Some((a, b, c)) = self.get(i) {
                T::push_some(&mut builder, f(a, b, c));
            } else {
                T::push_none(&mut builder);
            }
        }
        T::as_array(builder)
    }
}

impl<'a> TernaryOperatorSupport<'a> for (I64Array, I64Array, I64Array) {
    type A = i64;
    type B = i64;
    type C = i64;

    fn len(&'a self) -> usize {
        self.0.len()
    }

    fn get(&'a self, i: usize) -> Option<(Self::A, Self::B, Self::C)> {
        match (self.0.get(i), self.1.get(i), self.2.get(i)) {
            (Some(a), Some(b), Some(c)) => Some((a, b, c)),
            _ => None,
        }
    }
}

impl<'a> TernaryOperatorSupport<'a> for (StringArray, StringArray, StringArray) {
    type A = &'a str;
    type B = &'a str;
    type C = &'a str;

    fn len(&'a self) -> usize {
        self.0.len()
    }

    fn get(&'a self, i: usize) -> Option<(Self::A, Self::B, Self::C)> {
        match (self.0.get(i), self.1.get(i), self.2.get(i)) {
            (Some(a), Some(b), Some(c)) => Some((a, b, c)),
            _ => None,
        }
    }
}

impl<'a> TernaryOperatorSupport<'a> for (StringArray, I64Array, StringArray) {
    type A = &'a str;
    type B = i64;
    type C = &'a str;

    fn len(&'a self) -> usize {
        self.0.len()
    }

    fn get(&'a self, i: usize) -> Option<(Self::A, Self::B, Self::C)> {
        match (self.0.get(i), self.1.get(i), self.2.get(i)) {
            (Some(a), Some(b), Some(c)) => Some((a, b, c)),
            _ => None,
        }
    }
}

impl<'a> TernaryOperatorSupport<'a> for (StringArray, I64Array, I64Array) {
    type A = &'a str;
    type B = i64;
    type C = i64;

    fn len(&'a self) -> usize {
        self.0.len()
    }

    fn get(&'a self, i: usize) -> Option<(Self::A, Self::B, Self::C)> {
        match (self.0.get(i), self.1.get(i), self.2.get(i)) {
            (Some(a), Some(b), Some(c)) => Some((a, b, c)),
            _ => None,
        }
    }
}
