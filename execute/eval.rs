use ast::*;
use chrono::*;
use kernel::*;
use regex::{Captures, Regex};

use crate::map::ArrayExt;

pub(crate) fn all(
    predicates: &Vec<Scalar>,
    input: &RecordBatch,
    txn: i64,
) -> Result<BoolArray, String> {
    let mut mask = BoolArray::trues(input.len());
    for p in predicates {
        mask = eval(p, &input, txn)?.as_bool().and(&mask);
    }
    Ok(mask)
}

pub(crate) fn eval(scalar: &Scalar, input: &RecordBatch, txn: i64) -> Result<AnyArray, String> {
    let a = match scalar {
        Scalar::Literal(value) => value.repeat(input.len()),
        Scalar::Column(column) => {
            let find = column.canonical_name();
            input.find(&find).expect(&find).clone()
        }
        Scalar::Call(function) => eval_function(function.as_ref(), input, txn)?,
        Scalar::Cast(scalar, data_type) => eval(scalar, input, txn)?.cast(*data_type),
    };
    Ok(a)
}

fn eval_function(function: &F, input: &RecordBatch, txn: i64) -> Result<AnyArray, String> {
    let e = |scalar| eval(scalar, input, txn);
    let es = |scalars: &Vec<Scalar>| -> Result<Vec<AnyArray>, String> {
        let mut arrays = vec![];
        for scalar in scalars {
            arrays.push(eval(scalar, input, txn)?)
        }
        Ok(arrays)
    };
    match function {
        F::CurrentDate | F::CurrentTimestamp => panic!(
            "{} should have been eliminated in the rewrite phase",
            function.name()
        ),
        F::Xid => Ok(I64Array::from_values(vec![txn])
            .repeat(input.len())
            .as_any()),
        F::Coalesce(varargs) => {
            let mut tail = e(varargs.last().unwrap())?;
            for head in &varargs[..varargs.len() - 1] {
                tail = e(head)?.coalesce(&tail);
            }
            Ok(tail)
        }
        F::ConcatString(varargs) => {
            let mut strings = vec![];
            for arg in varargs {
                strings.push(e(arg)?.as_string())
            }
            Ok(StringArray::concat(strings).as_any())
        }
        F::Hash(varargs) => Ok(AnyArray::I64(I64Array::hash_all(&es(varargs)?))),
        F::Greatest(varargs) => Ok(AnyArray::greatest(es(varargs)?)),
        F::Least(varargs) => Ok(AnyArray::least(es(varargs)?)),
        F::AbsDouble(a) => e(a)?.as_f64().map(f64::abs),
        F::AbsInt64(a) => e(a)?.as_i64().map(i64::abs),
        F::AcosDouble(a) => e(a)?.as_f64().map(f64::acos),
        F::AcoshDouble(a) => e(a)?.as_f64().map(f64::acosh),
        F::AsinDouble(a) => e(a)?.as_f64().map(f64::asin),
        F::AsinhDouble(a) => e(a)?.as_f64().map(f64::asinh),
        F::AtanDouble(a) => e(a)?.as_f64().map(f64::atan),
        F::AtanhDouble(a) => e(a)?.as_f64().map(f64::atanh),
        F::ByteLengthString(a) => e(a)?.as_string().map(|a: &str| a.len() as i64),
        F::CeilDouble(a) => e(a)?.as_f64().map(f64::ceil),
        F::CharLengthString(a) => e(a)?.as_string().map(|a: &str| a.chars().count() as i64),
        F::ChrString(a) => e(a)?.as_i64().map(chr),
        F::CosDouble(a) => e(a)?.as_f64().map(f64::cos),
        F::CoshDouble(a) => e(a)?.as_f64().map(f64::cosh),
        F::DateFromTimestamp(a) => e(a)?.as_timestamp().map(date_from_timestamp),
        F::DateFromUnixDate(a) => e(a)?.as_i64().map(date_from_unix_date),
        F::DecimalLogarithmDouble(a) => e(a)?.as_f64().map(f64::log10),
        F::Error(a) => {
            let message = e(a)?.as_string().get(0).clone().unwrap_or("".to_string());
            return Err(message);
        }
        F::ExpDouble(a) => e(a)?.as_f64().map(f64::exp),
        F::ExtractDateFromTimestamp(a) => e(a)?.as_timestamp().map(date_from_timestamp),
        F::FloorDouble(a) => e(a)?.as_f64().map(f64::floor),
        F::IsFalse(a) => e(a)?
            .as_bool()
            .map(|a: Option<bool>| Ok(Some(a == Some(false)))),
        F::IsInf(a) => e(a)?.as_f64().map(f64::is_infinite),
        F::IsNan(a) => e(a)?.as_f64().map(f64::is_nan),
        F::IsNull(a) => Ok(e(a)?.is_null().as_any()),
        F::IsTrue(a) => e(a)?
            .as_bool()
            .map(|a: Option<bool>| Ok(Some(a == Some(true)))),
        F::LengthString(a) => e(a)?.as_string().map(|a: &str| a.chars().count() as i64),
        F::LowerString(a) => e(a)?.as_string().map(|a: &str| a.to_lowercase()),
        F::NaturalLogarithmDouble(a) => e(a)?.as_f64().map(f64::ln),
        F::Not(a) => e(a)?.as_bool().map(|a: bool| !a),
        F::ReverseString(a) => e(a)?
            .as_string()
            .map(|a: Option<&str>| Ok(a.map(|a| a.chars().rev().collect::<String>()))),
        F::RoundDouble(a) => e(a)?.as_f64().map(f64::round),
        F::SignDouble(a) => e(a)?.as_f64().map(f64::signum),
        F::SignInt64(a) => e(a)?.as_i64().map(i64::signum),
        F::SinDouble(a) => e(a)?.as_f64().map(f64::sin),
        F::SinhDouble(a) => e(a)?.as_f64().map(f64::sinh),
        F::SqrtDouble(a) => e(a)?.as_f64().map(f64::sqrt),
        F::StringFromDate(a) => e(a)?.as_date().map(string_from_date),
        F::StringFromTimestamp(a) => e(a)?.as_timestamp().map(string_from_timestamp),
        F::TanDouble(a) => e(a)?.as_f64().map(f64::tan),
        F::TanhDouble(a) => e(a)?.as_f64().map(f64::tanh),
        F::TimestampFromDate(a) => e(a)?.as_date().map(timestamp_from_date),
        F::TimestampFromString(a) => e(a)?.as_string().map(timestamp_from_string),
        F::TimestampFromUnixMicrosInt64(a) => e(a)?.as_i64().map(timestamp),
        F::TruncDouble(a) => e(a)?.as_f64().map(f64::trunc),
        F::UnaryMinusDouble(a) => e(a)?.as_f64().map(|a: f64| -a),
        F::UnaryMinusInt64(a) => e(a)?.as_i64().map(|a: i64| -a),
        F::UnixDate(a) => e(a)?.as_date().map(|a| epoch_date(a) as i64),
        F::UnixMicrosFromTimestamp(a) => e(a)?.as_timestamp().map(|a| epoch_micros(a) as i64),
        F::UpperString(a) => e(a)?.as_string().map(|a: &str| a.to_uppercase()),
        F::DateTruncDate(a, date_part) => e(a)?.as_date().map(|a| date_trunc(a, *date_part)),
        F::ExtractFromDate(a, date_part) => {
            e(a)?.as_date().map(|a| extract_from_date(a, *date_part))
        }
        F::ExtractFromTimestamp(a, date_part) => e(a)?
            .as_timestamp()
            .map(|a| extract_from_timestamp(a, *date_part)),
        F::TimestampTrunc(a, date_part) => {
            e(a)?.as_timestamp().map(|a| timestamp_trunc(a, *date_part))
        }
        F::In(a, varargs) => Ok(e(a)?.equal_any(es(varargs)?).as_any()),
        F::AddDouble(a, b) => e(a)?
            .as_f64()
            .bi_map(&e(b)?.as_f64(), |a: f64, b: f64| a + b),
        F::AddInt64(a, b) => e(a)?
            .as_i64()
            .bi_map(&e(b)?.as_i64(), |a: i64, b: i64| a + b),
        F::And(a, b) => Ok(e(a)?.as_bool().and(&e(b)?.as_bool()).as_any()),
        F::Atan2Double(a, b) => e(a)?
            .as_f64()
            .bi_map(&e(b)?.as_f64(), |a: f64, b: f64| a.atan2(b)),
        F::DivideDouble(a, b) => e(a)?
            .as_f64()
            .bi_map(&e(b)?.as_f64(), |a: f64, b: f64| a / b),
        F::DivInt64(a, b) => e(a)?
            .as_i64()
            .bi_map(&e(b)?.as_i64(), |a: i64, b: i64| a / b),
        F::EndsWithString(a, b) => e(a)?
            .as_string()
            .bi_map(&e(b)?.as_string(), |a: &str, b: &str| a.ends_with(b)),
        F::Equal(a, b) => Ok(e(a)?.equal(&e(b)?).as_any()),
        F::FormatDate(a, b) => e(a)?
            .as_string()
            .bi_map(&e(b)?.as_date(), |a, b| format_date(a, b)),
        F::FormatTimestamp(a, b) => e(a)?
            .as_string()
            .bi_map(&e(b)?.as_timestamp(), |a, b| format_timestamp(a, b)),
        F::Greater(a, b) => Ok(e(a)?.greater(&e(b)?).as_any()),
        F::GreaterOrEqual(a, b) => Ok(e(a)?.greater_equal(&e(b)?).as_any()),
        F::Ifnull(a, b) => Ok(e(a)?.coalesce(&e(b)?)),
        F::Is(a, b) => Ok(e(a)?.is(&e(b)?).as_any()),
        F::LeftString(a, b) => e(a)?
            .as_string()
            .bi_map(&e(b)?.as_i64(), |a: &str, b: i64| {
                a.chars().take(b as usize).collect::<String>()
            }),
        F::Less(a, b) => Ok(e(a)?.less(&e(b)?).as_any()),
        F::LessOrEqual(a, b) => Ok(e(a)?.less_equal(&e(b)?).as_any()),
        F::LogarithmDouble(a, b) => e(a)?
            .as_f64()
            .bi_map(&e(b)?.as_f64(), |a: f64, b: f64| a.log(b)),
        F::LtrimString(a, None) => e(a)?.as_string().map(|a: &str| a.trim_start().to_string()),
        F::LtrimString(a, Some(b)) => e(a)?.as_string().bi_map(&e(b)?.as_string(), ltrim),
        F::ModInt64(a, b) => e(a)?
            .as_i64()
            .bi_map(&e(b)?.as_i64(), |a: i64, b: i64| a % b),
        F::MultiplyDouble(a, b) => e(a)?
            .as_f64()
            .bi_map(&e(b)?.as_f64(), |a: f64, b: f64| a * b),
        F::MultiplyInt64(a, b) => e(a)?
            .as_i64()
            .bi_map(&e(b)?.as_i64(), |a: i64, b: i64| a * b),
        F::NotEqual(a, b) => Ok(e(a)?.not_equal(&e(b)?).as_any()),
        F::Nullif(a, b) => Ok(e(a)?.null_if(&e(b)?)),
        F::Or(a, b) => Ok(e(a)?.as_bool().or(&e(b)?.as_bool()).as_any()),
        F::ParseDate(a, b) => e(a)?.as_string().bi_map(&e(b)?.as_string(), parse_date),
        F::ParseTimestamp(a, b) => e(a)?
            .as_string()
            .bi_map(&e(b)?.as_string(), parse_timestamp),
        F::PowDouble(a, b) => e(a)?.as_f64().bi_map(&e(b)?.as_f64(), f64::powf),
        F::RegexpContainsString(a, b) => e(a)?
            .as_string()
            .bi_map(&e(b)?.as_string(), regexp_contains),
        F::RegexpExtractString(a, b) => {
            e(a)?
                .as_string()
                .bi_map(&e(b)?.as_string(), |a, b| match (a, b) {
                    (Some(a), Some(b)) => Ok(regexp_extract(a, b)),
                    _ => Ok(None),
                })
        }
        F::RepeatString(a, b) => e(a)?
            .as_string()
            .bi_map(&e(b)?.as_i64(), |a: &str, b: i64| a.repeat(b as usize)),
        F::RightString(a, b) => e(a)?.as_string().bi_map(&e(b)?.as_i64(), right),
        F::RoundWithDigitsDouble(a, b) => e(a)?.as_f64().bi_map(&e(b)?.as_i64(), round),
        F::RtrimString(a, None) => e(a)?.as_string().map(|a: &str| a.trim_end().to_string()),
        F::RtrimString(a, Some(b)) => e(a)?.as_string().bi_map(&e(b)?.as_string(), rtrim),
        F::StartsWithString(a, b) => e(a)?
            .as_string()
            .bi_map(&e(b)?.as_string(), |a: &str, b: &str| a.starts_with(b)),
        F::StringLike(a, b) => e(a)?.as_string().bi_map(&e(b)?.as_string(), like),
        F::StrposString(a, b) => e(a)?.as_string().bi_map(&e(b)?.as_string(), strpos),
        F::SubtractDouble(a, b) => e(a)?
            .as_f64()
            .bi_map(&e(b)?.as_f64(), |a: f64, b: f64| a - b),
        F::SubtractInt64(a, b) => e(a)?
            .as_i64()
            .bi_map(&e(b)?.as_i64(), |a: i64, b: i64| a - b),
        F::TrimString(a, None) => e(a)?.as_string().map(|a: &str| a.trim().to_string()),
        F::TrimString(a, Some(b)) => e(a)?.as_string().bi_map(&e(b)?.as_string(), trim),
        F::TruncWithDigitsDouble(a, b) => e(a)?.as_f64().bi_map(&e(b)?.as_i64(), trunc),
        F::DateAddDate(a, b, date_part) => e(a)?
            .as_date()
            .bi_map(&e(b)?.as_i64(), |a, b| date_add(a, b, *date_part)),
        F::DateDiffDate(a, b, date_part) => e(a)?
            .as_date()
            .bi_map(&e(b)?.as_date(), |a, b| date_diff(a, b, *date_part)),
        F::DateSubDate(a, b, date_part) => e(a)?
            .as_date()
            .bi_map(&e(b)?.as_i64(), |a, b| date_sub(a, b, *date_part)),
        F::TimestampAdd(a, b, date_part) => e(a)?
            .as_timestamp()
            .bi_map(&e(b)?.as_i64(), |a, b| timestamp_add(a, b, *date_part)),
        F::TimestampDiff(a, b, date_part) => {
            e(a)?.as_timestamp().bi_map(&e(b)?.as_timestamp(), |a, b| {
                timestamp_diff(a, b, *date_part)
            })
        }
        F::TimestampSub(a, b, date_part) => e(a)?
            .as_timestamp()
            .bi_map(&e(b)?.as_i64(), |a, b| timestamp_sub(a, b, *date_part)),
        F::Between(a, b, c) => {
            let a = e(a)?;
            let b = e(b)?;
            let c = &e(c)?;
            let left = a.greater_equal(&b);
            let right = a.less_equal(&c);
            Ok(left.and(&right).as_any())
        }
        F::DateFromYearMonthDay(a, b, c) => {
            e(a)?
                .as_i64()
                .tri_map(&e(b)?.as_i64(), &e(c)?.as_i64(), |a, b, c| {
                    date_from_ymd(a, b, c)
                })
        }
        F::If(a, b, c) => Ok(e(a)?.as_bool().blend(&e(b)?, &e(c)?)),
        F::LpadString(a, b, c) => {
            e(a)?
                .as_string()
                .tri_map(&e(b)?.as_i64(), &e(c)?.as_string(), |a, b, c| lpad(a, b, c))
        }
        F::RegexpReplaceString(a, b, c) => e(a)?.as_string().tri_map(
            &e(b)?.as_string(),
            &e(c)?.as_string(),
            |a: &str, b: &str, c: &str| regexp_replace(a, b, c),
        ),
        F::ReplaceString(a, b, c) => e(a)?.as_string().tri_map(
            &e(b)?.as_string(),
            &e(c)?.as_string(),
            |a: &str, b: &str, c: &str| a.replace(b, c),
        ),
        F::RpadString(a, b, c) => {
            e(a)?
                .as_string()
                .tri_map(&e(b)?.as_i64(), &e(c)?.as_string(), |a, b, c| rpad(a, b, c))
        }
        F::SubstrString(a, b, None) => e(a)?
            .as_string()
            .bi_map(&e(b)?.as_i64(), |a, b| substr(a, b, i64::MAX)),
        F::SubstrString(a, b, Some(c)) => {
            e(a)?
                .as_string()
                .tri_map(&e(b)?.as_i64(), &e(c)?.as_i64(), |a, b, c| substr(a, b, c))
        }
        F::CaseNoValue(cases, default) => {
            let mut acc = e(default)?;
            for (test, value) in cases.iter().rev() {
                acc = e(test)?.as_bool().blend(&e(value)?, &acc);
            }
            Ok(acc)
        }
        F::CaseWithValue(head, cases, default) => {
            let mut acc = e(default)?;
            let expect = e(head)?;
            for (found, value) in cases.iter().rev() {
                acc = expect.equal(&e(found)?).blend(&e(value)?, &acc);
            }
            Ok(acc)
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

fn strpos(string: Option<&str>, substring: Option<&str>) -> Result<Option<i64>, String> {
    match (string, substring) {
        (Some(string), Some(substring)) => Ok(string.find(substring).map(|i| i as i64 + 1)),
        _ => Ok(None),
    }
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

pub(crate) fn regexp_replace(
    value: &str,
    regexp: &str,
    replacement: &str,
) -> Result<Option<String>, String> {
    let regexp = match Regex::new(regexp) {
        Ok(regexp) => regexp,
        Err(err) => return Err(err.to_string()),
    };
    if let Some(err) = check_replacement(replacement) {
        return Err(err);
    }
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
    Ok(Some(regexp.replace_all(value, rewrite).to_string()))
}

fn check_replacement(replacement: &str) -> Option<String> {
    let mut i = 0;
    let chars: Vec<_> = replacement.chars().collect();
    while i < chars.len() {
        while i < chars.len() && chars[i] != '\\' {
            i += 1;
        }
        if i < chars.len() {
            i += 1;
            if i < chars.len() {
                let c = chars[i];
                if let Some(_) = c.to_digit(10) {
                    // Nothing to do
                } else if c == '\\' {
                    // Nothing to do
                } else {
                    return Some("Invalid REGEXP_REPLACE pattern".to_string());
                }
                i += 1;
            } else {
                return Some("REGEXP_REPLACE pattern ends with \\".to_string());
            }
        }
    }
    None
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

fn date_from_timestamp(value: DateTime<Utc>) -> Date<Utc> {
    value.date()
}

fn date_from_unix_date(value: i64) -> Date<Utc> {
    let naive = NaiveDate::from_ymd(1970, 1, 1) + Duration::days(value as i64);
    Utc.from_utc_date(&naive)
}

fn timestamp_from_date(value: Date<Utc>) -> DateTime<Utc> {
    value.and_hms(0, 0, 0)
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

fn string_from_date(value: Date<Utc>) -> String {
    value.format("%F").to_string()
}

fn string_from_timestamp(value: DateTime<Utc>) -> String {
    format_timestamp("%+", value)
}

fn format_date(format: &str, value: Date<Utc>) -> String {
    value.format(format).to_string()
}

fn format_timestamp(format: &str, value: DateTime<Utc>) -> String {
    value.format(format).to_string()
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
        DatePart::Nanosecond
        | DatePart::Microsecond
        | DatePart::Millisecond
        | DatePart::Second
        | DatePart::Minute
        | DatePart::Hour
        | DatePart::DayOfWeek
        | DatePart::DayOfYear => panic!("date_trunc(_, {:?}) is not supported", date_part),
    }
}

fn timestamp_trunc(
    ts: Option<DateTime<Utc>>,
    date_part: DatePart,
) -> Result<Option<DateTime<Utc>>, String> {
    if let Some(ts) = ts {
        let part = match date_part {
            DatePart::Nanosecond => {
                return Err("timestamp_trunc(_, NANOSECOND) is not supported".to_string())
            }
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
        };
        Ok(Some(part))
    } else {
        Ok(None)
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
        DatePart::Nanosecond
        | DatePart::Microsecond
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

fn extract_from_timestamp(
    ts: Option<DateTime<Utc>>,
    date_part: DatePart,
) -> Result<Option<i64>, String> {
    if let Some(ts) = ts {
        let part = match date_part {
            DatePart::Nanosecond => {
                return Err("extract(NANOSECOND from _) is not supported".to_string())
            }
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
        };
        Ok(Some(part))
    } else {
        Ok(None)
    }
}

const OVERFLOW: &'static str = "date_add/subtract overflowed";

pub(crate) fn date_add(
    date: Date<Utc>,
    amount: i64,
    date_part: DatePart,
) -> Result<Option<Date<Utc>>, String> {
    let ok = match date_part {
        DatePart::Day => date
            .checked_add_signed(Duration::days(amount))
            .ok_or(OVERFLOW.to_string())?,
        DatePart::Week(_) => date
            .checked_add_signed(Duration::days(amount * 7))
            .ok_or(OVERFLOW.to_string())?,
        DatePart::Month => add_months(date, amount).ok_or(OVERFLOW.to_string())?,
        DatePart::Quarter => return date_add(date, amount * 3, DatePart::Month),
        DatePart::Year => return date_add(date, amount * 12, DatePart::Month),
        DatePart::Nanosecond
        | DatePart::Microsecond
        | DatePart::Millisecond
        | DatePart::Second
        | DatePart::Minute
        | DatePart::Hour
        | DatePart::DayOfWeek
        | DatePart::DayOfYear
        | DatePart::IsoWeek
        | DatePart::IsoYear => panic!("date_add/subtract(_, {:?}) is not supported", date_part),
    };
    if is_overflow(ok) {
        return Err(OVERFLOW.to_string());
    }
    Ok(Some(ok))
}

fn add_months(date: Date<Utc>, amount: i64) -> Option<Date<Utc>> {
    let y = (date.year() as i64 * 12 + date.month0() as i64 + amount).div_euclid(12) as i32;
    let m = (date.month0() as i64 + amount).rem_euclid(12) as u32 + 1;
    let d = date.day().min(days_in_month(y, m));
    let naive = NaiveDate::from_ymd_opt(y, m, d)?;
    Some(Utc.from_utc_date(&naive))
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        9 | 4 | 6 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 29,
        _ => 31,
    }
}

fn is_leap_year(year: i32) -> bool {
    year % 4 == 0 && !(year % 100 == 0 && year % 400 != 0)
}

fn is_overflow(date: Date<Utc>) -> bool {
    let min = Utc.from_utc_date(&NaiveDate::from_ymd(1, 1, 1));
    let max = Utc.from_utc_date(&NaiveDate::from_ymd(9999, 12, 31));
    date < min || date > max
}

pub(crate) fn date_sub(
    date: Date<Utc>,
    amount: i64,
    date_part: DatePart,
) -> Result<Option<Date<Utc>>, String> {
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
        DatePart::Nanosecond
        | DatePart::Microsecond
        | DatePart::Millisecond
        | DatePart::Second
        | DatePart::Minute
        | DatePart::Hour
        | DatePart::DayOfWeek
        | DatePart::DayOfYear => panic!("date_diff(_, _, {:?}) is not supported", date_part),
    }
}

fn timestamp_add(
    ts: Option<DateTime<Utc>>,
    amount: Option<i64>,
    date_part: DatePart,
) -> Result<Option<DateTime<Utc>>, String> {
    match (ts, amount) {
        (Some(ts), Some(amount)) => Ok(Some(ts + timestamp_duration(amount, date_part)?)),
        _ => Ok(None),
    }
}

fn timestamp_sub(
    ts: Option<DateTime<Utc>>,
    amount: Option<i64>,
    date_part: DatePart,
) -> Result<Option<DateTime<Utc>>, String> {
    match (ts, amount) {
        (Some(ts), Some(amount)) => Ok(Some(ts - timestamp_duration(amount, date_part)?)),
        _ => Ok(None),
    }
}

fn timestamp_duration(amount: i64, date_part: DatePart) -> Result<Duration, String> {
    Ok(match date_part {
        DatePart::Nanosecond => {
            return Err("timestamp_add/subtract(_, NANOSECOND) is not supported".to_string())
        }
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
        | DatePart::IsoYear => {
            return Err(format!(
                "timestamp_add/subtract(_, {:?}) is not supported",
                date_part,
            ))
        }
    })
}

fn timestamp_diff(
    later: Option<DateTime<Utc>>,
    earlier: Option<DateTime<Utc>>,
    date_part: DatePart,
) -> Result<Option<i64>, String> {
    match (later, earlier) {
        (Some(later), Some(earlier)) => {
            let diff = match date_part {
                DatePart::Nanosecond => {
                    return Err("timestamp_diff(_, NANOSECOND) is not supported".to_string())
                }
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
                    panic!("timestamp_diff(_, _, {:?}) is not supported", date_part)
                }
            };
            Ok(Some(diff))
        }
        _ => Ok(None),
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
