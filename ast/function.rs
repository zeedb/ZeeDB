use kernel::*;
use serde::{Deserialize, Serialize};
use std::hash::Hash;

use crate::{DatePart, Scalar, Value};

// Functions appear in scalar expressions.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum F {
    CurrentDate,
    CurrentTimestamp,
    Xid,
    Coalesce(Vec<Scalar>),
    ConcatString(Vec<Scalar>),
    Hash(Vec<Scalar>),
    Greatest(Vec<Scalar>),
    Least(Vec<Scalar>),
    AbsDouble(Scalar),
    AbsInt64(Scalar),
    AcosDouble(Scalar),
    AcoshDouble(Scalar),
    AsinDouble(Scalar),
    AsinhDouble(Scalar),
    AtanDouble(Scalar),
    AtanhDouble(Scalar),
    ByteLengthString(Scalar),
    CeilDouble(Scalar),
    CharLengthString(Scalar),
    ChrString(Scalar),
    CosDouble(Scalar),
    CoshDouble(Scalar),
    DateFromTimestamp(Scalar),
    DateFromUnixDate(Scalar),
    DecimalLogarithmDouble(Scalar),
    ExpDouble(Scalar),
    ExtractDateFromTimestamp(Scalar),
    FloorDouble(Scalar),
    GetVar(Scalar),
    IsFalse(Scalar),
    IsInf(Scalar),
    IsNan(Scalar),
    IsNull(Scalar),
    IsTrue(Scalar),
    LengthString(Scalar),
    LowerString(Scalar),
    NaturalLogarithmDouble(Scalar),
    NextVal(Scalar),
    Not(Scalar),
    ReverseString(Scalar),
    RoundDouble(Scalar),
    SignDouble(Scalar),
    SignInt64(Scalar),
    SinDouble(Scalar),
    SinhDouble(Scalar),
    SqrtDouble(Scalar),
    StringFromDate(Scalar),
    StringFromTimestamp(Scalar),
    TanDouble(Scalar),
    TanhDouble(Scalar),
    TimestampFromDate(Scalar),
    TimestampFromString(Scalar),
    TimestampFromUnixMicrosInt64(Scalar),
    TruncDouble(Scalar),
    UnaryMinusDouble(Scalar),
    UnaryMinusInt64(Scalar),
    UnixDate(Scalar),
    UnixMicrosFromTimestamp(Scalar),
    UpperString(Scalar),
    DateTruncDate(Scalar, DatePart),
    ExtractFromDate(Scalar, DatePart),
    ExtractFromTimestamp(Scalar, DatePart),
    TimestampTrunc(Scalar, DatePart),
    In(Scalar, Vec<Scalar>),
    AddDouble(Scalar, Scalar),
    AddInt64(Scalar, Scalar),
    And(Scalar, Scalar),
    Atan2Double(Scalar, Scalar),
    DivideDouble(Scalar, Scalar),
    DivInt64(Scalar, Scalar),
    EndsWithString(Scalar, Scalar),
    Equal(Scalar, Scalar),
    FormatDate(Scalar, Scalar),
    FormatTimestamp(Scalar, Scalar),
    Greater(Scalar, Scalar),
    GreaterOrEqual(Scalar, Scalar),
    Ifnull(Scalar, Scalar),
    Is(Scalar, Scalar),
    LeftString(Scalar, Scalar),
    Less(Scalar, Scalar),
    LessOrEqual(Scalar, Scalar),
    LogarithmDouble(Scalar, Scalar),
    LtrimString(Scalar, Option<Scalar>),
    ModInt64(Scalar, Scalar),
    MultiplyDouble(Scalar, Scalar),
    MultiplyInt64(Scalar, Scalar),
    NotEqual(Scalar, Scalar),
    Nullif(Scalar, Scalar),
    Or(Scalar, Scalar),
    ParseDate(Scalar, Scalar),
    ParseTimestamp(Scalar, Scalar),
    PowDouble(Scalar, Scalar),
    RegexpContainsString(Scalar, Scalar),
    RegexpExtractString(Scalar, Scalar),
    RepeatString(Scalar, Scalar),
    RightString(Scalar, Scalar),
    RoundWithDigitsDouble(Scalar, Scalar),
    RtrimString(Scalar, Option<Scalar>),
    StartsWithString(Scalar, Scalar),
    StringLike(Scalar, Scalar),
    StrposString(Scalar, Scalar),
    SubtractDouble(Scalar, Scalar),
    SubtractInt64(Scalar, Scalar),
    TrimString(Scalar, Option<Scalar>),
    TruncWithDigitsDouble(Scalar, Scalar),
    DateAddDate(Scalar, Scalar, DatePart),
    DateDiffDate(Scalar, Scalar, DatePart),
    DateSubDate(Scalar, Scalar, DatePart),
    TimestampAdd(Scalar, Scalar, DatePart),
    TimestampDiff(Scalar, Scalar, DatePart),
    TimestampSub(Scalar, Scalar, DatePart),
    Between(Scalar, Scalar, Scalar),
    DateFromYearMonthDay(Scalar, Scalar, Scalar),
    If(Scalar, Scalar, Scalar),
    LpadString(Scalar, Scalar, Scalar),
    RegexpReplaceString(Scalar, Scalar, Scalar),
    ReplaceString(Scalar, Scalar, Scalar),
    RpadString(Scalar, Scalar, Scalar),
    SubstrString(Scalar, Scalar, Option<Scalar>),
    CaseNoValue(Vec<(Scalar, Scalar)>, Scalar),
    CaseWithValue(Scalar, Vec<(Scalar, Scalar)>, Scalar),
}

impl F {
    pub fn len(&self) -> usize {
        match self {
            F::CurrentDate | F::CurrentTimestamp | F::Xid => 0,
            F::AbsDouble(_)
            | F::AbsInt64(_)
            | F::AcosDouble(_)
            | F::AcoshDouble(_)
            | F::AsinDouble(_)
            | F::AsinhDouble(_)
            | F::AtanDouble(_)
            | F::AtanhDouble(_)
            | F::ByteLengthString(_)
            | F::CeilDouble(_)
            | F::CharLengthString(_)
            | F::ChrString(_)
            | F::CosDouble(_)
            | F::CoshDouble(_)
            | F::DateFromTimestamp(_)
            | F::DateFromUnixDate(_)
            | F::DecimalLogarithmDouble(_)
            | F::ExpDouble(_)
            | F::ExtractDateFromTimestamp(_)
            | F::FloorDouble(_)
            | F::GetVar(_)
            | F::IsFalse(_)
            | F::IsInf(_)
            | F::IsNan(_)
            | F::IsNull(_)
            | F::IsTrue(_)
            | F::LengthString(_)
            | F::LowerString(_)
            | F::NaturalLogarithmDouble(_)
            | F::NextVal(_)
            | F::Not(_)
            | F::ReverseString(_)
            | F::RoundDouble(_)
            | F::SignDouble(_)
            | F::SignInt64(_)
            | F::SinDouble(_)
            | F::SinhDouble(_)
            | F::SqrtDouble(_)
            | F::StringFromDate(_)
            | F::StringFromTimestamp(_)
            | F::TanDouble(_)
            | F::TanhDouble(_)
            | F::TimestampFromDate(_)
            | F::TimestampFromString(_)
            | F::TimestampFromUnixMicrosInt64(_)
            | F::TruncDouble(_)
            | F::UnaryMinusDouble(_)
            | F::UnaryMinusInt64(_)
            | F::UnixDate(_)
            | F::UnixMicrosFromTimestamp(_)
            | F::UpperString(_)
            | F::DateTruncDate(_, _)
            | F::ExtractFromDate(_, _)
            | F::ExtractFromTimestamp(_, _)
            | F::TrimString(_, None)
            | F::TimestampTrunc(_, _)
            | F::LtrimString(_, None)
            | F::RtrimString(_, None) => 1,
            F::AddDouble(_, _)
            | F::AddInt64(_, _)
            | F::And(_, _)
            | F::Atan2Double(_, _)
            | F::DivideDouble(_, _)
            | F::DivInt64(_, _)
            | F::EndsWithString(_, _)
            | F::Equal(_, _)
            | F::FormatDate(_, _)
            | F::FormatTimestamp(_, _)
            | F::Greater(_, _)
            | F::GreaterOrEqual(_, _)
            | F::Ifnull(_, _)
            | F::Is(_, _)
            | F::LeftString(_, _)
            | F::Less(_, _)
            | F::LessOrEqual(_, _)
            | F::LogarithmDouble(_, _)
            | F::LtrimString(_, Some(_))
            | F::ModInt64(_, _)
            | F::MultiplyDouble(_, _)
            | F::MultiplyInt64(_, _)
            | F::NotEqual(_, _)
            | F::Nullif(_, _)
            | F::Or(_, _)
            | F::ParseDate(_, _)
            | F::ParseTimestamp(_, _)
            | F::PowDouble(_, _)
            | F::RegexpContainsString(_, _)
            | F::RegexpExtractString(_, _)
            | F::RepeatString(_, _)
            | F::RightString(_, _)
            | F::RoundWithDigitsDouble(_, _)
            | F::RtrimString(_, Some(_))
            | F::StartsWithString(_, _)
            | F::StringLike(_, _)
            | F::StrposString(_, _)
            | F::SubtractDouble(_, _)
            | F::SubtractInt64(_, _)
            | F::TrimString(_, Some(_))
            | F::TruncWithDigitsDouble(_, _)
            | F::DateAddDate(_, _, _)
            | F::DateDiffDate(_, _, _)
            | F::DateSubDate(_, _, _)
            | F::TimestampAdd(_, _, _)
            | F::TimestampDiff(_, _, _)
            | F::TimestampSub(_, _, _)
            | F::SubstrString(_, _, None) => 2,
            F::Between(_, _, _)
            | F::DateFromYearMonthDay(_, _, _)
            | F::If(_, _, _)
            | F::LpadString(_, _, _)
            | F::RegexpReplaceString(_, _, _)
            | F::ReplaceString(_, _, _)
            | F::RpadString(_, _, _)
            | F::SubstrString(_, _, Some(_)) => 3,
            F::Coalesce(varargs)
            | F::ConcatString(varargs)
            | F::Hash(varargs)
            | F::Greatest(varargs)
            | F::Least(varargs) => varargs.len(),
            F::In(_, varargs) => varargs.len() + 1,
            F::CaseNoValue(varargs, _) => varargs.len() * 2 + 1,
            F::CaseWithValue(_, varargs, _) => varargs.len() * 2 + 2,
        }
    }
}

impl std::ops::Index<usize> for F {
    type Output = Scalar;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            F::CurrentDate | F::CurrentTimestamp | F::Xid => panic!("{}", index),
            F::AbsDouble(a)
            | F::AbsInt64(a)
            | F::AcosDouble(a)
            | F::AcoshDouble(a)
            | F::AsinDouble(a)
            | F::AsinhDouble(a)
            | F::AtanDouble(a)
            | F::AtanhDouble(a)
            | F::ByteLengthString(a)
            | F::CeilDouble(a)
            | F::CharLengthString(a)
            | F::ChrString(a)
            | F::CosDouble(a)
            | F::CoshDouble(a)
            | F::DateFromTimestamp(a)
            | F::DateFromUnixDate(a)
            | F::DecimalLogarithmDouble(a)
            | F::ExpDouble(a)
            | F::ExtractDateFromTimestamp(a)
            | F::FloorDouble(a)
            | F::GetVar(a)
            | F::IsFalse(a)
            | F::IsInf(a)
            | F::IsNan(a)
            | F::IsNull(a)
            | F::IsTrue(a)
            | F::LengthString(a)
            | F::LowerString(a)
            | F::NaturalLogarithmDouble(a)
            | F::NextVal(a)
            | F::Not(a)
            | F::ReverseString(a)
            | F::RoundDouble(a)
            | F::SignDouble(a)
            | F::SignInt64(a)
            | F::SinDouble(a)
            | F::SinhDouble(a)
            | F::SqrtDouble(a)
            | F::StringFromDate(a)
            | F::StringFromTimestamp(a)
            | F::TanDouble(a)
            | F::TanhDouble(a)
            | F::TimestampFromDate(a)
            | F::TimestampFromString(a)
            | F::TimestampFromUnixMicrosInt64(a)
            | F::TruncDouble(a)
            | F::UnaryMinusDouble(a)
            | F::UnaryMinusInt64(a)
            | F::UnixDate(a)
            | F::UnixMicrosFromTimestamp(a)
            | F::UpperString(a)
            | F::DateTruncDate(a, _)
            | F::ExtractFromDate(a, _)
            | F::ExtractFromTimestamp(a, _)
            | F::TrimString(a, None)
            | F::TimestampTrunc(a, _)
            | F::LtrimString(a, None)
            | F::RtrimString(a, None) => match index {
                0 => a,
                _ => panic!("{}", index),
            },
            F::AddDouble(a, b)
            | F::AddInt64(a, b)
            | F::And(a, b)
            | F::Atan2Double(a, b)
            | F::DivideDouble(a, b)
            | F::DivInt64(a, b)
            | F::EndsWithString(a, b)
            | F::Equal(a, b)
            | F::FormatDate(a, b)
            | F::FormatTimestamp(a, b)
            | F::Greater(a, b)
            | F::GreaterOrEqual(a, b)
            | F::Ifnull(a, b)
            | F::Is(a, b)
            | F::LeftString(a, b)
            | F::Less(a, b)
            | F::LessOrEqual(a, b)
            | F::LogarithmDouble(a, b)
            | F::LtrimString(a, Some(b))
            | F::ModInt64(a, b)
            | F::MultiplyDouble(a, b)
            | F::MultiplyInt64(a, b)
            | F::NotEqual(a, b)
            | F::Nullif(a, b)
            | F::Or(a, b)
            | F::ParseDate(a, b)
            | F::ParseTimestamp(a, b)
            | F::PowDouble(a, b)
            | F::RegexpContainsString(a, b)
            | F::RegexpExtractString(a, b)
            | F::RepeatString(a, b)
            | F::RightString(a, b)
            | F::RoundWithDigitsDouble(a, b)
            | F::RtrimString(a, Some(b))
            | F::StartsWithString(a, b)
            | F::StringLike(a, b)
            | F::StrposString(a, b)
            | F::SubtractDouble(a, b)
            | F::SubtractInt64(a, b)
            | F::TrimString(a, Some(b))
            | F::TruncWithDigitsDouble(a, b)
            | F::DateAddDate(a, b, _)
            | F::DateDiffDate(a, b, _)
            | F::DateSubDate(a, b, _)
            | F::TimestampAdd(a, b, _)
            | F::TimestampDiff(a, b, _)
            | F::TimestampSub(a, b, _)
            | F::SubstrString(a, b, None) => match index {
                0 => a,
                1 => b,
                _ => panic!("{}", index),
            },
            F::Between(a, b, c)
            | F::DateFromYearMonthDay(a, b, c)
            | F::If(a, b, c)
            | F::LpadString(a, b, c)
            | F::RegexpReplaceString(a, b, c)
            | F::ReplaceString(a, b, c)
            | F::RpadString(a, b, c)
            | F::SubstrString(a, b, Some(c)) => match index {
                0 => a,
                1 => b,
                2 => c,
                _ => panic!("{}", index),
            },
            F::Coalesce(varargs)
            | F::ConcatString(varargs)
            | F::Hash(varargs)
            | F::Greatest(varargs)
            | F::Least(varargs) => &varargs[index],
            F::In(a, varargs) => match index {
                0 => a,
                _ => &varargs[index - 1],
            },
            F::CaseNoValue(varargs, default) => {
                if index < varargs.len() * 2 {
                    let (a, b) = &varargs[index / 2];
                    match index % 2 {
                        0 => a,
                        1 => b,
                        _ => panic!(),
                    }
                } else if index == varargs.len() * 2 {
                    default
                } else {
                    panic!("{}", index)
                }
            }
            F::CaseWithValue(value, varargs, default) => {
                if index == 0 {
                    value
                } else if index - 1 < varargs.len() * 2 {
                    let (a, b) = &varargs[(index - 1) / 2];
                    match (index - 1) % 2 {
                        0 => a,
                        1 => b,
                        _ => panic!(),
                    }
                } else if (index - 1) == varargs.len() * 2 {
                    default
                } else {
                    panic!("{}", index)
                }
            }
        }
    }
}

impl std::ops::IndexMut<usize> for F {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        match self {
            F::CurrentDate | F::CurrentTimestamp | F::Xid => panic!("{}", index),
            F::AbsDouble(a)
            | F::AbsInt64(a)
            | F::AcosDouble(a)
            | F::AcoshDouble(a)
            | F::AsinDouble(a)
            | F::AsinhDouble(a)
            | F::AtanDouble(a)
            | F::AtanhDouble(a)
            | F::ByteLengthString(a)
            | F::CeilDouble(a)
            | F::CharLengthString(a)
            | F::ChrString(a)
            | F::CosDouble(a)
            | F::CoshDouble(a)
            | F::DateFromTimestamp(a)
            | F::DateFromUnixDate(a)
            | F::DecimalLogarithmDouble(a)
            | F::ExpDouble(a)
            | F::ExtractDateFromTimestamp(a)
            | F::FloorDouble(a)
            | F::GetVar(a)
            | F::IsFalse(a)
            | F::IsInf(a)
            | F::IsNan(a)
            | F::IsNull(a)
            | F::IsTrue(a)
            | F::LengthString(a)
            | F::LowerString(a)
            | F::NaturalLogarithmDouble(a)
            | F::NextVal(a)
            | F::Not(a)
            | F::ReverseString(a)
            | F::RoundDouble(a)
            | F::SignDouble(a)
            | F::SignInt64(a)
            | F::SinDouble(a)
            | F::SinhDouble(a)
            | F::SqrtDouble(a)
            | F::StringFromDate(a)
            | F::StringFromTimestamp(a)
            | F::TanDouble(a)
            | F::TanhDouble(a)
            | F::TimestampFromDate(a)
            | F::TimestampFromString(a)
            | F::TimestampFromUnixMicrosInt64(a)
            | F::TruncDouble(a)
            | F::UnaryMinusDouble(a)
            | F::UnaryMinusInt64(a)
            | F::UnixDate(a)
            | F::UnixMicrosFromTimestamp(a)
            | F::UpperString(a)
            | F::DateTruncDate(a, _)
            | F::ExtractFromDate(a, _)
            | F::ExtractFromTimestamp(a, _)
            | F::TrimString(a, None)
            | F::TimestampTrunc(a, _)
            | F::LtrimString(a, None)
            | F::RtrimString(a, None) => match index {
                0 => a,
                _ => panic!("{}", index),
            },
            F::AddDouble(a, b)
            | F::AddInt64(a, b)
            | F::And(a, b)
            | F::Atan2Double(a, b)
            | F::DivideDouble(a, b)
            | F::DivInt64(a, b)
            | F::EndsWithString(a, b)
            | F::Equal(a, b)
            | F::FormatDate(a, b)
            | F::FormatTimestamp(a, b)
            | F::Greater(a, b)
            | F::GreaterOrEqual(a, b)
            | F::Ifnull(a, b)
            | F::Is(a, b)
            | F::LeftString(a, b)
            | F::Less(a, b)
            | F::LessOrEqual(a, b)
            | F::LogarithmDouble(a, b)
            | F::LtrimString(a, Some(b))
            | F::ModInt64(a, b)
            | F::MultiplyDouble(a, b)
            | F::MultiplyInt64(a, b)
            | F::NotEqual(a, b)
            | F::Nullif(a, b)
            | F::Or(a, b)
            | F::ParseDate(a, b)
            | F::ParseTimestamp(a, b)
            | F::PowDouble(a, b)
            | F::RegexpContainsString(a, b)
            | F::RegexpExtractString(a, b)
            | F::RepeatString(a, b)
            | F::RightString(a, b)
            | F::RoundWithDigitsDouble(a, b)
            | F::RtrimString(a, Some(b))
            | F::StartsWithString(a, b)
            | F::StringLike(a, b)
            | F::StrposString(a, b)
            | F::SubtractDouble(a, b)
            | F::SubtractInt64(a, b)
            | F::TrimString(a, Some(b))
            | F::TruncWithDigitsDouble(a, b)
            | F::DateAddDate(a, b, _)
            | F::DateDiffDate(a, b, _)
            | F::DateSubDate(a, b, _)
            | F::TimestampAdd(a, b, _)
            | F::TimestampDiff(a, b, _)
            | F::TimestampSub(a, b, _)
            | F::SubstrString(a, b, None) => match index {
                0 => a,
                1 => b,
                _ => panic!("{}", index),
            },
            F::Between(a, b, c)
            | F::DateFromYearMonthDay(a, b, c)
            | F::If(a, b, c)
            | F::LpadString(a, b, c)
            | F::RegexpReplaceString(a, b, c)
            | F::ReplaceString(a, b, c)
            | F::RpadString(a, b, c)
            | F::SubstrString(a, b, Some(c)) => match index {
                0 => a,
                1 => b,
                2 => c,
                _ => panic!("{}", index),
            },
            F::Coalesce(varargs)
            | F::ConcatString(varargs)
            | F::Hash(varargs)
            | F::Greatest(varargs)
            | F::Least(varargs) => &mut varargs[index],
            F::In(a, varargs) => match index {
                0 => a,
                _ => &mut varargs[index - 1],
            },
            F::CaseNoValue(varargs, default) => {
                if index < varargs.len() * 2 {
                    let (a, b) = &mut varargs[index / 2];
                    match index % 2 {
                        0 => a,
                        1 => b,
                        _ => panic!(),
                    }
                } else if index == varargs.len() * 2 {
                    default
                } else {
                    panic!("{}", index)
                }
            }
            F::CaseWithValue(value, varargs, default) => {
                if index == 0 {
                    value
                } else if index - 1 < varargs.len() * 2 {
                    let (a, b) = &mut varargs[(index - 1) / 2];
                    match (index - 1) % 2 {
                        0 => a,
                        1 => b,
                        _ => panic!(),
                    }
                } else if (index - 1) == varargs.len() * 2 {
                    default
                } else {
                    panic!("{}", index)
                }
            }
        }
    }
}

fn thunk(args: Vec<Scalar>, f: impl Fn() -> F) -> F {
    assert_eq!(args.len(), 0);
    f()
}

fn unary(mut args: Vec<Scalar>, f: impl Fn(Scalar) -> F) -> F {
    assert_eq!(args.len(), 1);
    let a = args.pop().unwrap();
    f(a)
}

fn unary_date_part(mut args: Vec<Scalar>, f: impl Fn(Scalar, DatePart) -> F) -> F {
    assert_eq!(args.len(), 2);
    let b = DatePart::from_scalar(args.pop().unwrap());
    let a = args.pop().unwrap();
    f(a, b)
}

fn unary_vararg(mut args: Vec<Scalar>, f: impl Fn(Scalar, Vec<Scalar>) -> F) -> F {
    assert!(args.len() >= 2);
    f(args.remove(0), args)
}

fn binary(mut args: Vec<Scalar>, f: impl Fn(Scalar, Scalar) -> F) -> F {
    assert_eq!(args.len(), 2);
    let b = args.pop().unwrap();
    let a = args.pop().unwrap();
    f(a, b)
}

fn binary_date_part(mut args: Vec<Scalar>, f: impl Fn(Scalar, Scalar, DatePart) -> F) -> F {
    assert_eq!(args.len(), 3);
    let c = DatePart::from_scalar(args.pop().unwrap());
    let b = args.pop().unwrap();
    let a = args.pop().unwrap();
    f(a, b, c)
}

fn ternary(mut args: Vec<Scalar>, f: impl Fn(Scalar, Scalar, Scalar) -> F) -> F {
    assert_eq!(args.len(), 3);
    let c = args.pop().unwrap();
    let b = args.pop().unwrap();
    let a = args.pop().unwrap();
    f(a, b, c)
}

impl F {
    pub fn from(
        function: &zetasql::FunctionRefProto,
        signature: &zetasql::FunctionSignatureProto,
        mut args: Vec<Scalar>,
    ) -> Self {
        let name = function.name.as_ref().unwrap().as_str();
        let first_argument = signature
            .argument
            .first()
            .map(|argument| DataType::from(argument.r#type.as_ref().unwrap()));
        let returns = DataType::from(
            signature
                .return_type
                .as_ref()
                .unwrap()
                .r#type
                .as_ref()
                .unwrap(),
        );
        match name {
            "ZetaSQL:$add" if returns == DataType::F64 => binary(args, |a, b| F::AddDouble(a, b)),
            "ZetaSQL:$add" if returns == DataType::I64 => binary(args, |a, b| F::AddInt64(a, b)),
            "ZetaSQL:$and" => binary(args, |a, b| F::And(a, b)),
            "ZetaSQL:$case_no_value" => {
                let mut cases = vec![];
                while args.len() > 1 {
                    cases.push((args.remove(0), args.remove(0)))
                }
                let default = args.pop().unwrap();
                F::CaseNoValue(cases, default)
            }
            "ZetaSQL:$case_with_value" => {
                let value = args.remove(0);
                let mut cases = vec![];
                while args.len() > 1 {
                    cases.push((args.remove(0), args.remove(0)))
                }
                let default = args.pop().unwrap();
                F::CaseWithValue(value, cases, default)
            }
            "ZetaSQL:$divide" => binary(args, |a, b| F::DivideDouble(a, b)),
            "ZetaSQL:$greater" => binary(args, |a, b| F::Greater(a, b)),
            "ZetaSQL:$greater_or_equal" => binary(args, |a, b| F::GreaterOrEqual(a, b)),
            "ZetaSQL:$less" => binary(args, |a, b| F::Less(a, b)),
            "ZetaSQL:$less_or_equal" => binary(args, |a, b| F::LessOrEqual(a, b)),
            "ZetaSQL:$equal" => binary(args, |a, b| F::Equal(a, b)),
            "ZetaSQL:$like" => binary(args, |a, b| F::StringLike(a, b)),
            "ZetaSQL:$in" => unary_vararg(args, |a, varargs| F::In(a, varargs)),
            "ZetaSQL:$between" => ternary(args, |a, b, c| F::Between(a, b, c)),
            "ZetaSQL:$is_null" => unary(args, |a| F::IsNull(a)),
            "ZetaSQL:$is_true" => unary(args, |a| F::IsTrue(a)),
            "ZetaSQL:$is_false" => unary(args, |a| F::IsFalse(a)),
            "ZetaSQL:$multiply" if returns == DataType::F64 => {
                binary(args, |a, b| F::MultiplyDouble(a, b))
            }
            "ZetaSQL:$multiply" if returns == DataType::I64 => {
                binary(args, |a, b| F::MultiplyInt64(a, b))
            }
            "ZetaSQL:$not" => unary(args, |a| F::Not(a)),
            "ZetaSQL:$not_equal" => binary(args, |a, b| F::NotEqual(a, b)),
            "ZetaSQL:$or" => binary(args, |a, b| F::Or(a, b)),
            "ZetaSQL:$subtract" if returns == DataType::F64 => {
                binary(args, |a, b| F::SubtractDouble(a, b))
            }
            "ZetaSQL:$subtract" if returns == DataType::I64 => {
                binary(args, |a, b| F::SubtractInt64(a, b))
            }
            "ZetaSQL:$unary_minus" if returns == DataType::I64 => {
                unary(args, |a| F::UnaryMinusInt64(a))
            }
            "ZetaSQL:$unary_minus" if returns == DataType::F64 => {
                unary(args, |a| F::UnaryMinusDouble(a))
            }
            "ZetaSQL:concat" => F::ConcatString(args),
            "ZetaSQL:strpos" => binary(args, |a, b| F::StrposString(a, b)),
            "ZetaSQL:lower" => unary(args, |a| F::LowerString(a)),
            "ZetaSQL:upper" => unary(args, |a| F::UpperString(a)),
            "ZetaSQL:length" => unary(args, |a| F::LengthString(a)),
            "ZetaSQL:starts_with" => binary(args, |a, b| F::StartsWithString(a, b)),
            "ZetaSQL:ends_with" => binary(args, |a, b| F::EndsWithString(a, b)),
            "ZetaSQL:substr" if args.len() == 2 => binary(args, |a, b| F::SubstrString(a, b, None)),
            "ZetaSQL:substr" if args.len() == 3 => {
                ternary(args, |a, b, c| F::SubstrString(a, b, Some(c)))
            }
            "ZetaSQL:trim" if args.len() == 1 => unary(args, |a| F::TrimString(a, None)),
            "ZetaSQL:ltrim" if args.len() == 1 => unary(args, |a| F::LtrimString(a, None)),
            "ZetaSQL:rtrim" if args.len() == 1 => unary(args, |a| F::RtrimString(a, None)),
            "ZetaSQL:trim" if args.len() == 2 => binary(args, |a, b| F::TrimString(a, Some(b))),
            "ZetaSQL:ltrim" if args.len() == 2 => binary(args, |a, b| F::LtrimString(a, Some(b))),
            "ZetaSQL:rtrim" if args.len() == 2 => binary(args, |a, b| F::RtrimString(a, Some(b))),
            "ZetaSQL:replace" => ternary(args, |a, b, c| F::ReplaceString(a, b, c)),
            "ZetaSQL:regexp_extract" => binary(args, |a, b| F::RegexpExtractString(a, b)),
            "ZetaSQL:regexp_replace" => ternary(args, |a, b, c| F::RegexpReplaceString(a, b, c)),
            "ZetaSQL:byte_length" => unary(args, |a| F::ByteLengthString(a)),
            "ZetaSQL:char_length" => unary(args, |a| F::CharLengthString(a)),
            "ZetaSQL:regexp_contains" => binary(args, |a, b| F::RegexpContainsString(a, b)),
            "ZetaSQL:lpad" if args.len() == 2 => binary(args, |a, b| {
                F::LpadString(a, b, Scalar::Literal(Value::String(Some(" ".to_string()))))
            }),
            "ZetaSQL:lpad" if args.len() == 3 => ternary(args, |a, b, c| F::LpadString(a, b, c)),
            "ZetaSQL:rpad" if args.len() == 2 => binary(args, |a, b| {
                F::RpadString(a, b, Scalar::Literal(Value::String(Some(" ".to_string()))))
            }),
            "ZetaSQL:rpad" if args.len() == 3 => ternary(args, |a, b, c| F::RpadString(a, b, c)),
            "ZetaSQL:left" => binary(args, |a, b| F::LeftString(a, b)),
            "ZetaSQL:right" => binary(args, |a, b| F::RightString(a, b)),
            "ZetaSQL:repeat" => binary(args, |a, b| F::RepeatString(a, b)),
            "ZetaSQL:reverse" => unary(args, |a| F::ReverseString(a)),
            "ZetaSQL:chr" => unary(args, |a| F::ChrString(a)),
            "ZetaSQL:if" => ternary(args, |a, b, c| F::If(a, b, c)),
            "ZetaSQL:coalesce" => F::Coalesce(args),
            "ZetaSQL:ifnull" => binary(args, |a, b| F::Ifnull(a, b)),
            "ZetaSQL:nullif" => binary(args, |a, b| F::Nullif(a, b)),
            "ZetaSQL:current_date" => thunk(args, || F::CurrentDate),
            "ZetaSQL:current_timestamp" => thunk(args, || F::CurrentTimestamp),
            "ZetaSQL:date_add" => {
                binary_date_part(args, |a, b, date_part| F::DateAddDate(a, b, date_part))
            }
            "ZetaSQL:timestamp_add" => {
                binary_date_part(args, |a, b, date_part| F::TimestampAdd(a, b, date_part))
            }
            "ZetaSQL:date_diff" => {
                binary_date_part(args, |a, b, date_part| F::DateDiffDate(a, b, date_part))
            }
            "ZetaSQL:timestamp_diff" => {
                binary_date_part(args, |a, b, date_part| F::TimestampDiff(a, b, date_part))
            }
            "ZetaSQL:date_sub" => {
                binary_date_part(args, |a, b, date_part| F::DateSubDate(a, b, date_part))
            }
            "ZetaSQL:timestamp_sub" => {
                binary_date_part(args, |a, b, date_part| F::TimestampSub(a, b, date_part))
            }
            "ZetaSQL:date_trunc" => {
                unary_date_part(args, |a, date_part| F::DateTruncDate(a, date_part))
            }
            "ZetaSQL:timestamp_trunc" if args.len() == 2 => {
                unary_date_part(args, |a, date_part| F::TimestampTrunc(a, date_part))
            }
            "ZetaSQL:timestamp_trunc" if args.len() == 3 => {
                panic!("TIMESTAMP_TRUNC with time zone is not supported")
            }
            "ZetaSQL:date_from_unix_date" => unary(args, |a| F::DateFromUnixDate(a)),
            "ZetaSQL:timestamp_from_unix_micros" => {
                unary(args, |a| F::TimestampFromUnixMicrosInt64(a))
            }
            "ZetaSQL:unix_date" => unary(args, |a| F::UnixDate(a)),
            "ZetaSQL:unix_micros" => unary(args, |a| F::UnixMicrosFromTimestamp(a)),
            "ZetaSQL:date" if first_argument == Some(DataType::Timestamp) => {
                unary(args, |a| F::DateFromTimestamp(a))
            }
            "ZetaSQL:date" if signature.argument.len() == 3 => {
                ternary(args, |a, b, c| F::DateFromYearMonthDay(a, b, c))
            }
            "ZetaSQL:timestamp" if first_argument == Some(DataType::String) => {
                unary(args, |a| F::TimestampFromString(a))
            }
            "ZetaSQL:timestamp" if first_argument == Some(DataType::Date) => {
                unary(args, |a| F::TimestampFromDate(a))
            }
            "ZetaSQL:string" if first_argument == Some(DataType::Date) => {
                unary(args, |a| F::StringFromDate(a))
            }
            "ZetaSQL:string" if first_argument == Some(DataType::Timestamp) => {
                unary(args, |a| F::StringFromTimestamp(a))
            }
            "ZetaSQL:$extract" if first_argument == Some(DataType::Date) => {
                unary_date_part(args, |a, date_part| F::ExtractFromDate(a, date_part))
            }
            "ZetaSQL:$extract" if first_argument == Some(DataType::Timestamp) => {
                unary_date_part(args, |a, date_part| F::ExtractFromTimestamp(a, date_part))
            }
            "ZetaSQL:$extract_date" if args.len() == 1 => {
                unary(args, |a| F::ExtractDateFromTimestamp(a))
            }
            "ZetaSQL:$extract_date" if args.len() == 2 => {
                panic!("EXTRACT from date with time zone is not supported")
            }
            "ZetaSQL:format_date" => binary(args, |a, b| F::FormatDate(a, b)),
            "ZetaSQL:format_timestamp" if args.len() == 2 => {
                binary(args, |a, b| F::FormatTimestamp(a, b))
            }
            "ZetaSQL:format_timestamp" if args.len() == 3 => {
                panic!("FORMAT_TIMESTAMP with time zone is not supported")
            }
            "ZetaSQL:parse_date" => binary(args, |a, b| F::ParseDate(a, b)),
            "ZetaSQL:parse_timestamp" => binary(args, |a, b| F::ParseTimestamp(a, b)),
            "ZetaSQL:abs" if returns == DataType::I64 => unary(args, |a| F::AbsInt64(a)),
            "ZetaSQL:abs" if returns == DataType::F64 => unary(args, |a| F::AbsDouble(a)),
            "ZetaSQL:sign" if returns == DataType::I64 => unary(args, |a| F::SignInt64(a)),
            "ZetaSQL:sign" if returns == DataType::F64 => unary(args, |a| F::SignDouble(a)),
            "ZetaSQL:round" if signature.argument.len() == 1 => unary(args, |a| F::RoundDouble(a)),
            "ZetaSQL:round" if signature.argument.len() == 2 => {
                binary(args, |a, b| F::RoundWithDigitsDouble(a, b))
            }
            "ZetaSQL:trunc" if signature.argument.len() == 1 => unary(args, |a| F::TruncDouble(a)),
            "ZetaSQL:trunc" if signature.argument.len() == 2 => {
                binary(args, |a, b| F::TruncWithDigitsDouble(a, b))
            }
            "ZetaSQL:ceil" => unary(args, |a| F::CeilDouble(a)),
            "ZetaSQL:floor" => unary(args, |a| F::FloorDouble(a)),
            "ZetaSQL:mod" => binary(args, |a, b| F::ModInt64(a, b)),
            "ZetaSQL:div" => binary(args, |a, b| F::DivInt64(a, b)),
            "ZetaSQL:is_inf" => unary(args, |a| F::IsInf(a)),
            "ZetaSQL:is_nan" => unary(args, |a| F::IsNan(a)),
            "ZetaSQL:greatest" => F::Greatest(args),
            "ZetaSQL:least" => F::Least(args),
            "ZetaSQL:sqrt" => unary(args, |a| F::SqrtDouble(a)),
            "ZetaSQL:pow" => binary(args, |a, b| F::PowDouble(a, b)),
            "ZetaSQL:exp" => unary(args, |a| F::ExpDouble(a)),
            "ZetaSQL:ln" => unary(args, |a| F::NaturalLogarithmDouble(a)),
            "ZetaSQL:log10" => unary(args, |a| F::DecimalLogarithmDouble(a)),
            "ZetaSQL:log" if args.len() == 1 => unary(args, |a| F::NaturalLogarithmDouble(a)),
            "ZetaSQL:log" if args.len() == 2 => binary(args, |a, b| F::LogarithmDouble(a, b)),
            "ZetaSQL:cos" => unary(args, |a| F::CosDouble(a)),
            "ZetaSQL:cosh" => unary(args, |a| F::CoshDouble(a)),
            "ZetaSQL:acos" => unary(args, |a| F::AcosDouble(a)),
            "ZetaSQL:acosh" => unary(args, |a| F::AcoshDouble(a)),
            "ZetaSQL:sin" => unary(args, |a| F::SinDouble(a)),
            "ZetaSQL:sinh" => unary(args, |a| F::SinhDouble(a)),
            "ZetaSQL:asin" => unary(args, |a| F::AsinDouble(a)),
            "ZetaSQL:asinh" => unary(args, |a| F::AsinhDouble(a)),
            "ZetaSQL:tan" => unary(args, |a| F::TanDouble(a)),
            "ZetaSQL:tanh" => unary(args, |a| F::TanhDouble(a)),
            "ZetaSQL:atan" => unary(args, |a| F::AtanDouble(a)),
            "ZetaSQL:atanh" => unary(args, |a| F::AtanhDouble(a)),
            "ZetaSQL:atan2" => binary(args, |a, b| F::Atan2Double(a, b)),
            "system:next_val" => unary(args, |a| F::NextVal(a)),
            "system:get_var" => unary(args, |a| F::GetVar(a)),
            other => panic!("{} is not a known function name", other),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            F::CurrentDate => "CurrentDate",
            F::CurrentTimestamp => "CurrentTimestamp",
            F::Xid => "Xid",
            F::Coalesce(_) => "Coalesce",
            F::ConcatString(_) => "ConcatString",
            F::Hash(_) => "Hash",
            F::Greatest(_) => "Greatest",
            F::Least(_) => "Least",
            F::AbsDouble(_) => "AbsDouble",
            F::AbsInt64(_) => "AbsInt64",
            F::AcosDouble(_) => "AcosDouble",
            F::AcoshDouble(_) => "AcoshDouble",
            F::AsinDouble(_) => "AsinDouble",
            F::AsinhDouble(_) => "AsinhDouble",
            F::AtanDouble(_) => "AtanDouble",
            F::AtanhDouble(_) => "AtanhDouble",
            F::ByteLengthString(_) => "ByteLengthString",
            F::CeilDouble(_) => "CeilDouble",
            F::CharLengthString(_) => "CharLengthString",
            F::ChrString(_) => "ChrString",
            F::CosDouble(_) => "CosDouble",
            F::CoshDouble(_) => "CoshDouble",
            F::DateFromTimestamp(_) => "DateFromTimestamp",
            F::DateFromUnixDate(_) => "DateFromUnixDate",
            F::DecimalLogarithmDouble(_) => "DecimalLogarithmDouble",
            F::ExpDouble(_) => "ExpDouble",
            F::ExtractDateFromTimestamp(_) => "ExtractDateFromTimestamp",
            F::FloorDouble(_) => "FloorDouble",
            F::GetVar(_) => "GetVar",
            F::IsFalse(_) => "IsFalse",
            F::IsInf(_) => "IsInf",
            F::IsNan(_) => "IsNan",
            F::IsNull(_) => "IsNull",
            F::IsTrue(_) => "IsTrue",
            F::LengthString(_) => "LengthString",
            F::LowerString(_) => "LowerString",
            F::NaturalLogarithmDouble(_) => "NaturalLogarithmDouble",
            F::NextVal(_) => "NextVal",
            F::Not(_) => "Not",
            F::ReverseString(_) => "ReverseString",
            F::RoundDouble(_) => "RoundDouble",
            F::SignDouble(_) => "SignDouble",
            F::SignInt64(_) => "SignInt64",
            F::SinDouble(_) => "SinDouble",
            F::SinhDouble(_) => "SinhDouble",
            F::SqrtDouble(_) => "SqrtDouble",
            F::StringFromDate(_) => "StringFromDate",
            F::StringFromTimestamp(_) => "StringFromTimestamp",
            F::TanDouble(_) => "TanDouble",
            F::TanhDouble(_) => "TanhDouble",
            F::TimestampFromDate(_) => "TimestampFromDate",
            F::TimestampFromString(_) => "TimestampFromString",
            F::TimestampFromUnixMicrosInt64(_) => "TimestampFromUnixMicrosInt64",
            F::TruncDouble(_) => "TruncDouble",
            F::UnaryMinusDouble(_) => "UnaryMinusDouble",
            F::UnaryMinusInt64(_) => "UnaryMinusInt64",
            F::UnixDate(_) => "UnixDate",
            F::UnixMicrosFromTimestamp(_) => "UnixMicrosFromTimestamp",
            F::UpperString(_) => "UpperString",
            F::DateTruncDate(_, _) => "DateTruncDate",
            F::ExtractFromDate(_, _) => "ExtractFromDate",
            F::ExtractFromTimestamp(_, _) => "ExtractFromTimestamp",
            F::TimestampTrunc(_, _) => "TimestampTrunc",
            F::In(_, _) => "In",
            F::AddDouble(_, _) => "AddDouble",
            F::AddInt64(_, _) => "AddInt64",
            F::And(_, _) => "And",
            F::Atan2Double(_, _) => "Atan2Double",
            F::DivideDouble(_, _) => "DivideDouble",
            F::DivInt64(_, _) => "DivInt64",
            F::EndsWithString(_, _) => "EndsWithString",
            F::Equal(_, _) => "Equal",
            F::FormatDate(_, _) => "FormatDate",
            F::FormatTimestamp(_, _) => "FormatTimestamp",
            F::Greater(_, _) => "Greater",
            F::GreaterOrEqual(_, _) => "GreaterOrEqual",
            F::Ifnull(_, _) => "Ifnull",
            F::Is(_, _) => "Is",
            F::LeftString(_, _) => "LeftString",
            F::Less(_, _) => "Less",
            F::LessOrEqual(_, _) => "LessOrEqual",
            F::LogarithmDouble(_, _) => "LogarithmDouble",
            F::LtrimString(_, _) => "LtrimString",
            F::ModInt64(_, _) => "ModInt64",
            F::MultiplyDouble(_, _) => "MultiplyDouble",
            F::MultiplyInt64(_, _) => "MultiplyInt64",
            F::NotEqual(_, _) => "NotEqual",
            F::Nullif(_, _) => "Nullif",
            F::Or(_, _) => "Or",
            F::ParseDate(_, _) => "ParseDate",
            F::ParseTimestamp(_, _) => "ParseTimestamp",
            F::PowDouble(_, _) => "PowDouble",
            F::RegexpContainsString(_, _) => "RegexpContainsString",
            F::RegexpExtractString(_, _) => "RegexpExtractString",
            F::RepeatString(_, _) => "RepeatString",
            F::RightString(_, _) => "RightString",
            F::RoundWithDigitsDouble(_, _) => "RoundWithDigitsDouble",
            F::RtrimString(_, _) => "RtrimString",
            F::StartsWithString(_, _) => "StartsWithString",
            F::StringLike(_, _) => "StringLike",
            F::StrposString(_, _) => "StrposString",
            F::SubtractDouble(_, _) => "SubtractDouble",
            F::SubtractInt64(_, _) => "SubtractInt64",
            F::TrimString(_, _) => "TrimString",
            F::TruncWithDigitsDouble(_, _) => "TruncWithDigitsDouble",
            F::DateAddDate(_, _, _) => "DateAddDate",
            F::DateDiffDate(_, _, _) => "DateDiffDate",
            F::DateSubDate(_, _, _) => "DateSubDate",
            F::TimestampAdd(_, _, _) => "TimestampAdd",
            F::TimestampDiff(_, _, _) => "TimestampDiff",
            F::TimestampSub(_, _, _) => "TimestampSub",
            F::Between(_, _, _) => "Between",
            F::CaseNoValue(_, _) => "CaseNoValue",
            F::DateFromYearMonthDay(_, _, _) => "DateFromYearMonthDay",
            F::If(_, _, _) => "If",
            F::LpadString(_, _, _) => "LpadString",
            F::RegexpReplaceString(_, _, _) => "RegexpReplaceString",
            F::ReplaceString(_, _, _) => "ReplaceString",
            F::RpadString(_, _, _) => "RpadString",
            F::SubstrString(_, _, _) => "SubstrString",
            F::CaseWithValue(_, _, _) => "CaseWithValue",
        }
    }

    pub fn arguments(&self) -> Vec<&Scalar> {
        match self {
            F::CurrentDate | F::CurrentTimestamp | F::Xid => vec![],
            F::Coalesce(varargs)
            | F::ConcatString(varargs)
            | F::Hash(varargs)
            | F::Greatest(varargs)
            | F::Least(varargs) => {
                let mut arguments = vec![];
                for a in varargs {
                    arguments.push(a);
                }
                arguments
            }
            F::AbsDouble(a)
            | F::AbsInt64(a)
            | F::AcosDouble(a)
            | F::AcoshDouble(a)
            | F::AsinDouble(a)
            | F::AsinhDouble(a)
            | F::AtanDouble(a)
            | F::AtanhDouble(a)
            | F::ByteLengthString(a)
            | F::CeilDouble(a)
            | F::CharLengthString(a)
            | F::ChrString(a)
            | F::CosDouble(a)
            | F::CoshDouble(a)
            | F::DateFromTimestamp(a)
            | F::DateFromUnixDate(a)
            | F::DecimalLogarithmDouble(a)
            | F::ExpDouble(a)
            | F::ExtractDateFromTimestamp(a)
            | F::FloorDouble(a)
            | F::GetVar(a)
            | F::IsFalse(a)
            | F::IsInf(a)
            | F::IsNan(a)
            | F::IsNull(a)
            | F::IsTrue(a)
            | F::LengthString(a)
            | F::LowerString(a)
            | F::NaturalLogarithmDouble(a)
            | F::NextVal(a)
            | F::Not(a)
            | F::ReverseString(a)
            | F::RoundDouble(a)
            | F::SignDouble(a)
            | F::SignInt64(a)
            | F::SinDouble(a)
            | F::SinhDouble(a)
            | F::SqrtDouble(a)
            | F::StringFromDate(a)
            | F::StringFromTimestamp(a)
            | F::TanDouble(a)
            | F::TanhDouble(a)
            | F::TimestampFromDate(a)
            | F::TimestampFromString(a)
            | F::TimestampFromUnixMicrosInt64(a)
            | F::TruncDouble(a)
            | F::UnaryMinusDouble(a)
            | F::UnaryMinusInt64(a)
            | F::UnixDate(a)
            | F::UnixMicrosFromTimestamp(a)
            | F::UpperString(a)
            | F::DateTruncDate(a, _)
            | F::ExtractFromDate(a, _)
            | F::ExtractFromTimestamp(a, _)
            | F::TimestampTrunc(a, _)
            | F::LtrimString(a, None)
            | F::RtrimString(a, None)
            | F::TrimString(a, None) => vec![a],
            F::In(a, varargs) => {
                let mut arguments = vec![a];
                for a in varargs {
                    arguments.push(a);
                }
                arguments
            }
            F::AddDouble(a, b)
            | F::AddInt64(a, b)
            | F::And(a, b)
            | F::Atan2Double(a, b)
            | F::DivideDouble(a, b)
            | F::DivInt64(a, b)
            | F::EndsWithString(a, b)
            | F::Equal(a, b)
            | F::FormatDate(a, b)
            | F::FormatTimestamp(a, b)
            | F::Greater(a, b)
            | F::GreaterOrEqual(a, b)
            | F::Ifnull(a, b)
            | F::Is(a, b)
            | F::LeftString(a, b)
            | F::Less(a, b)
            | F::LessOrEqual(a, b)
            | F::LogarithmDouble(a, b)
            | F::LtrimString(a, Some(b))
            | F::ModInt64(a, b)
            | F::MultiplyDouble(a, b)
            | F::MultiplyInt64(a, b)
            | F::NotEqual(a, b)
            | F::Nullif(a, b)
            | F::Or(a, b)
            | F::ParseDate(a, b)
            | F::ParseTimestamp(a, b)
            | F::PowDouble(a, b)
            | F::RegexpContainsString(a, b)
            | F::RegexpExtractString(a, b)
            | F::RepeatString(a, b)
            | F::RightString(a, b)
            | F::RoundWithDigitsDouble(a, b)
            | F::RtrimString(a, Some(b))
            | F::StartsWithString(a, b)
            | F::StringLike(a, b)
            | F::StrposString(a, b)
            | F::SubtractDouble(a, b)
            | F::SubtractInt64(a, b)
            | F::TrimString(a, Some(b))
            | F::TruncWithDigitsDouble(a, b)
            | F::DateAddDate(a, b, _)
            | F::DateDiffDate(a, b, _)
            | F::DateSubDate(a, b, _)
            | F::TimestampAdd(a, b, _)
            | F::TimestampDiff(a, b, _)
            | F::TimestampSub(a, b, _)
            | F::SubstrString(a, b, None) => vec![a, b],
            F::Between(a, b, c)
            | F::DateFromYearMonthDay(a, b, c)
            | F::If(a, b, c)
            | F::LpadString(a, b, c)
            | F::RegexpReplaceString(a, b, c)
            | F::ReplaceString(a, b, c)
            | F::RpadString(a, b, c)
            | F::SubstrString(a, b, Some(c)) => vec![a, b, c],
            F::CaseNoValue(cases, default) => {
                let mut arguments = vec![];
                for (a, b) in cases {
                    arguments.push(a);
                    arguments.push(b);
                }
                arguments.push(default);
                arguments
            }
            F::CaseWithValue(value, cases, default) => {
                let mut arguments = vec![value];
                for (a, b) in cases {
                    arguments.push(a);
                    arguments.push(b);
                }
                arguments.push(default);
                arguments
            }
        }
    }

    pub fn returns(&self) -> DataType {
        match self {
            F::CaseNoValue(_, scalar)
            | F::CaseWithValue(_, _, scalar)
            | F::If(_, scalar, _)
            | F::Ifnull(scalar, _)
            | F::Nullif(scalar, _) => scalar.data_type(),
            F::Coalesce(varargs) | F::Greatest(varargs) | F::Least(varargs) => {
                varargs[0].data_type()
            }
            F::And { .. }
            | F::Between { .. }
            | F::EndsWithString { .. }
            | F::Equal { .. }
            | F::Greater { .. }
            | F::GreaterOrEqual { .. }
            | F::In { .. }
            | F::Is { .. }
            | F::IsFalse { .. }
            | F::IsInf { .. }
            | F::IsNan { .. }
            | F::IsNull { .. }
            | F::IsTrue { .. }
            | F::Less { .. }
            | F::LessOrEqual { .. }
            | F::Not { .. }
            | F::NotEqual { .. }
            | F::Or { .. }
            | F::RegexpContainsString { .. }
            | F::StartsWithString { .. }
            | F::StringLike { .. } => DataType::Bool,
            F::CurrentDate { .. }
            | F::DateAddDate { .. }
            | F::DateFromTimestamp { .. }
            | F::DateFromUnixDate { .. }
            | F::DateFromYearMonthDay { .. }
            | F::DateSubDate { .. }
            | F::DateTruncDate { .. }
            | F::ExtractDateFromTimestamp { .. }
            | F::ParseDate { .. } => DataType::Date,
            F::AbsDouble { .. }
            | F::AcosDouble { .. }
            | F::AcoshDouble { .. }
            | F::AddDouble { .. }
            | F::AsinDouble { .. }
            | F::AsinhDouble { .. }
            | F::Atan2Double { .. }
            | F::AtanDouble { .. }
            | F::AtanhDouble { .. }
            | F::CeilDouble { .. }
            | F::CosDouble { .. }
            | F::CoshDouble { .. }
            | F::DecimalLogarithmDouble { .. }
            | F::DivideDouble { .. }
            | F::ExpDouble { .. }
            | F::FloorDouble { .. }
            | F::LogarithmDouble { .. }
            | F::MultiplyDouble { .. }
            | F::NaturalLogarithmDouble { .. }
            | F::PowDouble { .. }
            | F::RoundDouble { .. }
            | F::RoundWithDigitsDouble { .. }
            | F::SignDouble { .. }
            | F::SinDouble { .. }
            | F::SinhDouble { .. }
            | F::SqrtDouble { .. }
            | F::SubtractDouble { .. }
            | F::TanDouble { .. }
            | F::TanhDouble { .. }
            | F::TruncDouble { .. }
            | F::TruncWithDigitsDouble { .. }
            | F::UnaryMinusDouble { .. } => DataType::F64,
            F::AbsInt64 { .. }
            | F::AddInt64 { .. }
            | F::ByteLengthString { .. }
            | F::CharLengthString { .. }
            | F::DateDiffDate { .. }
            | F::DivInt64 { .. }
            | F::ExtractFromDate { .. }
            | F::ExtractFromTimestamp { .. }
            | F::GetVar { .. }
            | F::Hash { .. }
            | F::LengthString { .. }
            | F::ModInt64 { .. }
            | F::MultiplyInt64 { .. }
            | F::NextVal { .. }
            | F::SignInt64 { .. }
            | F::StrposString { .. }
            | F::SubtractInt64 { .. }
            | F::TimestampDiff { .. }
            | F::UnaryMinusInt64 { .. }
            | F::UnixDate { .. }
            | F::UnixMicrosFromTimestamp { .. }
            | F::Xid { .. } => DataType::I64,
            F::ChrString { .. }
            | F::ConcatString { .. }
            | F::FormatDate { .. }
            | F::FormatTimestamp { .. }
            | F::LeftString { .. }
            | F::LowerString { .. }
            | F::LpadString { .. }
            | F::LtrimString { .. }
            | F::RegexpExtractString { .. }
            | F::RegexpReplaceString { .. }
            | F::RepeatString { .. }
            | F::ReplaceString { .. }
            | F::ReverseString { .. }
            | F::RightString { .. }
            | F::RpadString { .. }
            | F::RtrimString { .. }
            | F::StringFromDate { .. }
            | F::StringFromTimestamp { .. }
            | F::SubstrString { .. }
            | F::TrimString { .. }
            | F::UpperString { .. } => DataType::String,
            F::CurrentTimestamp { .. }
            | F::ParseTimestamp { .. }
            | F::TimestampAdd { .. }
            | F::TimestampFromDate { .. }
            | F::TimestampFromString { .. }
            | F::TimestampFromUnixMicrosInt64 { .. }
            | F::TimestampSub { .. }
            | F::TimestampTrunc { .. } => DataType::Timestamp,
        }
    }

    pub fn map(self, f: impl Fn(Scalar) -> Scalar) -> Self {
        match self {
            F::CurrentDate => F::CurrentDate,
            F::CurrentTimestamp => F::CurrentTimestamp,
            F::Xid => F::Xid,
            F::Coalesce(mut varargs) => F::Coalesce(varargs.drain(..).map(f).collect()),
            F::ConcatString(mut varargs) => F::ConcatString(varargs.drain(..).map(f).collect()),
            F::Hash(mut varargs) => F::Hash(varargs.drain(..).map(f).collect()),
            F::Greatest(mut varargs) => F::Greatest(varargs.drain(..).map(f).collect()),
            F::Least(mut varargs) => F::Least(varargs.drain(..).map(f).collect()),
            F::In(a, mut varargs) => F::In(f(a), varargs.drain(..).map(f).collect()),
            F::CaseNoValue(mut cases, default) => F::CaseNoValue(
                cases.drain(..).map(|(a, b)| (f(a), f(b))).collect(),
                f(default),
            ),
            F::CaseWithValue(value, mut cases, default) => F::CaseWithValue(
                f(value),
                cases.drain(..).map(|(a, b)| (f(a), f(b))).collect(),
                f(default),
            ),
            F::AbsDouble(a) => F::AbsDouble(f(a)),
            F::AbsInt64(a) => F::AbsInt64(f(a)),
            F::AcosDouble(a) => F::AcosDouble(f(a)),
            F::AcoshDouble(a) => F::AcoshDouble(f(a)),
            F::AsinDouble(a) => F::AsinDouble(f(a)),
            F::AsinhDouble(a) => F::AsinhDouble(f(a)),
            F::AtanDouble(a) => F::AtanDouble(f(a)),
            F::AtanhDouble(a) => F::AtanhDouble(f(a)),
            F::ByteLengthString(a) => F::ByteLengthString(f(a)),
            F::CeilDouble(a) => F::CeilDouble(f(a)),
            F::CharLengthString(a) => F::CharLengthString(f(a)),
            F::ChrString(a) => F::ChrString(f(a)),
            F::CosDouble(a) => F::CosDouble(f(a)),
            F::CoshDouble(a) => F::CoshDouble(f(a)),
            F::DateFromTimestamp(a) => F::DateFromTimestamp(f(a)),
            F::DateFromUnixDate(a) => F::DateFromUnixDate(f(a)),
            F::DecimalLogarithmDouble(a) => F::DecimalLogarithmDouble(f(a)),
            F::ExpDouble(a) => F::ExpDouble(f(a)),
            F::ExtractDateFromTimestamp(a) => F::ExtractDateFromTimestamp(f(a)),
            F::FloorDouble(a) => F::FloorDouble(f(a)),
            F::GetVar(a) => F::GetVar(f(a)),
            F::IsFalse(a) => F::IsFalse(f(a)),
            F::IsInf(a) => F::IsInf(f(a)),
            F::IsNan(a) => F::IsNan(f(a)),
            F::IsNull(a) => F::IsNull(f(a)),
            F::IsTrue(a) => F::IsTrue(f(a)),
            F::LengthString(a) => F::LengthString(f(a)),
            F::LowerString(a) => F::LowerString(f(a)),
            F::NaturalLogarithmDouble(a) => F::NaturalLogarithmDouble(f(a)),
            F::NextVal(a) => F::NextVal(f(a)),
            F::Not(a) => F::Not(f(a)),
            F::ReverseString(a) => F::ReverseString(f(a)),
            F::RoundDouble(a) => F::RoundDouble(f(a)),
            F::SignDouble(a) => F::SignDouble(f(a)),
            F::SignInt64(a) => F::SignInt64(f(a)),
            F::SinDouble(a) => F::SinDouble(f(a)),
            F::SinhDouble(a) => F::SinhDouble(f(a)),
            F::SqrtDouble(a) => F::SqrtDouble(f(a)),
            F::StringFromDate(a) => F::StringFromDate(f(a)),
            F::StringFromTimestamp(a) => F::StringFromTimestamp(f(a)),
            F::TanDouble(a) => F::TanDouble(f(a)),
            F::TanhDouble(a) => F::TanhDouble(f(a)),
            F::TimestampFromDate(a) => F::TimestampFromDate(f(a)),
            F::TimestampFromString(a) => F::TimestampFromString(f(a)),
            F::TimestampFromUnixMicrosInt64(a) => F::TimestampFromUnixMicrosInt64(f(a)),
            F::TruncDouble(a) => F::TruncDouble(f(a)),
            F::UnaryMinusDouble(a) => F::UnaryMinusDouble(f(a)),
            F::UnaryMinusInt64(a) => F::UnaryMinusInt64(f(a)),
            F::UnixDate(a) => F::UnixDate(f(a)),
            F::UnixMicrosFromTimestamp(a) => F::UnixMicrosFromTimestamp(f(a)),
            F::UpperString(a) => F::UpperString(f(a)),
            F::DateTruncDate(a, date_part) => F::DateTruncDate(f(a), date_part),
            F::ExtractFromDate(a, date_part) => F::ExtractFromDate(f(a), date_part),
            F::ExtractFromTimestamp(a, date_part) => F::ExtractFromTimestamp(f(a), date_part),
            F::TimestampTrunc(a, date_part) => F::TimestampTrunc(f(a), date_part),
            F::AddDouble(a, b) => F::AddDouble(f(a), f(b)),
            F::AddInt64(a, b) => F::AddInt64(f(a), f(b)),
            F::And(a, b) => F::And(f(a), f(b)),
            F::Atan2Double(a, b) => F::Atan2Double(f(a), f(b)),
            F::DivideDouble(a, b) => F::DivideDouble(f(a), f(b)),
            F::DivInt64(a, b) => F::DivInt64(f(a), f(b)),
            F::EndsWithString(a, b) => F::EndsWithString(f(a), f(b)),
            F::Equal(a, b) => F::Equal(f(a), f(b)),
            F::FormatDate(a, b) => F::FormatDate(f(a), f(b)),
            F::FormatTimestamp(a, b) => F::FormatTimestamp(f(a), f(b)),
            F::Greater(a, b) => F::Greater(f(a), f(b)),
            F::GreaterOrEqual(a, b) => F::GreaterOrEqual(f(a), f(b)),
            F::Ifnull(a, b) => F::Ifnull(f(a), f(b)),
            F::Is(a, b) => F::Is(f(a), f(b)),
            F::LeftString(a, b) => F::LeftString(f(a), f(b)),
            F::Less(a, b) => F::Less(f(a), f(b)),
            F::LessOrEqual(a, b) => F::LessOrEqual(f(a), f(b)),
            F::LogarithmDouble(a, b) => F::LogarithmDouble(f(a), f(b)),
            F::LtrimString(a, b) => F::LtrimString(f(a), b.map(f)),
            F::ModInt64(a, b) => F::ModInt64(f(a), f(b)),
            F::MultiplyDouble(a, b) => F::MultiplyDouble(f(a), f(b)),
            F::MultiplyInt64(a, b) => F::MultiplyInt64(f(a), f(b)),
            F::NotEqual(a, b) => F::NotEqual(f(a), f(b)),
            F::Nullif(a, b) => F::Nullif(f(a), f(b)),
            F::Or(a, b) => F::Or(f(a), f(b)),
            F::ParseDate(a, b) => F::ParseDate(f(a), f(b)),
            F::ParseTimestamp(a, b) => F::ParseTimestamp(f(a), f(b)),
            F::PowDouble(a, b) => F::PowDouble(f(a), f(b)),
            F::RegexpContainsString(a, b) => F::RegexpContainsString(f(a), f(b)),
            F::RegexpExtractString(a, b) => F::RegexpExtractString(f(a), f(b)),
            F::RepeatString(a, b) => F::RepeatString(f(a), f(b)),
            F::RightString(a, b) => F::RightString(f(a), f(b)),
            F::RoundWithDigitsDouble(a, b) => F::RoundWithDigitsDouble(f(a), f(b)),
            F::RtrimString(a, b) => F::RtrimString(f(a), b.map(f)),
            F::StartsWithString(a, b) => F::StartsWithString(f(a), f(b)),
            F::StringLike(a, b) => F::StringLike(f(a), f(b)),
            F::StrposString(a, b) => F::StrposString(f(a), f(b)),
            F::SubtractDouble(a, b) => F::SubtractDouble(f(a), f(b)),
            F::SubtractInt64(a, b) => F::SubtractInt64(f(a), f(b)),
            F::TrimString(a, b) => F::TrimString(f(a), b.map(f)),
            F::TruncWithDigitsDouble(a, b) => F::TruncWithDigitsDouble(f(a), f(b)),
            F::DateAddDate(a, b, date_part) => F::DateAddDate(f(a), f(b), date_part),
            F::DateDiffDate(a, b, date_part) => F::DateDiffDate(f(a), f(b), date_part),
            F::DateSubDate(a, b, date_part) => F::DateSubDate(f(a), f(b), date_part),
            F::TimestampAdd(a, b, date_part) => F::TimestampAdd(f(a), f(b), date_part),
            F::TimestampDiff(a, b, date_part) => F::TimestampDiff(f(a), f(b), date_part),
            F::TimestampSub(a, b, date_part) => F::TimestampSub(f(a), f(b), date_part),
            F::Between(a, b, c) => F::Between(f(a), f(b), f(c)),
            F::DateFromYearMonthDay(a, b, c) => F::DateFromYearMonthDay(f(a), f(b), f(c)),
            F::If(a, b, c) => F::If(f(a), f(b), f(c)),
            F::LpadString(a, b, c) => F::LpadString(f(a), f(b), f(c)),
            F::RegexpReplaceString(a, b, c) => F::RegexpReplaceString(f(a), f(b), f(c)),
            F::ReplaceString(a, b, c) => F::ReplaceString(f(a), f(b), f(c)),
            F::RpadString(a, b, c) => F::RpadString(f(a), f(b), f(c)),
            F::SubstrString(a, b, c) => F::SubstrString(f(a), f(b), c.map(f)),
        }
    }
}
