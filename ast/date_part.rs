use chrono::Weekday;
use serde::{Deserialize, Serialize};

use crate::{Scalar, Value};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(from = "i32", into = "i32")]
pub enum DatePart {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    DayOfWeek,
    Day,
    DayOfYear,
    Week(Weekday),
    IsoWeek,
    Month,
    Quarter,
    Year,
    IsoYear,
}

impl DatePart {
    pub(crate) fn from_scalar(scalar: Scalar) -> Self {
        if let Scalar::Literal(Value::EnumValue(i)) = scalar {
            match i {
                1 => DatePart::Year,
                2 => DatePart::Month,
                3 => DatePart::Day,
                4 => DatePart::DayOfWeek,
                5 => DatePart::DayOfYear,
                6 => DatePart::Quarter,
                7 => DatePart::Hour,
                8 => DatePart::Minute,
                9 => DatePart::Second,
                10 => DatePart::Millisecond,
                11 => DatePart::Microsecond,
                12 => DatePart::Nanosecond,
                13 => panic!("DATE part is not supported"),
                14 => DatePart::Week(Weekday::Sun),
                15 => panic!("DATETIME part is not supported"),
                16 => panic!("TIME part is not supported"),
                17 => DatePart::IsoYear,
                18 => DatePart::IsoWeek,
                19 => DatePart::Week(Weekday::Mon),
                20 => DatePart::Week(Weekday::Tue),
                21 => DatePart::Week(Weekday::Wed),
                22 => DatePart::Week(Weekday::Thu),
                23 => DatePart::Week(Weekday::Fri),
                24 => DatePart::Week(Weekday::Sat),
                _ => panic!("{} is not a recognized date part", i),
            }
        } else {
            panic!("{:?} is not an enum value", scalar)
        }
    }
}

impl From<i32> for DatePart {
    fn from(i: i32) -> Self {
        match i {
            0 => DatePart::Nanosecond,
            1 => DatePart::Microsecond,
            2 => DatePart::Millisecond,
            3 => DatePart::Second,
            4 => DatePart::Minute,
            5 => DatePart::Hour,
            6 => DatePart::DayOfWeek,
            7 => DatePart::Day,
            8 => DatePart::DayOfYear,
            9 => DatePart::Week(Weekday::Mon),
            10 => DatePart::Week(Weekday::Tue),
            11 => DatePart::Week(Weekday::Wed),
            12 => DatePart::Week(Weekday::Thu),
            13 => DatePart::Week(Weekday::Fri),
            14 => DatePart::Week(Weekday::Sat),
            15 => DatePart::Week(Weekday::Sun),
            16 => DatePart::IsoWeek,
            17 => DatePart::Month,
            18 => DatePart::Quarter,
            19 => DatePart::Year,
            20 => DatePart::IsoYear,
            other => panic!("{}", other),
        }
    }
}
impl Into<i32> for DatePart {
    fn into(self) -> i32 {
        match self {
            DatePart::Nanosecond => 0,
            DatePart::Microsecond => 1,
            DatePart::Millisecond => 2,
            DatePart::Second => 3,
            DatePart::Minute => 4,
            DatePart::Hour => 5,
            DatePart::DayOfWeek => 6,
            DatePart::Day => 7,
            DatePart::DayOfYear => 8,
            DatePart::Week(Weekday::Mon) => 9,
            DatePart::Week(Weekday::Tue) => 10,
            DatePart::Week(Weekday::Wed) => 11,
            DatePart::Week(Weekday::Thu) => 12,
            DatePart::Week(Weekday::Fri) => 13,
            DatePart::Week(Weekday::Sat) => 14,
            DatePart::Week(Weekday::Sun) => 15,
            DatePart::IsoWeek => 16,
            DatePart::Month => 17,
            DatePart::Quarter => 18,
            DatePart::Year => 19,
            DatePart::IsoYear => 20,
        }
    }
}
