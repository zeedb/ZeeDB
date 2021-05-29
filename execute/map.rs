use chrono::*;
use kernel::*;

pub trait FromArray<'a, Container: Array>: Sized {
    fn get(array: &'a Container, i: usize) -> Option<Self>;
}

pub trait IntoArray<Container: Array>: Sized {
    fn into_element(self) -> Result<Option<Container::Element>, Exception>;
}

pub trait ArrayExt: Array {
    fn map<'a, A: FromArray<'a, Self>, B: IntoArray<BArray>, BArray: Array>(
        &'a self,
        f: impl Fn(A) -> B,
    ) -> Result<AnyArray, Exception> {
        let mut output = BArray::with_capacity(self.len());
        for i in 0..self.len() {
            if let Some(next) = A::get(self, i) {
                output.push(f(next).into_element()?);
            } else {
                output.push(None);
            }
        }
        Ok(output.as_any())
    }

    fn bi_map<
        'a,
        'b,
        A: FromArray<'a, Self>,
        B: FromArray<'b, BArray>,
        C: IntoArray<CArray>,
        BArray: Array,
        CArray: Array,
    >(
        &'a self,
        right: &'b BArray,
        f: impl Fn(A, B) -> C,
    ) -> Result<AnyArray, Exception> {
        assert_eq!(self.len(), right.len());
        let mut output = CArray::with_capacity(self.len());
        for i in 0..self.len() {
            match (A::get(&self, i), B::get(right, i)) {
                (Some(a), Some(b)) => output.push(f(a, b).into_element()?),
                _ => output.push(None),
            }
        }
        Ok(output.as_any())
    }

    fn tri_map<
        'a,
        'b,
        'c,
        A: FromArray<'a, Self>,
        B: FromArray<'b, BArray>,
        C: FromArray<'c, CArray>,
        D: IntoArray<DArray>,
        BArray: Array,
        CArray: Array,
        DArray: Array,
    >(
        &'a self,
        middle: &'b BArray,
        right: &'c CArray,
        f: impl Fn(A, B, C) -> D,
    ) -> Result<AnyArray, Exception> {
        assert_eq!(self.len(), middle.len());
        assert_eq!(self.len(), right.len());
        let mut output = DArray::with_capacity(self.len());
        for i in 0..self.len() {
            match (A::get(&self, i), B::get(middle, i), C::get(right, i)) {
                (Some(a), Some(b), Some(c)) => output.push(f(a, b, c).into_element()?),
                _ => output.push(None),
            }
        }
        Ok(output.as_any())
    }
}

impl<'a> FromArray<'a, BoolArray> for bool {
    fn get(array: &'a BoolArray, i: usize) -> Option<Self> {
        array.get(i)
    }
}

impl<'a> FromArray<'a, BoolArray> for Option<bool> {
    fn get(array: &'a BoolArray, i: usize) -> Option<Self> {
        Some(array.get(i))
    }
}

impl IntoArray<BoolArray> for bool {
    fn into_element(self) -> Result<Option<bool>, Exception> {
        Ok(Some(self))
    }
}

impl IntoArray<BoolArray> for Option<bool> {
    fn into_element(self) -> Result<Option<bool>, Exception> {
        Ok(self)
    }
}

impl IntoArray<BoolArray> for Result<Option<bool>, Exception> {
    fn into_element(self) -> Result<Option<bool>, Exception> {
        self
    }
}

impl<'a> FromArray<'a, I64Array> for i64 {
    fn get(array: &'a I64Array, i: usize) -> Option<Self> {
        array.get(i)
    }
}

impl<'a> FromArray<'a, I64Array> for Option<i64> {
    fn get(array: &'a I64Array, i: usize) -> Option<Self> {
        Some(array.get(i))
    }
}

impl IntoArray<I64Array> for i64 {
    fn into_element(self) -> Result<Option<i64>, Exception> {
        Ok(Some(self))
    }
}

impl IntoArray<I64Array> for Option<i64> {
    fn into_element(self) -> Result<Option<i64>, Exception> {
        Ok(self)
    }
}

impl IntoArray<I64Array> for Result<Option<i64>, Exception> {
    fn into_element(self) -> Result<Option<i64>, Exception> {
        self
    }
}

impl<'a> FromArray<'a, F64Array> for f64 {
    fn get(array: &'a F64Array, i: usize) -> Option<Self> {
        array.get(i)
    }
}

impl<'a> FromArray<'a, F64Array> for Option<f64> {
    fn get(array: &'a F64Array, i: usize) -> Option<Self> {
        Some(array.get(i))
    }
}

impl IntoArray<F64Array> for f64 {
    fn into_element(self) -> Result<Option<f64>, Exception> {
        Ok(Some(self))
    }
}

impl IntoArray<F64Array> for Option<f64> {
    fn into_element(self) -> Result<Option<f64>, Exception> {
        Ok(self)
    }
}

impl IntoArray<F64Array> for Result<Option<f64>, Exception> {
    fn into_element(self) -> Result<Option<f64>, Exception> {
        self
    }
}

impl<'a> FromArray<'a, DateArray> for Date<Utc> {
    fn get(array: &'a DateArray, i: usize) -> Option<Self> {
        array.get(i).map(date)
    }
}

impl<'a> FromArray<'a, DateArray> for Option<Date<Utc>> {
    fn get(array: &'a DateArray, i: usize) -> Option<Self> {
        Some(array.get(i).map(date))
    }
}

impl IntoArray<DateArray> for Date<Utc> {
    fn into_element(self) -> Result<Option<i32>, Exception> {
        Ok(Some(epoch_date(self)))
    }
}

impl IntoArray<DateArray> for Option<Date<Utc>> {
    fn into_element(self) -> Result<Option<i32>, Exception> {
        Ok(self.map(epoch_date))
    }
}

impl IntoArray<DateArray> for Result<Option<Date<Utc>>, Exception> {
    fn into_element(self) -> Result<Option<i32>, Exception> {
        self.map(|option| option.map(epoch_date))
    }
}

impl<'a> FromArray<'a, TimestampArray> for DateTime<Utc> {
    fn get(array: &'a TimestampArray, i: usize) -> Option<Self> {
        array.get(i).map(timestamp)
    }
}

impl<'a> FromArray<'a, TimestampArray> for Option<DateTime<Utc>> {
    fn get(array: &'a TimestampArray, i: usize) -> Option<Self> {
        Some(array.get(i).map(timestamp))
    }
}

impl IntoArray<TimestampArray> for DateTime<Utc> {
    fn into_element(self) -> Result<Option<i64>, Exception> {
        Ok(Some(epoch_micros(self)))
    }
}

impl IntoArray<TimestampArray> for Option<DateTime<Utc>> {
    fn into_element(self) -> Result<Option<i64>, Exception> {
        Ok(self.map(epoch_micros))
    }
}

impl IntoArray<TimestampArray> for Result<Option<DateTime<Utc>>, Exception> {
    fn into_element(self) -> Result<Option<i64>, Exception> {
        self.map(|option| option.map(epoch_micros))
    }
}

impl<'a> FromArray<'a, StringArray> for &'a str {
    fn get(array: &'a StringArray, i: usize) -> Option<Self> {
        array.get_str(i)
    }
}

impl<'a> FromArray<'a, StringArray> for Option<&'a str> {
    fn get(array: &'a StringArray, i: usize) -> Option<Self> {
        Some(array.get_str(i))
    }
}

impl IntoArray<StringArray> for String {
    fn into_element(self) -> Result<Option<String>, Exception> {
        Ok(Some(self))
    }
}

impl IntoArray<StringArray> for Option<String> {
    fn into_element(self) -> Result<Option<String>, Exception> {
        Ok(self)
    }
}

impl IntoArray<StringArray> for Result<Option<String>, Exception> {
    fn into_element(self) -> Result<Option<String>, Exception> {
        self
    }
}

impl ArrayExt for BoolArray {}
impl ArrayExt for I64Array {}
impl ArrayExt for F64Array {}
impl ArrayExt for DateArray {}
impl ArrayExt for TimestampArray {}
impl ArrayExt for StringArray {}

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
