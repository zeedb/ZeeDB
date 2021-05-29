use chrono::*;
use kernel::*;

pub trait FromArray<'a>: Sized {
    type Container: Array;

    fn from_array(array: &'a Self::Container, i: usize) -> Option<Self>;
}

pub trait IntoArray: Sized {
    type Container: Array;

    fn into_element(self) -> Result<Option<<Self::Container as Array>::Element>, Exception>;
}

pub trait ArrayExt<'a, A: FromArray<'a>> {
    fn map<B: IntoArray>(&'a self, f: impl Fn(A) -> B) -> Result<AnyArray, Exception>;

    fn bi_map<'b, B: FromArray<'b>, C: IntoArray>(
        &'a self,
        right: &'b B::Container,
        f: impl Fn(A, B) -> C,
    ) -> Result<AnyArray, Exception>;

    fn tri_map<'b, 'c, B: FromArray<'b>, C: FromArray<'c>, D: IntoArray>(
        &'a self,
        middle: &'b B::Container,
        right: &'c C::Container,
        f: impl Fn(A, B, C) -> D,
    ) -> Result<AnyArray, Exception>;
}

impl<'a> FromArray<'a> for bool {
    type Container = BoolArray;

    fn from_array(array: &'a BoolArray, i: usize) -> Option<Self> {
        array.get(i)
    }
}

impl<'a> FromArray<'a> for Option<bool> {
    type Container = BoolArray;

    fn from_array(array: &'a BoolArray, i: usize) -> Option<Self> {
        Some(array.get(i))
    }
}

impl IntoArray for bool {
    type Container = BoolArray;

    fn into_element(self) -> Result<Option<bool>, Exception> {
        Ok(Some(self))
    }
}

impl IntoArray for Option<bool> {
    type Container = BoolArray;

    fn into_element(self) -> Result<Option<bool>, Exception> {
        Ok(self)
    }
}

impl IntoArray for Result<Option<bool>, Exception> {
    type Container = BoolArray;

    fn into_element(self) -> Result<Option<bool>, Exception> {
        self
    }
}

impl<'a> FromArray<'a> for i64 {
    type Container = I64Array;

    fn from_array(array: &'a I64Array, i: usize) -> Option<Self> {
        array.get(i)
    }
}

impl<'a> FromArray<'a> for Option<i64> {
    type Container = I64Array;

    fn from_array(array: &'a I64Array, i: usize) -> Option<Self> {
        Some(array.get(i))
    }
}

impl IntoArray for i64 {
    type Container = I64Array;

    fn into_element(self) -> Result<Option<i64>, Exception> {
        Ok(Some(self))
    }
}

impl IntoArray for Option<i64> {
    type Container = I64Array;

    fn into_element(self) -> Result<Option<i64>, Exception> {
        Ok(self)
    }
}

impl IntoArray for Result<Option<i64>, Exception> {
    type Container = I64Array;

    fn into_element(self) -> Result<Option<i64>, Exception> {
        self
    }
}

impl<'a> FromArray<'a> for f64 {
    type Container = F64Array;

    fn from_array(array: &'a F64Array, i: usize) -> Option<Self> {
        array.get(i)
    }
}

impl<'a> FromArray<'a> for Option<f64> {
    type Container = F64Array;

    fn from_array(array: &'a F64Array, i: usize) -> Option<Self> {
        Some(array.get(i))
    }
}

impl IntoArray for f64 {
    type Container = F64Array;

    fn into_element(self) -> Result<Option<f64>, Exception> {
        Ok(Some(self))
    }
}

impl IntoArray for Option<f64> {
    type Container = F64Array;

    fn into_element(self) -> Result<Option<f64>, Exception> {
        Ok(self)
    }
}

impl IntoArray for Result<Option<f64>, Exception> {
    type Container = F64Array;

    fn into_element(self) -> Result<Option<f64>, Exception> {
        self
    }
}

impl<'a> FromArray<'a> for Date<Utc> {
    type Container = DateArray;

    fn from_array(array: &'a DateArray, i: usize) -> Option<Self> {
        array.get(i).map(date)
    }
}

impl<'a> FromArray<'a> for Option<Date<Utc>> {
    type Container = DateArray;

    fn from_array(array: &'a DateArray, i: usize) -> Option<Self> {
        Some(array.get(i).map(date))
    }
}

impl IntoArray for Date<Utc> {
    type Container = DateArray;

    fn into_element(self) -> Result<Option<i32>, Exception> {
        Ok(Some(epoch_date(self)))
    }
}

impl IntoArray for Option<Date<Utc>> {
    type Container = DateArray;

    fn into_element(self) -> Result<Option<i32>, Exception> {
        Ok(self.map(epoch_date))
    }
}

impl IntoArray for Result<Option<Date<Utc>>, Exception> {
    type Container = DateArray;

    fn into_element(self) -> Result<Option<i32>, Exception> {
        self.map(|option| option.map(epoch_date))
    }
}

impl<'a> FromArray<'a> for DateTime<Utc> {
    type Container = TimestampArray;

    fn from_array(array: &'a TimestampArray, i: usize) -> Option<Self> {
        array.get(i).map(timestamp)
    }
}

impl<'a> FromArray<'a> for Option<DateTime<Utc>> {
    type Container = TimestampArray;

    fn from_array(array: &'a TimestampArray, i: usize) -> Option<Self> {
        Some(array.get(i).map(timestamp))
    }
}

impl IntoArray for DateTime<Utc> {
    type Container = TimestampArray;

    fn into_element(self) -> Result<Option<i64>, Exception> {
        Ok(Some(epoch_micros(self)))
    }
}

impl IntoArray for Option<DateTime<Utc>> {
    type Container = TimestampArray;

    fn into_element(self) -> Result<Option<i64>, Exception> {
        Ok(self.map(epoch_micros))
    }
}

impl IntoArray for Result<Option<DateTime<Utc>>, Exception> {
    type Container = TimestampArray;

    fn into_element(self) -> Result<Option<i64>, Exception> {
        self.map(|option| option.map(epoch_micros))
    }
}

impl<'a> FromArray<'a> for &'a str {
    type Container = StringArray;

    fn from_array(array: &'a StringArray, i: usize) -> Option<Self> {
        array.get_str(i)
    }
}

impl<'a> FromArray<'a> for Option<&'a str> {
    type Container = StringArray;

    fn from_array(array: &'a StringArray, i: usize) -> Option<Self> {
        Some(array.get_str(i))
    }
}

impl IntoArray for String {
    type Container = StringArray;

    fn into_element(self) -> Result<Option<String>, Exception> {
        Ok(Some(self))
    }
}

impl IntoArray for Option<String> {
    type Container = StringArray;

    fn into_element(self) -> Result<Option<String>, Exception> {
        Ok(self)
    }
}

impl IntoArray for Result<Option<String>, Exception> {
    type Container = StringArray;

    fn into_element(self) -> Result<Option<String>, Exception> {
        self
    }
}

impl<'a, A: FromArray<'a>> ArrayExt<'a, A> for A::Container {
    fn map<B: IntoArray>(&'a self, f: impl Fn(A) -> B) -> Result<AnyArray, Exception> {
        let mut output = B::Container::with_capacity(self.len());
        for i in 0..self.len() {
            if let Some(next) = <A as FromArray<'a>>::from_array(self, i) {
                output.push(f(next).into_element()?);
            } else {
                output.push(None);
            }
        }
        Ok(output.as_any())
    }

    fn bi_map<'b, B: FromArray<'b>, C: IntoArray>(
        &'a self,
        right: &'b B::Container,
        f: impl Fn(A, B) -> C,
    ) -> Result<AnyArray, Exception> {
        assert_eq!(self.len(), right.len());
        let mut output = C::Container::with_capacity(self.len());
        for i in 0..self.len() {
            match (
                <A as FromArray<'a>>::from_array(&self, i),
                <B as FromArray<'b>>::from_array(right, i),
            ) {
                (Some(a), Some(b)) => output.push(f(a, b).into_element()?),
                _ => output.push(None),
            }
        }
        Ok(output.as_any())
    }

    fn tri_map<'b, 'c, B: FromArray<'b>, C: FromArray<'c>, D: IntoArray>(
        &'a self,
        middle: &'b B::Container,
        right: &'c C::Container,
        f: impl Fn(A, B, C) -> D,
    ) -> Result<AnyArray, Exception> {
        assert_eq!(self.len(), middle.len());
        assert_eq!(self.len(), right.len());
        let mut output = D::Container::with_capacity(self.len());
        for i in 0..self.len() {
            match (
                <A as FromArray<'a>>::from_array(&self, i),
                <B as FromArray<'b>>::from_array(middle, i),
                <C as FromArray<'c>>::from_array(right, i),
            ) {
                (Some(a), Some(b), Some(c)) => output.push(f(a, b, c).into_element()?),
                _ => output.push(None),
            }
        }
        Ok(output.as_any())
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
