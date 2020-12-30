use chrono::*;

pub(crate) fn date(value: i32) -> Date<Utc> {
    let naive = NaiveDate::from_ymd(1970, 1, 1) + Duration::days(value as i64);
    Utc.from_utc_date(&naive)
}

pub(crate) fn timestamp(value: i64) -> DateTime<Utc> {
    Utc.timestamp(
        value / MICROSECONDS,
        (value % MICROSECONDS * MILLISECONDS) as u32,
    )
}

pub(crate) fn cast_date_as_timestamp(value: i32) -> i64 {
    let ts = date(value).and_hms(0, 0, 0);
    epoch_micros(ts)
}

pub(crate) fn cast_timestamp_as_date(value: i64) -> i32 {
    let duration = timestamp(value).date() - date(0);
    duration.num_days() as i32
}

pub fn parse_date(value: &str) -> i32 {
    let naive = NaiveDate::parse_from_str(value, "%Y-%m-%d").expect(value);
    epoch_date(Utc.from_utc_date(&naive))
}

pub fn parse_timestamp(value: &str) -> i64 {
    let ts = DateTime::parse_from_rfc3339(value)
        .expect(value)
        .with_timezone(&Utc);
    epoch_micros(ts)
}

fn epoch_micros(ts: DateTime<Utc>) -> i64 {
    ts.timestamp() * MICROSECONDS + ts.timestamp_subsec_micros() as i64
}

fn epoch_date(d: Date<Utc>) -> i32 {
    (d - date(0)).num_days() as i32
}

/// Number of milliseconds in a second
const MILLISECONDS: i64 = 1_000;
/// Number of microseconds in a second
const MICROSECONDS: i64 = 1_000_000;
