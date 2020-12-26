use crate::any_array::*;
use crate::record_batch::*;
use chrono::NaiveDateTime;

pub fn fixed_width(record_batch: &RecordBatch) -> String {
    let columns: Vec<Vec<String>> = record_batch
        .columns
        .iter()
        .map(|(name, array)| fixed_width_column(name, array))
        .collect();
    let widths: Vec<usize> = columns
        .iter()
        .map(|column| column.iter().map(|value| value.len()).max().unwrap_or(0))
        .collect();
    let padded: Vec<Vec<String>> = (0..columns.len())
        .map(|i| {
            columns[i]
                .iter()
                .map(|value| format!("{:1$}", value, widths[i]))
                .collect()
        })
        .collect();
    let rows: Vec<String> = (0..record_batch.len() + 1)
        .map(|i| {
            (0..record_batch.columns.len())
                .map(|j| padded[j][i].clone())
                .collect::<Vec<String>>()
                .join(" ")
        })
        .collect();
    rows.join("\n")
}

fn fixed_width_column(name: &String, array: &Array) -> Vec<String> {
    let values: Vec<String> = match array {
        Array::Bool(array) => (0..array.len())
            .map(|i| match array.get(i) {
                None => "NULL".to_string(),
                Some(value) => value.to_string(),
            })
            .collect(),
        Array::I64(array) => (0..array.len())
            .map(|i| match array.get(i) {
                None => "NULL".to_string(),
                Some(value) => value.to_string(),
            })
            .collect(),
        Array::F64(array) => (0..array.len())
            .map(|i| match array.get(i) {
                None => "NULL".to_string(),
                Some(value) => value.to_string(),
            })
            .collect(),
        Array::Date(array) => (0..array.len())
            .map(|i| match array.get(i) {
                None => "NULL".to_string(),
                Some(value) => NaiveDateTime::from_timestamp(value as i64 * SECONDS_IN_DAY, 0)
                    .format("%F")
                    .to_string(),
            })
            .collect(),
        Array::Timestamp(array) => (0..array.len())
            .map(|i| match array.get(i) {
                None => "NULL".to_string(),
                Some(value) => NaiveDateTime::from_timestamp(
                    value / MICROSECONDS,
                    (value % MICROSECONDS * MILLISECONDS) as u32,
                )
                .format("%F %T")
                .to_string(),
            })
            .collect(),
        Array::String(array) => (0..array.len())
            .map(|i| match array.get(i) {
                None => "NULL".to_string(),
                Some(value) => value.to_string(),
            })
            .collect(),
    };
    let mut header = vec![name.clone()];
    header.extend_from_slice(&values);
    header
}
// DataType::Date32(DateUnit::Day) => {
//     let column = as_primitive_array::<Date32Type>(column);
//     (0..column.len())
//         .map(|i| {
//             if column.is_null(i) {
//                 "NULL".to_string()
//             } else {
//                 column.value_as_date(i).unwrap().format("%F").to_string()
//             }
//         })
//         .collect()
// }
// DataType::Timestamp(TimeUnit::Microsecond, None) => {
//     let column: &TimestampMicrosecondArray =
//         as_primitive_array::<TimestampMicrosecondType>(column);
//     (0..column.len())
//         .map(|i| {
//             if column.is_null(i) {
//                 "NULL".to_string()
//             } else {
//                 column
//                     .value_as_datetime(i)
//                     .unwrap()
//                     .format("%F %T")
//                     .to_string()
//             }
//         })
//         .collect()
// }

/// Number of seconds in a day
const SECONDS_IN_DAY: i64 = 86_400;
/// Number of milliseconds in a second
const MILLISECONDS: i64 = 1_000;
/// Number of microseconds in a second
const MICROSECONDS: i64 = 1_000_000;
