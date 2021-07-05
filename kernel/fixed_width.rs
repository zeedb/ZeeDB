use crate::{
    dates::{date, timestamp},
    AnyArray, Array, RecordBatch,
};

pub fn fixed_width(batch: &RecordBatch) -> String {
    let header: Vec<Vec<String>> = batch
        .columns
        .iter()
        .map(|(name, _)| vec![name.clone()])
        .collect();
    let data: Vec<Vec<String>> = batch
        .columns
        .iter()
        .map(|(_, array)| fixed_width_column(array))
        .collect();
    let columns = cat(header, data);
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
    let rows: Vec<String> = (0..columns[0].len())
        .map(|i| {
            (0..columns.len())
                .map(|j| padded[j][i].clone())
                .collect::<Vec<String>>()
                .join(" ")
        })
        .collect();
    rows.join("\n")
}

fn fixed_width_column(array: &AnyArray) -> Vec<String> {
    match array {
        AnyArray::Bool(array) => (0..array.len())
            .map(|i| match array.get(i) {
                None => "NULL".to_string(),
                Some(value) => value.to_string(),
            })
            .collect(),
        AnyArray::I64(array) => (0..array.len())
            .map(|i| match array.get(i) {
                None => "NULL".to_string(),
                Some(value) => value.to_string(),
            })
            .collect(),
        AnyArray::F64(array) => (0..array.len())
            .map(|i| match array.get(i) {
                None => "NULL".to_string(),
                Some(value) => format!("{:.3}", value),
            })
            .collect(),
        AnyArray::Date(array) => (0..array.len())
            .map(|i| match array.get(i) {
                None => "NULL".to_string(),
                Some(value) => date(value).format("%F").to_string(),
            })
            .collect(),
        AnyArray::Timestamp(array) => (0..array.len())
            .map(|i| match array.get(i) {
                None => "NULL".to_string(),
                Some(value) => timestamp(value).format("%F %T").to_string(),
            })
            .collect(),
        AnyArray::String(array) => (0..array.len())
            .map(|i| match array.get(i) {
                None => "NULL".to_string(),
                Some(value) => value.to_string(),
            })
            .collect(),
    }
}

fn cat(header: Vec<Vec<String>>, data: Vec<Vec<String>>) -> Vec<Vec<String>> {
    let mut columns = header;
    for i in 0..data.len() {
        columns[i].extend_from_slice(data[i].as_slice())
    }
    columns
}
