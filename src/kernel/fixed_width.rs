use crate::any_array::*;
use crate::dates::*;
use crate::record_batch::*;

pub fn fixed_width(batches: &Vec<RecordBatch>) -> String {
    let header: Vec<Vec<String>> = batches[0]
        .columns
        .iter()
        .map(|(name, _)| vec![name.clone()])
        .collect();
    let data: Vec<Vec<Vec<String>>> = batches
        .iter()
        .map(|batch| {
            batch
                .columns
                .iter()
                .map(|(_, array)| fixed_width_column(array))
                .collect()
        })
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

fn fixed_width_column(array: &Array) -> Vec<String> {
    match array {
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
                Some(value) => date(value).format("%F").to_string(),
            })
            .collect(),
        Array::Timestamp(array) => (0..array.len())
            .map(|i| match array.get(i) {
                None => "NULL".to_string(),
                Some(value) => timestamp(value).format("%F %T").to_string(),
            })
            .collect(),
        Array::String(array) => (0..array.len())
            .map(|i| match array.get(i) {
                None => "NULL".to_string(),
                Some(value) => value.to_string(),
            })
            .collect(),
    }
}

fn cat(header: Vec<Vec<String>>, data: Vec<Vec<Vec<String>>>) -> Vec<Vec<String>> {
    let mut columns = header;
    for batch in data {
        for i in 0..batch.len() {
            columns[i].extend_from_slice(batch[i].as_slice())
        }
    }
    columns
}