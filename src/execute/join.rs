use crate::hash_table::HashTable;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use ast::*;
use kernel::{Bits, Error};
use std::sync::Arc;

pub fn hash_join(
    left: &HashTable,
    right: &RecordBatch,
    partition_right: &Vec<Arc<dyn Array>>,
    mut filter: impl FnMut(&RecordBatch) -> Result<BooleanArray, Error>,
    keep_unmatched_left: Option<&mut Bits>,
    keep_unmatched_right: bool,
) -> Result<RecordBatch, Error> {
    let (left_index, right_index) = left.probe(partition_right);
    let left_input = kernel::gather(&left.tuples, &left_index);
    let right_input = kernel::gather(right, &right_index);
    let input = kernel::zip(&left_input, &right_input);
    let mask = filter(&input)?;
    let matched = kernel::gather_logical(&input, &mask);
    if let Some(unmatched_left) = keep_unmatched_left {
        for i in 0..left_index.len() {
            if mask.value(i) {
                unmatched_left.unset(left_index.value(i) as usize)
            }
        }
    }
    if keep_unmatched_right {
        let matched_indexes = kernel::gather_logical(&right_index, &mask);
        let matched_mask = kernel::scatter(
            &kernel::falses(right.num_rows()),
            &matched_indexes,
            &kernel::trues(matched_indexes.len()),
        );
        let unmatched_mask = kernel::not(&matched_mask);
        let unmatched_right = kernel::gather_logical(right, &unmatched_mask);
        let unmatched_left = null_batch(unmatched_right.num_rows(), left_input.schema());
        let unmatched = kernel::zip(&unmatched_left, &unmatched_right);
        Ok(kernel::cat(&vec![matched, unmatched]))
    } else {
        Ok(matched)
    }
}

pub fn unmatched_tuples(
    left: &RecordBatch,
    unmatched_left: Bits,
    right: &Arc<Schema>,
) -> RecordBatch {
    let mask = unmatched_left.freeze();
    let unmatched_left = kernel::gather_logical(left, &mask);
    let unmatched_right = null_batch(unmatched_left.num_rows(), right.clone());
    kernel::zip(&unmatched_left, &unmatched_right)
}

pub fn nested_loop(
    left: &RecordBatch,
    right: &RecordBatch,
    mut filter: impl FnMut(&RecordBatch) -> Result<BooleanArray, Error>,
    keep_unmatched_left: Option<&mut Bits>,
    keep_unmatched_right: bool,
) -> Result<RecordBatch, Error> {
    let input = cross_product(left, right)?;
    let mask = filter(&input)?;
    let matched = kernel::gather_logical(&input, &mask);
    let mut result = vec![matched];
    if let Some(unmatched_left) = keep_unmatched_left {
        let left_mask = kernel::reshape_rows_none(&mask, right.num_rows());
        for i in 0..left.num_rows() {
            if left_mask.value(i) {
                unmatched_left.unset(i)
            }
        }
    }
    if keep_unmatched_right {
        let right_mask = kernel::reshape_columns_none(&mask, left.num_rows());
        let right_unmatched = kernel::gather_logical(right, &right_mask);
        let left_nulls = null_batch(right_unmatched.num_rows(), left.schema());
        result.push(kernel::zip(&left_nulls, &right_unmatched));
    }
    Ok(kernel::cat(&result))
}

pub fn nested_loop_semi(
    left: &RecordBatch,
    right: &RecordBatch,
    mut filter: impl FnMut(&RecordBatch) -> Result<BooleanArray, Error>,
) -> Result<RecordBatch, Error> {
    let input = cross_product(left, right)?;
    let mask = filter(&input)?;
    let right_mask = kernel::reshape_columns_any(&mask, left.num_rows());
    Ok(kernel::gather_logical(right, &right_mask))
}

pub fn nested_loop_anti(
    left: &RecordBatch,
    right: &RecordBatch,
    mut filter: impl FnMut(&RecordBatch) -> Result<BooleanArray, Error>,
) -> Result<RecordBatch, Error> {
    let input = cross_product(left, right)?;
    let mask = filter(&input)?;
    let right_mask = kernel::reshape_columns_none(&mask, left.num_rows());
    Ok(kernel::gather_logical(right, &right_mask))
}

pub fn nested_loop_single(
    left: &RecordBatch,
    right: &RecordBatch,
    mut filter: impl FnMut(&RecordBatch) -> Result<BooleanArray, Error>,
) -> Result<RecordBatch, Error> {
    let head = cross_product(left, &right)?;
    let mask = filter(&head)?;
    // TODO verify that each right row found at most one join partner on the left.
    let matched = kernel::gather_logical(&head, &mask);
    let right_mask = kernel::reshape_columns_none(&mask, left.num_rows());
    let right_unmatched = kernel::gather_logical(right, &right_mask);
    let left_nulls = null_batch(right_unmatched.num_rows(), left.schema());
    let unmatched = kernel::zip(&left_nulls, &right_unmatched);
    let combined = kernel::cat(&vec![matched, unmatched]);
    assert!(combined.num_rows() >= right.num_rows());
    Ok(combined)
}

pub fn nested_loop_mark(
    mark: &Column,
    left: &RecordBatch,
    right: &RecordBatch,
    mut filter: impl FnMut(&RecordBatch) -> Result<BooleanArray, Error>,
) -> Result<RecordBatch, Error> {
    let input = cross_product(left, right)?;
    let mask = filter(&input)?;
    let right_mask = kernel::reshape_columns_any(&mask, left.num_rows());
    let right_column = RecordBatch::try_new(
        Arc::new(Schema::new(vec![mark.into_query_field()])),
        vec![Arc::new(right_mask)],
    )
    .unwrap();
    Ok(kernel::zip(right, &right_column))
}

fn null_batch(num_rows: usize, schema: Arc<Schema>) -> RecordBatch {
    let columns = schema
        .fields()
        .iter()
        .map(|field| null_array(num_rows, field.data_type()))
        .collect();
    RecordBatch::try_new(schema, columns).unwrap()
}

pub fn null_array(len: usize, data_type: &DataType) -> Arc<dyn Array> {
    match data_type {
        DataType::Boolean => null_bool_array(len),
        DataType::Int64 => null_primitive_array::<Int64Type>(len),
        DataType::Float64 => null_primitive_array::<Float64Type>(len),
        DataType::Date32(DateUnit::Day) => null_primitive_array::<Date32Type>(len),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            null_primitive_array::<TimestampMicrosecondType>(len)
        }
        DataType::Utf8 => null_string_array(len),
        other => panic!("{:?} not supported", other),
    }
}

fn null_bool_array(num_rows: usize) -> Arc<dyn Array> {
    let mut array = BooleanArray::builder(num_rows);
    for _ in 0..num_rows {
        array.append_null().unwrap();
    }
    Arc::new(array.finish())
}

fn null_primitive_array<T: ArrowNumericType>(num_rows: usize) -> Arc<dyn Array> {
    let mut array = PrimitiveArray::<T>::builder(num_rows);
    for _ in 0..num_rows {
        array.append_null().unwrap();
    }
    Arc::new(array.finish())
}

fn null_string_array(num_rows: usize) -> Arc<dyn Array> {
    let mut array = StringBuilder::new(num_rows);
    for _ in 0..num_rows {
        array.append_null().unwrap();
    }
    Arc::new(array.finish())
}

fn cross_product(left: &RecordBatch, right: &RecordBatch) -> Result<RecordBatch, Error> {
    let (index_left, index_right) = index_cross_product(left.num_rows(), right.num_rows());
    let left = kernel::gather(left, &index_left);
    let right = kernel::gather(right, &index_right);
    Ok(kernel::zip(&left, &right))
}

fn index_cross_product(num_left: usize, num_right: usize) -> (UInt32Array, UInt32Array) {
    let num_rows = num_left * num_right;
    let mut index_left = UInt32Builder::new(num_rows);
    let mut index_right = UInt32Builder::new(num_rows);
    for i in 0..num_right {
        for j in 0..num_left {
            index_right.append_value(i as u32).unwrap();
            index_left.append_value(j as u32).unwrap();
        }
    }
    (index_left.finish(), index_right.finish())
}
