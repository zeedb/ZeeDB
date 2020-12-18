use crate::execute::Session;
use crate::hash_table::HashTable;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use ast::*;
use kernel::Error;
use std::sync::Arc;

pub fn hash_join(
    left: &HashTable,
    right: &RecordBatch,
    partition_right: &Vec<Scalar>,
    join: &Join,
    state: &mut Session,
) -> Result<RecordBatch, Error> {
    match join {
        Join::Inner(predicates) => hash_join_inner(predicates, left, right, partition_right, state),
        Join::Right(predicates) => hash_join_right(predicates, left, right, partition_right, state),
        Join::Outer(_) => panic!("call hash_join_outer separately"),
        Join::Semi(_) => todo!(),
        Join::Anti(_) => todo!(),
        Join::Single(_) => todo!(),
        Join::Mark(_, _) => todo!(),
    }
}

fn hash_join_inner(
    predicates: &Vec<Scalar>,
    left: &HashTable,
    right: &RecordBatch,
    partition_right: &Vec<Scalar>,
    state: &mut Session,
) -> Result<RecordBatch, Error> {
    let partition_right: Result<Vec<_>, _> = partition_right
        .iter()
        .map(|x| crate::eval::eval(x, right, state))
        .collect();
    let (left_index, right_index) = left.probe(&partition_right?);
    let left_input = kernel::gather(&left.tuples, &left_index);
    let right_input = kernel::gather(right, &right_index);
    let input = kernel::zip(&left_input, &right_input);
    let mask = crate::eval::all(predicates, &input, state)?;
    Ok(kernel::gather_logical(&input, &mask))
}

fn hash_join_right(
    predicates: &Vec<Scalar>,
    left: &HashTable,
    right: &RecordBatch,
    partition_right: &Vec<Scalar>,
    state: &mut Session,
) -> Result<RecordBatch, Error> {
    let partition_right: Result<Vec<_>, _> = partition_right
        .iter()
        .map(|x| crate::eval::eval(x, right, state))
        .collect();
    let (left_index, right_index) = left.probe(&partition_right?);
    let left_input = kernel::gather(&left.tuples, &left_index);
    let right_input = kernel::gather(right, &right_index);
    let input = kernel::zip(&left_input, &right_input);
    let mask = crate::eval::all(predicates, &input, state)?;
    let matched = kernel::gather_logical(&input, &mask);
    // Figure out which indexes on the right were not matched.
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
    // Stitch together the two parts.
    Ok(kernel::cat(&vec![matched, unmatched]))
}

pub fn hash_join_outer(
    predicates: &Vec<Scalar>,
    left: &HashTable,
    unmatched_left: &mut Vec<bool>,
    right: &RecordBatch,
    partition_right: &Vec<Scalar>,
    state: &mut Session,
) -> Result<RecordBatch, Error> {
    let partition_right: Result<Vec<_>, _> = partition_right
        .iter()
        .map(|x| crate::eval::eval(x, right, state))
        .collect();
    let (left_index, right_index) = left.probe(&partition_right?);
    let left_input = kernel::gather(&left.tuples, &left_index);
    let right_input = kernel::gather(right, &right_index);
    let input = kernel::zip(&left_input, &right_input);
    let mask = crate::eval::all(predicates, &input, state)?;
    let matched = kernel::gather_logical(&input, &mask);
    // Remember which indexes on the left were matched, so we can add unmatched left-side rows at the end.
    for i in 0..left_index.len() {
        if mask.value(i) {
            unmatched_left[left_index.value(i) as usize] = false;
        }
    }
    // Figure out which indexes on the right were not matched.
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
    // Stitch together the two parts.
    Ok(kernel::cat(&vec![matched, unmatched]))
}

pub fn hash_join_outer_unmatched(
    left: &HashTable,
    unmatched_left: Vec<bool>,
    right: &Arc<Schema>,
) -> RecordBatch {
    let mask = BooleanArray::from(unmatched_left);
    let unmatched_left = kernel::gather_logical(&left.tuples, &mask);
    let unmatched_right = null_batch(unmatched_left.num_rows(), right.clone());
    kernel::zip(&unmatched_left, &unmatched_right)
}

pub fn nested_loop(
    left: &RecordBatch,
    right: &RecordBatch,
    join: &Join,
    state: &mut Session,
) -> Result<RecordBatch, Error> {
    match join {
        Join::Inner(predicates) => nested_loop_inner(predicates, left, right, state),
        Join::Right(predicates) => nested_loop_right(predicates, left, right, state),
        Join::Outer(predicates) => nested_loop_outer(predicates, left, right, state),
        Join::Semi(predicates) => nested_loop_semi(predicates, left, right, state),
        Join::Anti(predicates) => nested_loop_anti(predicates, left, right, state),
        Join::Single(predicates) => nested_loop_single(predicates, left, right, state),
        Join::Mark(mark, predicates) => nested_loop_mark(mark, predicates, left, right, state),
    }
}

fn nested_loop_inner(
    predicates: &Vec<Scalar>,
    left: &RecordBatch,
    right: &RecordBatch,
    state: &mut Session,
) -> Result<RecordBatch, Error> {
    let input = cross_product(left, right)?;
    let mask = crate::eval::all(predicates, &input, state)?;
    Ok(kernel::gather_logical(&input, &mask))
}

fn nested_loop_right(
    predicates: &Vec<Scalar>,
    left: &RecordBatch,
    right: &RecordBatch,
    state: &mut Session,
) -> Result<RecordBatch, Error> {
    let input = cross_product(left, right)?;
    let mask = crate::eval::all(predicates, &input, state)?;
    let right_mask = kernel::reshape_columns_none(&mask, left.num_rows());
    let right_unmatched = kernel::gather_logical(right, &right_mask);
    let left_nulls = null_batch(right_unmatched.num_rows(), left.schema());
    Ok(kernel::cat(&vec![
        kernel::gather_logical(&input, &mask),
        kernel::zip(&left_nulls, &right_unmatched),
    ]))
}

fn nested_loop_outer(
    predicates: &Vec<Scalar>,
    left: &RecordBatch,
    right: &RecordBatch,
    state: &mut Session,
) -> Result<RecordBatch, Error> {
    let input = cross_product(left, right)?;
    let mask = crate::eval::all(predicates, &input, state)?;
    let left_mask = kernel::reshape_rows_none(&mask, right.num_rows());
    let left_unmatched = kernel::gather_logical(left, &left_mask);
    let right_nulls = null_batch(left_unmatched.num_rows(), left.schema());
    let right_mask = kernel::reshape_columns_none(&mask, left.num_rows());
    let right_unmatched = kernel::gather_logical(right, &right_mask);
    let left_nulls = null_batch(right_unmatched.num_rows(), left.schema());
    Ok(kernel::cat(&vec![
        kernel::gather_logical(&input, &mask),
        kernel::zip(&left_unmatched, &right_nulls),
        kernel::zip(&left_nulls, &right_unmatched),
    ]))
}

fn nested_loop_semi(
    predicates: &Vec<Scalar>,
    left: &RecordBatch,
    right: &RecordBatch,
    state: &mut Session,
) -> Result<RecordBatch, Error> {
    let input = cross_product(left, right)?;
    let mask = crate::eval::all(predicates, &input, state)?;
    let right_mask = kernel::reshape_columns_any(&mask, left.num_rows());
    Ok(kernel::gather_logical(right, &right_mask))
}

fn nested_loop_anti(
    predicates: &Vec<Scalar>,
    left: &RecordBatch,
    right: &RecordBatch,
    state: &mut Session,
) -> Result<RecordBatch, Error> {
    let input = cross_product(left, right)?;
    let mask = crate::eval::all(predicates, &input, state)?;
    let right_mask = kernel::reshape_columns_none(&mask, left.num_rows());
    Ok(kernel::gather_logical(right, &right_mask))
}

fn nested_loop_single(
    predicates: &Vec<Scalar>,
    left: &RecordBatch,
    right: &RecordBatch,
    state: &mut Session,
) -> Result<RecordBatch, Error> {
    let head = cross_product(left, &right)?;
    let mask = crate::eval::all(predicates, &head, state)?;
    let head = kernel::gather_logical(&head, &mask);
    // TODO use reshape_columns_none
    let tail = unmatched(left, right, &mask);
    let combined = kernel::cat(&vec![head, tail]);
    assert!(combined.num_rows() >= right.num_rows());
    Ok(combined)
}

fn nested_loop_mark(
    mark: &Column,
    predicates: &Vec<Scalar>,
    left: &RecordBatch,
    right: &RecordBatch,
    state: &mut Session,
) -> Result<RecordBatch, Error> {
    let input = cross_product(left, right)?;
    let mask = crate::eval::all(predicates, &input, state)?;
    let right_mask = kernel::reshape_columns_any(&mask, left.num_rows());
    let right_column = RecordBatch::try_new(
        Arc::new(Schema::new(vec![mark.into_query_field()])),
        vec![Arc::new(right_mask)],
    )
    .unwrap();
    Ok(kernel::zip(right, &right_column))
}

fn unmatched(left: &RecordBatch, right: &RecordBatch, mask: &BooleanArray) -> RecordBatch {
    let mask = mask.as_any().downcast_ref::<BooleanArray>().unwrap();
    let mut unmatched = Vec::with_capacity(right.num_rows());
    unmatched.resize(right.num_rows(), true);
    for i in 0..right.num_rows() {
        for j in 0..left.num_rows() {
            if mask.value(i * left.num_rows() + j) {
                unmatched[i] = false;
            }
        }
    }
    let left = null_batch(right.num_rows(), left.schema());
    let right = kernel::zip(&left, right);
    kernel::gather_logical(&right, &BooleanArray::from(unmatched))
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
