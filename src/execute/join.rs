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
        Join::Right(_) => todo!(),
        Join::Outer(_) => todo!(),
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
    let (left, right_index) = left.probe(&partition_right?);
    let right = kernel::gather(right, &right_index);
    let input = kernel::zip(&left, &right);
    let mask = crate::eval::all(predicates, &input, state)?;
    Ok(kernel::gather_logical(&input, &mask))
}

pub fn nested_loop(
    left: &RecordBatch,
    right: &RecordBatch,
    join: &Join,
    state: &mut Session,
) -> Result<RecordBatch, Error> {
    match join {
        Join::Inner(predicates) => nested_loop_inner(predicates, left, right, state),
        Join::Right(_) => todo!(),
        Join::Outer(_) => todo!(),
        Join::Semi(predicates) => nested_loop_semi(predicates, left, right, state),
        Join::Anti(predicates) => nested_loop_anti(predicates, left, right, state),
        Join::Single(predicates) => nested_loop_single(predicates, left, right, state),
        Join::Mark(_, _) => todo!(),
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

fn nested_loop_semi(
    predicates: &Vec<Scalar>,
    left: &RecordBatch,
    right: &RecordBatch,
    state: &mut Session,
) -> Result<RecordBatch, Error> {
    let input = cross_product(left, right)?;
    let mask = crate::eval::all(predicates, &input, state)?;
    let right_mask = kernel::reshape_any(&mask, left.num_rows());
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
    let right_mask = kernel::reshape_none(&mask, left.num_rows());
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
    let tail = unmatched(left, right, &mask);
    let combined = kernel::cat(&vec![head, tail]);
    assert!(combined.num_rows() >= right.num_rows());
    Ok(combined)
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
