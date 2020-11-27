use crate::hash_table::HashTable;
use crate::state::State;
use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use ast::*;
use kernel::Error;
use std::sync::Arc;

pub fn hash_join(
    left: &HashTable,
    right: &RecordBatch,
    partition_right: &Vec<Scalar>,
    join: &Join,
    state: &mut State,
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
    state: &mut State,
) -> Result<RecordBatch, Error> {
    let buckets = crate::eval::hash(partition_right, left.n_buckets(), right, state)?;
    let (left, right_index) = left.probe(&buckets);
    let right = kernel::gather(right, &right_index);
    let input = kernel::zip(&left, &right);
    let mask = crate::eval::all(predicates, &input, state)?;
    Ok(kernel::gather_logical(&input, &mask))
}

pub fn nested_loop(
    left: &RecordBatch,
    right: &RecordBatch,
    join: &Join,
    state: &mut State,
) -> Result<RecordBatch, Error> {
    match join {
        Join::Inner(predicates) => nested_loop_inner(predicates, left, right, state),
        Join::Right(_) => todo!(),
        Join::Outer(_) => todo!(),
        Join::Semi(_) => todo!(),
        Join::Anti(_) => todo!(),
        Join::Single(predicates) => nested_loop_single(predicates, left, right, state),
        Join::Mark(_, _) => todo!(),
    }
}

fn nested_loop_inner(
    predicates: &Vec<Scalar>,
    left: &RecordBatch,
    right: &RecordBatch,
    state: &mut State,
) -> Result<RecordBatch, Error> {
    let input = cross_product(left, right)?;
    let mask = crate::eval::all(predicates, &input, state)?;
    Ok(kernel::gather_logical(&input, &mask))
}

fn nested_loop_single(
    predicates: &Vec<Scalar>,
    left: &RecordBatch,
    right: &RecordBatch,
    state: &mut State,
) -> Result<RecordBatch, Error> {
    let head = cross_product(left, &right)?;
    let mask = crate::eval::all(predicates, &head, state)?;
    let head = kernel::gather_logical(&head, &mask);
    let tail = unmatched(left, right, &mask);
    let combined = kernel::cat(&vec![head, tail]);
    assert!(combined.num_rows() >= right.num_rows());
    Ok(combined)
}

fn unmatched(left: &RecordBatch, right: &RecordBatch, mask: &Arc<dyn Array>) -> RecordBatch {
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
    let left = nulls(right.num_rows(), left.schema());
    let right = kernel::zip(&left, right);
    let index: Arc<dyn Array> = Arc::new(BooleanArray::from(unmatched));
    kernel::gather_logical(&right, &Arc::new(index))
}

fn nulls(num_rows: usize, schema: Arc<Schema>) -> RecordBatch {
    let columns = schema
        .fields()
        .iter()
        .map(|field| kernel::nulls(num_rows, field.data_type()))
        .collect();
    RecordBatch::try_new(schema, columns).unwrap()
}

fn cross_product(left: &RecordBatch, right: &RecordBatch) -> Result<RecordBatch, Error> {
    let (index_left, index_right) = index_cross_product(left.num_rows(), right.num_rows());
    let index_left: Arc<dyn Array> = Arc::new(index_left);
    let index_right: Arc<dyn Array> = Arc::new(index_right);
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
