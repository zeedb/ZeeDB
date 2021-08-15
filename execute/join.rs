use ast::*;
use kernel::*;

use crate::hash_table::HashTable;

pub fn hash_join(
    left: &HashTable,
    right: &RecordBatch,
    partition_right: &I64Array,
    filter: impl Fn(&RecordBatch) -> Result<BoolArray, String>,
    keep_unmatched_left: Option<&mut BoolArray>,
    keep_unmatched_right: bool,
) -> Result<RecordBatch, String> {
    let (left_index, right_index) = left.probe(partition_right);
    let left_input = left.build().gather(&left_index);
    let right_input = right.gather(&right_index);
    let input = RecordBatch::zip(left_input, right_input);
    let mask = filter(&input)?;
    let matched = input.compress(&mask);
    if let Some(unmatched_left) = keep_unmatched_left {
        let matched_indexes = left_index.compress(&mask);
        BoolArray::falses(matched_indexes.len()).scatter(&matched_indexes, unmatched_left);
    }
    if keep_unmatched_right {
        let matched_indexes = right_index.compress(&mask);
        let mut unmatched_mask = BoolArray::trues(right.len());
        BoolArray::falses(matched_indexes.len()).scatter(&matched_indexes, &mut unmatched_mask);
        let unmatched_right = right.compress(&unmatched_mask);
        let unmatched_left = RecordBatch::nulls(left.build().schema(), unmatched_right.len());
        let unmatched = RecordBatch::zip(unmatched_left, unmatched_right);
        Ok(RecordBatch::cat(vec![matched, unmatched]).unwrap())
    } else {
        Ok(matched)
    }
}

pub fn hash_join_semi(
    left: &HashTable,
    right: &RecordBatch,
    partition_right: &I64Array,
    filter: impl Fn(&RecordBatch) -> Result<BoolArray, String>,
) -> Result<RecordBatch, String> {
    let (left_index, right_index) = left.probe(partition_right);
    let left_input = left.build().gather(&left_index);
    let right_input = right.gather(&right_index);
    let input = RecordBatch::zip(left_input, right_input);
    let mask = filter(&input)?;
    let matched_indexes = right_index.compress(&mask);
    let mut matched_mask = BoolArray::trues(right.len());
    BoolArray::trues(matched_indexes.len()).scatter(&matched_indexes, &mut matched_mask);
    let result = right.compress(&matched_mask);
    assert!(
        result.len() <= right.len(),
        "Result of semi-join #{} is larger than input #{}",
        result.len(),
        right.len()
    );
    Ok(result)
}

pub fn hash_join_anti(
    left: &HashTable,
    right: &RecordBatch,
    partition_right: &I64Array,
    filter: impl Fn(&RecordBatch) -> Result<BoolArray, String>,
) -> Result<RecordBatch, String> {
    let (left_index, right_index) = left.probe(partition_right);
    let left_input = left.build().gather(&left_index);
    let right_input = right.gather(&right_index);
    let input = RecordBatch::zip(left_input, right_input);
    let mask = filter(&input)?;
    let matched_indexes = right_index.compress(&mask);
    let mut unmatched_mask = BoolArray::trues(right.len());
    BoolArray::falses(matched_indexes.len()).scatter(&matched_indexes, &mut unmatched_mask);
    let result = right.compress(&unmatched_mask);
    assert!(
        result.len() <= right.len(),
        "Result of anti-join #{} is larger than input #{}",
        result.len(),
        right.len()
    );
    Ok(result)
}

pub fn hash_join_single(
    left: &HashTable,
    right: &RecordBatch,
    partition_right: &I64Array,
    filter: impl Fn(&RecordBatch) -> Result<BoolArray, String>,
) -> Result<RecordBatch, String> {
    let (left_index, right_index) = left.probe(partition_right);
    let left_input = left.build().gather(&left_index);
    let right_input = right.gather(&right_index);
    let input = RecordBatch::zip(left_input, right_input);
    let mask = filter(&input)?;
    if right_index.conflict(&mask, right.len()) {
        panic!("Scalar subquery produced more than one element");
    }
    let matched = input.compress(&mask);
    let matched_indexes = right_index.compress(&mask);
    let mut unmatched_mask = BoolArray::trues(right.len());
    BoolArray::falses(matched_indexes.len()).scatter(&matched_indexes, &mut unmatched_mask);
    let right_unmatched = right.compress(&unmatched_mask);
    let left_nulls = RecordBatch::nulls(left.build().schema(), right_unmatched.len());
    let unmatched = RecordBatch::zip(left_nulls, right_unmatched);
    let result = RecordBatch::cat(vec![matched, unmatched]).unwrap();
    assert_eq!(
        result.len(),
        right.len(),
        "Result of single join #{} is not the same size as input #{}",
        result.len(),
        right.len()
    );
    Ok(result)
}

pub fn hash_join_mark(
    mark: &Column,
    left: &HashTable,
    right: &RecordBatch,
    partition_right: &I64Array,
    filter: impl Fn(&RecordBatch) -> Result<BoolArray, String>,
) -> Result<RecordBatch, String> {
    let (left_index, right_index) = left.probe(partition_right);
    let left_input = left.build().gather(&left_index);
    let right_input = right.gather(&right_index);
    let input = RecordBatch::zip(left_input, right_input);
    let mask = filter(&input)?;
    let matched_indexes = right_index.compress(&mask);
    let mut matched_mask = BoolArray::falses(right.len());
    BoolArray::trues(matched_indexes.len()).scatter(&matched_indexes, &mut matched_mask);
    let right_column =
        RecordBatch::new(vec![(mark.canonical_name(), AnyArray::Bool(matched_mask))]);
    let result = RecordBatch::zip(right.clone(), right_column);
    assert_eq!(
        result.len(),
        right.len(),
        "Result of mark join #{} is not the same size as input #{}",
        result.len(),
        right.len()
    );
    Ok(result)
}

pub fn unmatched_tuples(
    left: &RecordBatch,
    unmatched_left: &BoolArray,
    right: &Vec<(String, DataType)>,
) -> Result<RecordBatch, String> {
    let unmatched_left = left.compress(unmatched_left);
    let unmatched_right = RecordBatch::nulls(right.clone(), unmatched_left.len());
    Ok(RecordBatch::zip(unmatched_left, unmatched_right))
}

pub fn nested_loop(
    left: &RecordBatch,
    right: &RecordBatch,
    filter: impl Fn(&RecordBatch) -> Result<BoolArray, String>,
    keep_unmatched_left: Option<&mut BoolArray>,
    keep_unmatched_right: bool,
) -> Result<RecordBatch, String> {
    let input = cross_product(left, right);
    let mask = filter(&input)?;
    let matched = input.compress(&mask);
    let mut result = vec![matched];
    if let Some(unmatched_left) = keep_unmatched_left {
        let left_matched_mask = mask.transpose(right.len()).any(left.len());
        *unmatched_left = unmatched_left.and_not(&left_matched_mask);
    }
    if keep_unmatched_right {
        let right_unmatched_mask = mask.none(right.len());
        let right_unmatched = right.compress(&right_unmatched_mask);
        let left_nulls = RecordBatch::nulls(left.schema(), right_unmatched.len());
        result.push(RecordBatch::zip(left_nulls, right_unmatched));
    }
    Ok(RecordBatch::cat(result).unwrap())
}

pub fn nested_loop_semi(
    left: &RecordBatch,
    right: &RecordBatch,
    filter: impl Fn(&RecordBatch) -> Result<BoolArray, String>,
) -> Result<RecordBatch, String> {
    let input = cross_product(left, right);
    let mask = filter(&input)?;
    let right_mask = mask.any(right.len());
    Ok(right.compress(&right_mask))
}

pub fn nested_loop_anti(
    left: &RecordBatch,
    right: &RecordBatch,
    filter: impl Fn(&RecordBatch) -> Result<BoolArray, String>,
) -> Result<RecordBatch, String> {
    let input = cross_product(left, right);
    let mask = filter(&input)?;
    let right_mask = mask.none(right.len());
    Ok(right.compress(&right_mask))
}

pub fn nested_loop_single(
    left: &RecordBatch,
    right: &RecordBatch,
    filter: impl Fn(&RecordBatch) -> Result<BoolArray, String>,
) -> Result<RecordBatch, String> {
    let head = cross_product(left, &right);
    let mask = filter(&head)?;
    let count = mask.count(right.len());
    if count.greater_scalar(Some(1)).any(1).get(0).unwrap() {
        panic!("Scalar subquery produced more than one element");
    }
    let matched = head.compress(&mask);
    let right_mask = mask.none(right.len());
    let right_unmatched = right.compress(&right_mask);
    let left_nulls = RecordBatch::nulls(left.schema(), right_unmatched.len());
    let unmatched = RecordBatch::zip(left_nulls, right_unmatched);
    assert!(matched.len() + unmatched.len() >= right.len());
    Ok(RecordBatch::cat(vec![matched, unmatched]).unwrap())
}

pub fn nested_loop_mark(
    mark: &Column,
    left: &RecordBatch,
    right: &RecordBatch,
    filter: impl Fn(&RecordBatch) -> Result<BoolArray, String>,
) -> Result<RecordBatch, String> {
    let input = cross_product(left, right);
    let mask = filter(&input)?;
    let right_mask = mask.any(right.len());
    let right_column = RecordBatch::new(vec![(mark.canonical_name(), AnyArray::Bool(right_mask))]);
    Ok(RecordBatch::zip(right.clone(), right_column))
}

fn cross_product(left: &RecordBatch, right: &RecordBatch) -> RecordBatch {
    let n_left = left.len();
    let n_right = right.len();
    let left = left.repeat(n_right).transpose(n_left);
    let right = right.repeat(n_left);
    RecordBatch::zip(left, right)
}
