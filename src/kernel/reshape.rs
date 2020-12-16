use arrow::array::*;

/// Shrink mask by or-ing stride-length stretches of values together.
pub fn reshape_any(mask: &BooleanArray, stride: usize) -> BooleanArray {
    let mut result = vec![false].repeat(mask.len() / stride);
    for i in 0..mask.len() {
        result[i / stride] |= mask.value(i)
    }
    BooleanArray::from(result)
}

/// Shrink mask by and-not-ing stride-length stretches of values together.
pub fn reshape_none(mask: &BooleanArray, stride: usize) -> BooleanArray {
    let mut result = vec![true].repeat(mask.len() / stride);
    for i in 0..mask.len() {
        result[i / stride] &= !mask.value(i)
    }
    BooleanArray::from(result)
}
