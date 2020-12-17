use arrow::array::BooleanArray;

pub fn trues(len: usize) -> BooleanArray {
    BooleanArray::from(vec![true].repeat(len))
}

pub fn falses(len: usize) -> BooleanArray {
    BooleanArray::from(vec![false].repeat(len))
}
