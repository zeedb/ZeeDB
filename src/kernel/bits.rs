use arrow::array::*;
use arrow::buffer::Buffer;
use arrow::datatypes::*;

pub fn bit_or<T: ArrowPrimitiveType>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> PrimitiveArray<T> {
    let mut builder = ArrayData::builder(left.data_type().clone()).len(left.len());
    let left_data = left.data();
    let right_data = right.data();
    let left_buffer = left_data.buffers().first().unwrap();
    let right_buffer = right_data.buffers().first().unwrap();
    let buffer = (left_buffer | right_buffer).unwrap();
    if let Some(nulls) = null_or(left, right) {
        builder = builder.null_bit_buffer(nulls);
    }
    builder = builder.add_buffer(buffer);
    PrimitiveArray::from(builder.build())
}

fn null_or<T: Array>(left: &T, right: &T) -> Option<Buffer> {
    let left_data = left.data();
    let left_nulls = left_data.null_buffer();
    let right_data = right.data();
    let right_nulls = right_data.null_buffer();
    match (left_nulls, right_nulls) {
        (Some(left), Some(right)) => Some((left | right).unwrap()),
        (Some(bits), _) | (_, Some(bits)) => Some(bits.clone()),
        (None, None) => None,
    }
}
