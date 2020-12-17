use arrow::array::*;
use arrow::buffer::Buffer;
use arrow::datatypes::*;

pub fn not(mask: &BooleanArray) -> BooleanArray {
    // arrow::compute::not(mask).unwrap()
    let mut builder = ArrayData::builder(DataType::Boolean).len(mask.len());
    let data = !mask.data().buffers().first().unwrap();
    builder = builder.add_buffer(data);
    if let Some(nulls) = mask.data().null_buffer() {
        builder = builder.null_bit_buffer(nulls.clone());
    }
    BooleanArray::from(builder.build())
}

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

#[inline]
pub fn get_bit(data: &[u8], i: usize) -> bool {
    (data[i >> 3] & BIT_MASK[i & 7]) != 0
}

#[inline]
pub unsafe fn get_bit_raw(data: *const u8, i: usize) -> bool {
    (*data.add(i >> 3) & BIT_MASK[i & 7]) != 0
}

#[inline]
pub fn set_bit(data: &mut [u8], i: usize) {
    data[i >> 3] |= BIT_MASK[i & 7];
}

#[inline]
pub unsafe fn set_bit_raw(data: *mut u8, i: usize) {
    *data.add(i >> 3) |= BIT_MASK[i & 7];
}

#[inline]
pub fn unset_bit(data: &mut [u8], i: usize) {
    data[i >> 3] &= !BIT_MASK[i & 7];
}

#[inline]
pub unsafe fn unset_bit_raw(data: *mut u8, i: usize) {
    *data.add(i >> 3) &= !BIT_MASK[i & 7];
}

static BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
