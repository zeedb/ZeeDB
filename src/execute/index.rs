use crate::byte_key::ByteKey;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use std::ops::Range;
use std::sync::Arc;
use storage::Art;

pub(crate) fn insert(
    index: &mut Art,
    columns: &Vec<String>,
    input: &RecordBatch,
    tids: &UInt64Array,
) {
    // Convert all the index columns into bytes, in lexicographic order.
    let mut index_columns = vec![];
    for column in columns {
        for (i, field) in input.schema().fields().iter().enumerate() {
            if storage::base_name(field.name()) == column {
                index_columns.push(bytes(input.column(i)));
            }
        }
    }
    index_columns.push(bytes_static(tids));
    // Insert the keys into the index.
    let keys = zip(&index_columns);
    for i in 0..keys.len() {
        index.insert(keys.value(i), tids.value(i));
    }
}

pub fn key(columns: &Vec<Arc<dyn Array>>) -> GenericBinaryArray<i32> {
    todo!()
}

pub fn prefix(key: &[u8]) -> Range<&[u8]> {
    todo!()
}

fn bytes(column: &Arc<dyn Array>) -> GenericBinaryArray<i32> {
    match column.data_type() {
        DataType::Boolean => bytes_bool(column),
        DataType::Int64 => bytes_generic::<Int64Type>(column),
        DataType::UInt32 => bytes_generic::<UInt32Type>(column),
        DataType::UInt64 => bytes_generic::<UInt64Type>(column),
        DataType::Float64 => bytes_generic::<Float64Type>(column),
        DataType::FixedSizeBinary(16) => todo!(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            bytes_generic::<TimestampMicrosecondType>(column)
        }
        DataType::Date32(DateUnit::Day) => bytes_generic::<Date32Type>(column),
        DataType::Utf8 => bytes_utf8(column),
        other => panic!("type {:?} is not supported", other),
    }
}

fn bytes_bool(column: &Arc<dyn Array>) -> GenericBinaryArray<i32> {
    // TODO deal with nulls somehow. Encode as 0 and accept collisions? Encode as empty array?
    let column: &BooleanArray = column.as_any().downcast_ref::<BooleanArray>().unwrap();
    let mut buffer = BinaryBuilder::new(column.len());
    for i in 0..column.len() {
        buffer
            .append_value(if column.value(i) { &[1u8] } else { &[0u8] })
            .unwrap();
    }
    buffer.finish()
}

fn bytes_generic<T>(column: &Arc<dyn Array>) -> GenericBinaryArray<i32>
where
    T: ArrowNumericType,
    T::Native: ByteKey,
{
    // TODO deal with nulls somehow. Encode as 0 and accept collisions? Encode as empty array?
    let column: &PrimitiveArray<T> = column.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    bytes_static(column)
}

fn bytes_static<T>(column: &PrimitiveArray<T>) -> GenericBinaryArray<i32>
where
    T: ArrowNumericType,
    T::Native: ByteKey,
{
    let mut buffer = BinaryBuilder::new(column.len() * std::mem::size_of::<T::Native>());
    for i in 0..column.len() {
        buffer
            .append_value(column.value(i).key().as_slice())
            .unwrap();
    }
    buffer.finish()
}

fn bytes_utf8(column: &Arc<dyn Array>) -> GenericBinaryArray<i32> {
    // TODO deal with nulls somehow. Encode as 0 and accept collisions? Encode as empty array?
    let column: &StringArray = column.as_any().downcast_ref::<StringArray>().unwrap();
    BinaryArray::from(
        ArrayData::builder(DataType::Binary)
            .add_buffer(column.value_data())
            .build(),
    )
}

fn zip(columns: &Vec<GenericBinaryArray<i32>>) -> GenericBinaryArray<i32> {
    let len = columns.iter().map(|c| c.len()).sum();
    let mut buffer = BinaryBuilder::new(len);
    for i in 0..columns[0].len() {
        for column in columns {
            for b in column.value(i) {
                buffer.append_byte(*b).unwrap();
            }
            buffer.append(true).unwrap();
        }
    }
    buffer.finish()
}
