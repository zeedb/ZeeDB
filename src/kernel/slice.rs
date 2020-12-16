use arrow::array::*;
use arrow::record_batch::*;
use std::ops::Range;
use std::sync::Arc;

pub trait SliceProvider {
    fn slice(values: &Self, range: Range<usize>) -> Self;
}

pub fn slice<P: SliceProvider>(values: &P, range: Range<usize>) -> P {
    P::slice(values, range)
}

macro_rules! primitive {
    ($T:ty) => {
        impl SliceProvider for $T {
            fn slice(values: &Self, range: Range<usize>) -> Self {
                Self::from(values.slice(range.start, range.end - range.start).data())
            }
        }
    };
}

primitive!(BooleanArray);
primitive!(Int64Array);
primitive!(Float64Array);
primitive!(Date32Array);
primitive!(TimestampMicrosecondArray);
primitive!(StringArray);

impl SliceProvider for Arc<dyn Array> {
    fn slice(values: &Self, range: Range<usize>) -> Self {
        values.slice(range.start, range.end - range.start)
    }
}

impl SliceProvider for RecordBatch {
    fn slice(values: &Self, range: Range<usize>) -> Self {
        let columns = values
            .columns()
            .iter()
            .map(|c| slice(c, range.clone()))
            .collect();
        RecordBatch::try_new(values.schema(), columns).unwrap()
    }
}
