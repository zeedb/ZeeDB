use arrow::array::*;
use arrow::record_batch::*;
use std::sync::Arc;

pub trait GatherProvider {
    fn gather(values: &Self, indexes: &UInt32Array) -> Self;
    fn gather_logical(values: &Self, mask: &BooleanArray) -> Self;
}

pub fn gather<P: GatherProvider>(values: &P, indexes: &UInt32Array) -> P {
    P::gather(values, indexes)
}

pub fn gather_logical<P: GatherProvider>(values: &P, mask: &BooleanArray) -> P {
    P::gather_logical(values, mask)
}

macro_rules! primitive {
    ($T:ty) => {
        impl GatherProvider for $T {
            fn gather(values: &Self, indexes: &UInt32Array) -> Self {
                let any = arrow::compute::take(&crate::util::any(values), indexes, None).unwrap();
                Self::from(any.data())
            }

            fn gather_logical(values: &Self, mask: &BooleanArray) -> Self {
                let any = arrow::compute::filter(crate::util::any(values).as_ref(), mask).unwrap();
                Self::from(any.data())
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

impl GatherProvider for Arc<dyn Array> {
    fn gather(values: &Self, indexes: &UInt32Array) -> Self {
        arrow::compute::take(values, indexes, None).unwrap()
    }

    fn gather_logical(values: &Self, mask: &BooleanArray) -> Self {
        arrow::compute::filter(values.as_ref(), mask).unwrap()
    }
}

impl GatherProvider for Vec<Arc<dyn Array>> {
    fn gather(values: &Self, indexes: &UInt32Array) -> Self {
        values.iter().map(|c| gather(c, indexes)).collect()
    }

    fn gather_logical(values: &Self, mask: &BooleanArray) -> Self {
        values.iter().map(|c| gather_logical(c, mask)).collect()
    }
}

impl GatherProvider for RecordBatch {
    fn gather(values: &Self, indexes: &UInt32Array) -> Self {
        RecordBatch::try_new(values.schema(), gather(&values.columns().to_vec(), indexes)).unwrap()
    }

    fn gather_logical(values: &Self, mask: &BooleanArray) -> Self {
        RecordBatch::try_new(
            values.schema(),
            gather_logical(&values.columns().to_vec(), mask),
        )
        .unwrap()
    }
}
