use arrow::array::*;
use arrow::compute::SortColumn;
use arrow::record_batch::*;
use std::sync::Arc;

pub trait SortProvider {
    fn sort(values: &Self) -> UInt32Array;
}

pub fn sort<P: SortProvider>(values: &P) -> UInt32Array {
    P::sort(values)
}

macro_rules! primitive {
    ($T:ty) => {
        impl SortProvider for $T {
            fn sort(values: &Self) -> UInt32Array {
                let values = crate::util::any(values);
                arrow::compute::sort_to_indices(&values, None).unwrap()
            }
        }
    };
}

primitive!(BooleanArray);
primitive!(UInt32Array);
primitive!(Int64Array);
primitive!(Float64Array);
primitive!(Date32Array);
primitive!(TimestampMicrosecondArray);
primitive!(StringArray);

impl SortProvider for Arc<dyn Array> {
    fn sort(values: &Self) -> UInt32Array {
        arrow::compute::sort_to_indices(values, None).unwrap()
    }
}

impl SortProvider for Vec<Arc<dyn Array>> {
    fn sort(values: &Self) -> UInt32Array {
        let sort_columns: Vec<SortColumn> = values
            .iter()
            .map(|c| SortColumn {
                values: c.clone(),
                options: None,
            })
            .collect();
        arrow::compute::lexsort_to_indices(sort_columns.as_slice()).unwrap()
    }
}

impl SortProvider for RecordBatch {
    fn sort(values: &Self) -> UInt32Array {
        sort(&values.columns().to_vec())
    }
}
