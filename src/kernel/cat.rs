use arrow::array::*;
use arrow::record_batch::*;
use std::sync::Arc;

pub trait CatProvider: Sized {
    fn cat(values: &Vec<Self>) -> Self;
}

pub fn cat<P: CatProvider>(values: &Vec<P>) -> P {
    P::cat(values)
}

macro_rules! primitive {
    ($T:ty) => {
        impl CatProvider for $T {
            fn cat(values: &Vec<Self>) -> Self {
                let values: Vec<Arc<dyn Array>> =
                    values.iter().map(|c| crate::util::any(c)).collect();
                let result = arrow::compute::concat(values.as_slice()).unwrap();
                Self::from(result.data())
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

impl CatProvider for Arc<dyn Array> {
    fn cat(values: &Vec<Self>) -> Self {
        arrow::compute::concat(values.as_slice()).unwrap()
    }
}

impl CatProvider for Vec<Arc<dyn Array>> {
    fn cat(batches: &Vec<Self>) -> Self {
        let num_columns = batches[0].len();
        (0..num_columns)
            .map(|i| {
                let column: Vec<_> = batches.iter().map(|batch| batch[i].clone()).collect();
                cat(&column)
            })
            .collect()
    }
}

impl CatProvider for RecordBatch {
    fn cat(batches: &Vec<Self>) -> Self {
        let columns: Vec<Vec<Arc<dyn Array>>> = batches
            .iter()
            .map(|batch| batch.columns().to_vec())
            .collect();
        RecordBatch::try_new(batches[0].schema(), cat(&columns)).unwrap()
    }
}
