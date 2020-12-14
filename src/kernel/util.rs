use arrow::array::*;
use std::sync::Arc;

pub(crate) fn any<T: Array>(array: &T) -> Arc<dyn Array> {
    make_array(array.data())
}
