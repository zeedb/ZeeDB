use arrow::array::*;
use arrow::datatypes::*;
use arrow::error::ArrowError;
use arrow::record_batch::*;
use ast::Column;
use std::sync::Arc;

pub trait Kernel: Sized {
    fn cat(any: &Vec<Self>) -> Self;
    fn gather(any: &Self, uint32: &Arc<dyn Array>) -> Self;
    fn gather_logical(any: &Self, boolean: &Arc<dyn Array>) -> Self;
    fn scatter(any: &Self, uint32: &Arc<dyn Array>) -> Self;
    fn repeat(any: &Self, len: usize) -> Self;
    fn sort(any: &Self) -> Arc<dyn Array>;
}

#[derive(Debug)]
pub enum Error {
    Arrow(ArrowError),
    MultipleRows,
    Empty,
}

impl From<ArrowError> for Error {
    fn from(e: ArrowError) -> Self {
        Error::Arrow(e)
    }
}

pub fn cat<K: Kernel>(any: &Vec<K>) -> K {
    K::cat(any)
}

pub fn gather<K: Kernel>(any: &K, uint32: &Arc<dyn Array>) -> K {
    K::gather(any, uint32)
}

pub fn gather_logical<K: Kernel>(any: &K, boolean: &Arc<dyn Array>) -> K {
    K::gather_logical(any, boolean)
}

pub fn scatter<K: Kernel>(any: &K, uint32: &Arc<dyn Array>) -> K {
    K::scatter(any, uint32)
}

pub fn repeat<K: Kernel>(any: &K, len: usize) -> K {
    K::repeat(any, len)
}

pub fn sort<K: Kernel>(any: &K) -> Arc<dyn Array> {
    K::sort(any)
}
