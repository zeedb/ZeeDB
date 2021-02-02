#![feature(slice_strip)]
#![feature(is_sorted)]

mod art;
#[cfg(test)]
mod art_tests;
mod byte_key;
#[cfg(test)]
mod byte_key_tests;
mod heap;
#[cfg(test)]
mod heap_tests;
mod page;
#[cfg(test)]
mod page_tests;
mod storage;

pub use crate::{
    art::Art,
    byte_key::*,
    heap::Heap,
    page::{Page, PAGE_SIZE},
    storage::{Storage, STORAGE_KEY},
};
