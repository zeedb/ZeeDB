#![feature(slice_strip)]
#![feature(is_sorted)]

mod art;
#[cfg(test)]
mod art_tests;
mod byte_key;
#[cfg(test)]
mod byte_key_tests;
mod counter;
#[cfg(test)]
mod counter_tests;
mod heap;
#[cfg(test)]
mod heap_tests;
mod page;
#[cfg(test)]
mod page_tests;
mod storage;

pub use art::Art;
pub use byte_key::*;
pub use heap::Heap;
pub use page::{Page, PAGE_SIZE};
pub use storage::Storage;
