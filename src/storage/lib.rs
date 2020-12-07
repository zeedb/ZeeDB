#![feature(slice_strip)]
#![feature(is_sorted)]

mod art;
#[cfg(test)]
mod art_tests;
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
pub use heap::Heap;
pub use page::base_name;
pub use page::Page;
pub use page::PAGE_SIZE;
pub use storage::Storage;
