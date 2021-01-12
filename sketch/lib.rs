#![feature(shrink_to)]

mod counter;
#[cfg(test)]
mod counter_tests;
mod histogram;
#[cfg(test)]
mod histogram_tests;

pub use counter::Counter;
pub use histogram::Histogram;
