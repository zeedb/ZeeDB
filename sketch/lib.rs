#![feature(shrink_to)]

mod histogram;
#[cfg(test)]
mod histogram_tests;
mod sketch;
#[cfg(test)]
mod sketch_tests;

pub use sketch::Sketch;
