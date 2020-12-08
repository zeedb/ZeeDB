#![feature(test)]

#[cfg(test)]
mod adventure_works;
mod byte_key;
#[cfg(test)]
mod byte_key_tests;
mod catalog;
mod eval;
mod execute;
#[cfg(test)]
mod execute_benches;
#[cfg(test)]
mod execute_tests;
mod hash_table;
#[cfg(test)]
mod hash_table_tests;
mod index;
mod join;
#[cfg(test)]
mod optimize_tests;
mod sort;
mod state;

pub use crate::execute::{execute, Program};
