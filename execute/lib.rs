#![feature(assoc_char_funcs)]

mod adventure_works;
mod aggregate;
mod catalog;
mod eval;
#[cfg(test)]
mod eval_tests;
mod execute;
#[cfg(test)]
mod execute_tests;
mod hash_table;
#[cfg(test)]
mod hash_table_tests;
mod index;
mod join;
#[cfg(test)]
mod optimize_tests;
#[cfg(test)]
mod scalar_tests;
#[cfg(test)]
mod test_suite;

pub use crate::{
    adventure_works::adventure_works,
    catalog::{catalog, indexes},
    execute::{compile, Execute, Program},
};
