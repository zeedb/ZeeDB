mod adventure_works;
mod aggregate;
mod catalog;
mod eval;
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
mod test_suite;

pub use crate::{
    adventure_works::adventure_works,
    catalog::{catalog, indexes},
    execute::{compile, Execute, Program},
};
