#![feature(shrink_to)]

mod column_statistics;
#[cfg(test)]
mod column_statistics_tests;
mod histogram;
#[cfg(test)]
mod histogram_tests;
mod table_statistics;

pub use crate::{
    column_statistics::{ColumnStatistics, NotNan, TypedColumnStatistics},
    table_statistics::TableStatistics,
};
