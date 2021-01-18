#![feature(shrink_to)]

mod column_statistics;
#[cfg(test)]
mod column_statistics_tests;
mod histogram;
#[cfg(test)]
mod histogram_tests;
mod table_statistics;

pub use column_statistics::{ColumnStatistics, NotNan, TypedColumnStatistics};
pub use table_statistics::TableStatistics;
