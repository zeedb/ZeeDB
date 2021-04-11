#![allow(dead_code)]

mod cost;
mod optimize;
// optimize_tests are in execute, so we can use real data for testing.
mod cardinality_estimation;
mod distribution;
mod rewrite;
mod rule;
mod search_space;
mod unnest;

pub use optimize::optimize;
