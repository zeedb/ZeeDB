#![feature(map_entry_replace)]
#![feature(option_expect_none)]

mod record_stream;
mod worker;
#[cfg(test)]
mod worker_tests;

pub use worker::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    todo!()
}