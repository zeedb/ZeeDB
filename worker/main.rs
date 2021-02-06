#![feature(map_entry_replace)]
#![feature(option_expect_none)]

mod worker;
#[cfg(test)]
mod worker_tests;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    todo!()
}
