use std::sync::Arc;

use chrono::*;
use coordinator::CoordinatorNode;
use rand::{rngs::SmallRng, Rng, SeedableRng};
use worker::WorkerNode;

// cargo install flamegraph
// sudo cargo flamegraph --dev -p benchmarks -b benchmarks -o benchmarks/profiles/generate_adventure_works.svg
fn main() {
    // Start up an in-process cluster.
    // let mut server = coordinator_server();
    // server.start();
    // let client = coordinator_client();
    // fn timestamp(secs: i64) -> DateTime<Utc> {
    //     DateTime::from_utc(NaiveDateTime::from_timestamp(secs, 0), Utc)
    // }
    // let mut lines = vec![];
    // const LOW_TIME: i64 = 946688400;
    // const HIGH_TIME: i64 = 1577840400;
    // let mut rng = SmallRng::from_seed([0; 32]);
    // for store_id in 0..10_000 {
    //     let store_name = rng.gen_range(0..1_000_000);
    //     let modified_date = timestamp(rng.gen_range(LOW_TIME..HIGH_TIME)).to_rfc3339();
    //     let line = format!(
    //         "({}, '{}', timestamp '{}')",
    //         store_id, store_name, modified_date
    //     );
    //     lines.push(line);
    // }
    // execute(
    //     "create table store (store_id int64, name string, modified_date timestamp);".to_string(),
    //     &client,
    // );
    // let insert = format!("insert into store values\n{};", lines.join(",\n"));
    // for _ in 0..10 {
    //     println!("insert 10,000 rows into store");
    //     execute(insert.clone(), &client);
    // }
}
