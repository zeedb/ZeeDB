use std::{collections::HashMap, sync::Mutex};

use once_cell::sync::OnceCell;
use rpc::StatisticsRequest;

use crate::{ColumnStatistics, TableStatistics};

// TODO this should actually be a cache that falls back on storing statistics in the database itself.
/// Cache of recently-used table statistics on the coordinator.
static CACHE: OnceCell<Mutex<HashMap<i64, TableStatistics>>> = OnceCell::new();

#[log::trace]
pub fn approx_cardinality(table_id: i64) -> f64 {
    CACHE
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .entry(table_id)
        .or_insert_with(|| populate_cache(table_id))
        .approx_cardinality() as f64
}

#[log::trace]
pub fn column_statistics(table_id: i64, column_name: &str) -> Option<ColumnStatistics> {
    CACHE
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .entry(table_id)
        .or_insert_with(|| populate_cache(table_id))
        .column(column_name)
        .cloned()
}

// TODO invalidate the cache when the table has been heavily updated.
#[log::trace]
fn populate_cache(table_id: i64) -> TableStatistics {
    log::rpc(async {
        let mut combined = TableStatistics::default();
        for mut worker in remote_execution::workers().await {
            let response = worker
                .statistics(StatisticsRequest { table_id })
                .await
                .unwrap()
                .into_inner();
            let part: TableStatistics = bincode::deserialize(&response.table_statistics).unwrap();
            combined.merge(part);
        }
        combined
    })
}
