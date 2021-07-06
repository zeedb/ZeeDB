use rpc::{ApproxCardinalityRequest, ColumnStatisticsRequest};

use crate::ColumnStatistics;

#[log::trace]
pub fn approx_cardinality(table_id: i64) -> f64 {
    log::rpc(async move {
        let mut total = 0.0;
        for mut worker in remote_execution::workers().await {
            let request = ApproxCardinalityRequest { table_id };
            let response = worker
                .approx_cardinality(request)
                .await
                .unwrap()
                .into_inner()
                .cardinality;
            total += response
        }
        total
    })
}

#[log::trace]
pub fn column_statistics(table_id: i64, column_name: &str) -> Option<ColumnStatistics> {
    log::rpc(async move {
        let mut total = None;
        for mut worker in remote_execution::workers().await {
            let request = ColumnStatisticsRequest {
                table_id,
                column_name: column_name.to_string(),
            };
            let response = worker
                .column_statistics(request)
                .await
                .unwrap()
                .into_inner()
                .statistics;
            if let Some(bytes) = response {
                let partial: ColumnStatistics = bincode::deserialize(&bytes).unwrap();
                match &mut total {
                    None => total = Some(partial),
                    Some(total) => total.merge(&partial),
                }
            }
        }
        total
    })
}
