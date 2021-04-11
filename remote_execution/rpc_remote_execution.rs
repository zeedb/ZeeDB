use std::{collections::HashMap, sync::Arc};

use ast::Expr;
use futures::{Stream, StreamExt};
use grpcio::{ChannelBuilder, EnvBuilder};
use kernel::{AnyArray, RecordBatch};
use protos::{
    ApproxCardinalityRequest, BroadcastRequest, ColumnStatisticsRequest, ExchangeRequest, Page,
    WorkerClient,
};
use regex::Regex;
use statistics::ColumnStatistics;

use crate::{remote_execution::RecordStream, RemoteExecution};

pub struct RpcRemoteExecution {
    workers: Vec<WorkerClient>,
}

impl Default for RpcRemoteExecution {
    fn default() -> Self {
        let re = Regex::new(r"WORKER_\d+").unwrap();
        let workers: Vec<_> = std::env::vars()
            .filter(|(key, _)| re.is_match(&key))
            .map(|(_, dst)| {
                WorkerClient::new(
                    ChannelBuilder::new(Arc::new(EnvBuilder::new().build())).connect(&dst),
                )
            })
            .collect();
        assert!(
            workers.len() > 0,
            "There are no environment variables starting with WORKER_"
        );
        Self { workers }
    }
}

impl RemoteExecution for RpcRemoteExecution {
    fn submit(&self, expr: Expr, variables: &HashMap<String, AnyArray>, txn: i64) -> RecordStream {
        Box::new(futures::stream::select_all(self.workers.iter().map(
            |worker| {
                worker
                    .broadcast(&BroadcastRequest {
                        expr: bincode::serialize(&expr).unwrap(),
                        variables: variables
                            .iter()
                            .map(|(name, value)| (name.clone(), bincode::serialize(value).unwrap()))
                            .collect(),
                        txn,
                        listeners: 1,
                    })
                    .unwrap()
                    .map(unwrap_page)
            },
        )))
    }

    fn broadcast(
        &self,
        expr: Expr,
        variables: &HashMap<String, AnyArray>,
        txn: i64,
    ) -> RecordStream {
        Box::new(futures::stream::select_all(self.workers.iter().map(
            |worker| {
                worker
                    .broadcast(&BroadcastRequest {
                        expr: bincode::serialize(&expr).unwrap(),
                        variables: variables
                            .iter()
                            .map(|(name, value)| (name.clone(), bincode::serialize(value).unwrap()))
                            .collect(),
                        txn,
                        listeners: self.workers.len() as i32,
                    })
                    .unwrap()
                    .map(unwrap_page)
            },
        )))
    }

    fn exchange(
        &self,
        expr: Expr,
        variables: &HashMap<String, AnyArray>,
        txn: i64,
        hash_column: String,
        hash_bucket: i32,
    ) -> RecordStream {
        Box::new(futures::stream::select_all(self.workers.iter().map(
            |worker| {
                worker
                    .exchange(&ExchangeRequest {
                        expr: bincode::serialize(&expr).unwrap(),
                        variables: variables
                            .iter()
                            .map(|(name, value)| (name.clone(), bincode::serialize(value).unwrap()))
                            .collect(),
                        txn,
                        listeners: self.workers.len() as i32,
                        hash_column: hash_column.clone(),
                        hash_bucket,
                    })
                    .unwrap()
                    .map(unwrap_page)
            },
        )))
    }

    fn approx_cardinality(&self, table_id: i64) -> f64 {
        self.workers
            .iter()
            .map(|worker| {
                worker
                    .approx_cardinality(&ApproxCardinalityRequest { table_id })
                    .unwrap()
                    .cardinality
            })
            .sum()
    }

    fn column_statistics(&self, table_id: i64, column_name: &str) -> Option<ColumnStatistics> {
        let mut total = None;
        for worker in &self.workers {
            if let Some(bytes) = worker
                .column_statistics(&ColumnStatisticsRequest {
                    table_id,
                    column_name: column_name.to_string(),
                })
                .unwrap()
                .statistics
            {
                let part: ColumnStatistics = bincode::deserialize(&bytes).unwrap();
                match &mut total {
                    None => total = Some(part),
                    Some(total) => total.merge(&part),
                }
            }
        }
        total
    }
}

fn unwrap_page(page: grpcio::Result<Page>) -> RecordBatch {
    bincode::deserialize(&page.unwrap().record_batch).unwrap()
}
