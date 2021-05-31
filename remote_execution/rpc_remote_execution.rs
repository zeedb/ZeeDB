use std::{collections::HashMap, sync::Mutex};

use ast::Expr;
use futures::StreamExt;
use kernel::{AnyArray, Exception, RecordBatch};
use regex::Regex;
use rpc::{
    worker_client::WorkerClient, ApproxCardinalityRequest, BroadcastRequest,
    ColumnStatisticsRequest, ExchangeRequest, Page,
};
use statistics::ColumnStatistics;
use tonic::{
    transport::{Channel, Endpoint},
    Request, Status,
};

use crate::{RecordStream, RemoteExecution};

pub struct RpcRemoteExecution {
    workers: Vec<Mutex<WorkerClient<Channel>>>,
}

impl Default for RpcRemoteExecution {
    fn default() -> Self {
        let workers = rpc::runtime().block_on(async {
            let re = Regex::new(r"WORKER_\d+").unwrap();
            let workers: Vec<_> = std::env::vars()
                .filter(|(key, _)| re.is_match(&key))
                .map(|(_, dst)| {
                    Mutex::new(WorkerClient::new(
                        Endpoint::new(dst).unwrap().connect_lazy().unwrap(),
                    ))
                })
                .collect();
            assert!(
                workers.len() > 0,
                "There are no environment variables starting with WORKER_"
            );
            workers
        });
        Self { workers }
    }
}

impl RemoteExecution for RpcRemoteExecution {
    fn submit(&self, expr: Expr, variables: HashMap<String, AnyArray>, txn: i64) -> RecordStream {
        rpc::runtime().block_on(async move {
            let mut streams = vec![];
            for worker in &self.workers {
                let request = BroadcastRequest {
                    expr: bincode::serialize(&expr).unwrap(),
                    variables: variables
                        .iter()
                        .map(|(name, value)| (name.clone(), bincode::serialize(value).unwrap()))
                        .collect(),
                    txn,
                    listeners: 1,
                };
                let response = worker
                    .lock()
                    .unwrap()
                    .broadcast(Request::new(request))
                    .await
                    .unwrap()
                    .into_inner()
                    .map(unwrap_page);
                streams.push(response);
            }
            RecordStream::new(Box::new(futures::stream::select_all(streams)))
        })
    }

    fn broadcast(
        &self,
        expr: Expr,
        variables: HashMap<String, AnyArray>,
        txn: i64,
    ) -> RecordStream {
        rpc::runtime().block_on(async move {
            let mut streams = vec![];
            for worker in &self.workers {
                let request = BroadcastRequest {
                    expr: bincode::serialize(&expr).unwrap(),
                    variables: variables
                        .iter()
                        .map(|(name, value)| (name.clone(), bincode::serialize(value).unwrap()))
                        .collect(),
                    txn,
                    listeners: self.workers.len() as i32,
                };
                let response = worker
                    .lock()
                    .unwrap()
                    .broadcast(Request::new(request))
                    .await
                    .unwrap()
                    .into_inner()
                    .map(unwrap_page);
                streams.push(response);
            }
            RecordStream::new(Box::new(futures::stream::select_all(streams)))
        })
    }

    fn exchange(
        &self,
        expr: Expr,
        variables: HashMap<String, AnyArray>,
        txn: i64,
        hash_column: String,
        hash_bucket: i32,
    ) -> RecordStream {
        rpc::runtime().block_on(async move {
            let mut streams = vec![];
            for worker in &self.workers {
                let request = ExchangeRequest {
                    expr: bincode::serialize(&expr).unwrap(),
                    variables: variables
                        .iter()
                        .map(|(name, value)| (name.clone(), bincode::serialize(value).unwrap()))
                        .collect(),
                    txn,
                    listeners: self.workers.len() as i32,
                    hash_column: hash_column.clone(),
                    hash_bucket,
                };
                let response = worker
                    .lock()
                    .unwrap()
                    .exchange(Request::new(request))
                    .await
                    .unwrap()
                    .into_inner()
                    .map(unwrap_page);
                streams.push(response);
            }
            RecordStream::new(Box::new(futures::stream::select_all(streams)))
        })
    }

    fn approx_cardinality(&self, table_id: i64) -> f64 {
        rpc::runtime().block_on(async move {
            let mut total = 0.0;
            for worker in &self.workers {
                let request = ApproxCardinalityRequest { table_id };
                let response = worker
                    .lock()
                    .unwrap()
                    .approx_cardinality(Request::new(request))
                    .await
                    .unwrap()
                    .into_inner()
                    .cardinality;
                total += response
            }
            total
        })
    }

    fn column_statistics(&self, table_id: i64, column_name: &str) -> Option<ColumnStatistics> {
        rpc::runtime().block_on(async move {
            let mut total = None;
            for worker in &self.workers {
                let request = ColumnStatisticsRequest {
                    table_id,
                    column_name: column_name.to_string(),
                };
                let response = worker
                    .lock()
                    .unwrap()
                    .column_statistics(Request::new(request))
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
}

fn unwrap_page(page: Result<Page, Status>) -> Result<RecordBatch, Exception> {
    match page.unwrap().result.unwrap() {
        rpc::page::Result::RecordBatch(bytes) => {
            let record_batch = bincode::deserialize(&bytes).unwrap();
            Ok(record_batch)
        }
        rpc::page::Result::Error(error) => Err(Exception::Error(error)),
    }
}
