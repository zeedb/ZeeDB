use std::collections::HashMap;

use ast::{Expr, Value};
use futures::{
    stream::{select_all, SelectAll},
    StreamExt,
};
use kernel::{Next, RecordBatch};
use once_cell::sync::OnceCell;
use regex::Regex;
use rpc::{
    coordinator_client::CoordinatorClient, page::Part, worker_client::WorkerClient,
    BroadcastRequest, ExchangeRequest, GatherRequest, Page, QueryRequest,
};
use tonic::{transport::Channel, Streaming};

pub struct RecordStream {
    select: SelectAll<Streaming<Page>>,
}

impl RecordStream {
    fn new(streams: Vec<Streaming<Page>>) -> Self {
        Self {
            select: select_all(streams),
        }
    }

    pub fn next(&mut self) -> Next {
        match log::rpc(self.select.next()) {
            Some(Ok(page)) => Next::Page(unwrap_page(page)?),
            Some(Err(status)) => Next::Error(status.message().to_string()),
            None => Next::End,
        }
    }
}

/// Submit a query to the coordinator.
pub fn submit(
    sql: &str,
    variables: &HashMap<String, Value>,
    catalog_id: i64,
    txn: Option<i64>,
) -> RecordBatch {
    log::rpc(async move {
        let request = QueryRequest {
            sql: sql.to_string(),
            variables: variables
                .iter()
                .map(|(name, value)| (name.clone(), value.into_proto()))
                .collect(),
            catalog_id,
            txn,
        };
        let response = coordinator()
            .await
            .query(request)
            .await
            .unwrap()
            .into_inner();
        bincode::deserialize(&response.record_batch).unwrap()
    })
}

/// Execute a compiled expression on every worker and send the result to 1 listener.
pub fn gather(expr: &Expr, txn: i64, stage: i32) -> RecordStream {
    log::rpc(async move {
        let mut streams = vec![];
        let workers = workers().await;
        for mut worker in workers {
            let request = GatherRequest {
                txn,
                stage,
                expr: bincode::serialize(expr).unwrap(),
            };
            let response = worker.gather(request).await.unwrap().into_inner();
            streams.push(response);
        }
        RecordStream::new(streams)
    })
}

/// Execute a compiled expression on every worker and send the result to every worker.
pub fn broadcast(expr: &Expr, txn: i64, stage: i32) -> RecordStream {
    log::rpc(async move {
        let mut streams = vec![];
        let workers = workers().await;
        for mut worker in workers {
            let request = BroadcastRequest {
                txn,
                stage,
                expr: bincode::serialize(expr).unwrap(),
            };
            let response = worker.broadcast(request).await.unwrap().into_inner();
            streams.push(response);
        }
        RecordStream::new(streams)
    })
}

/// Execute a compiled expression on every worker and send a partition the results between workers.
pub fn exchange(
    expr: &Expr,
    txn: i64,
    stage: i32,
    hash_column: String,
    hash_bucket: i32,
) -> RecordStream {
    log::rpc(async move {
        let mut streams = vec![];
        let workers = workers().await;
        for mut worker in workers {
            let request = ExchangeRequest {
                txn,
                stage,
                expr: bincode::serialize(expr).unwrap(),
                hash_column: hash_column.clone(),
                hash_bucket,
            };
            let response = worker.exchange(request).await.unwrap().into_inner();
            streams.push(response);
        }
        RecordStream::new(streams)
    })
}

fn unwrap_page(page: Page) -> Result<RecordBatch, String> {
    match page.part.unwrap() {
        Part::RecordBatch(bytes) => {
            let batch = bincode::deserialize(&bytes).unwrap();
            Ok(batch)
        }
        Part::Error(error) => Err(error),
    }
}

pub async fn coordinator() -> CoordinatorClient<Channel> {
    static COORDINATOR: OnceCell<String> = OnceCell::new();
    let url = COORDINATOR.get_or_init(|| std::env::var("COORDINATOR").unwrap());
    CoordinatorClient::connect(url.as_str()).await.unwrap()
}

pub async fn workers() -> Vec<WorkerClient<Channel>> {
    static WORKERS: OnceCell<Vec<String>> = OnceCell::new();
    let urls = WORKERS.get_or_init(|| {
        let re = Regex::new(r"WORKER_\d+").unwrap();
        let mut workers = vec![];
        for (key, dst) in std::env::vars() {
            if re.is_match(&key) {
                workers.push(dst);
            }
        }
        assert!(
            workers.len() > 0,
            "There are no environment variables starting with WORKER_"
        );
        workers
    });
    let mut workers = vec![];
    for url in urls {
        workers.push(WorkerClient::connect(url.as_str()).await.unwrap());
    }
    workers
}
