use std::{
    collections::HashMap,
    sync::{
        mpsc::{sync_channel, Receiver, SyncSender},
        Mutex,
    },
};

use ast::Expr;
use context::ContextKey;
use kernel::{AnyArray, RecordBatch};
use protos::{worker_client::WorkerClient, BroadcastRequest, ExchangeRequest, Page};
use tokio::runtime::Runtime;
use tonic::{
    transport::{Channel, Endpoint},
    Request, Streaming,
};

pub const REMOTE_EXECUTION_KEY: ContextKey<RemoteExecution> = ContextKey::new("REMOTE_EXECUTION");

pub struct RemoteExecution {
    workers: Vec<Mutex<WorkerClient<Channel>>>,
    runtime: Runtime,
}

impl Default for RemoteExecution {
    fn default() -> Self {
        let workers: Vec<_> = std::env::vars()
            .filter(|(key, _)| key.starts_with("WORKER_"))
            .map(|(_, dst)| Mutex::new(worker(dst)))
            .collect();
        assert!(
            workers.len() > 0,
            "There are no environment variables starting with WORKER_"
        );
        Self {
            workers,
            runtime: Runtime::new().unwrap(),
        }
    }
}

impl RemoteExecution {
    pub fn submit(
        &self,
        expr: Expr,
        variables: &HashMap<String, AnyArray>,
        txn: i64,
    ) -> Receiver<RecordBatch> {
        let (sender, receiver) = sync_channel(0);
        for worker in &self.workers {
            let mut worker = worker.lock().unwrap();
            let request = Request::new(BroadcastRequest {
                expr: bincode::serialize(&expr).unwrap(),
                variables: variables
                    .iter()
                    .map(|(name, value)| (name.clone(), bincode::serialize(value).unwrap()))
                    .collect(),
                txn,
                listeners: 1,
            });
            let stream = self
                .runtime
                .block_on(worker.broadcast(request))
                .unwrap()
                .into_inner();
            self.runtime.spawn(consume(stream, sender.clone()));
        }
        receiver
    }

    pub fn broadcast(
        &self,
        expr: Expr,
        variables: &HashMap<String, AnyArray>,
        txn: i64,
    ) -> Receiver<RecordBatch> {
        let (sender, receiver) = sync_channel(0);
        for worker in &self.workers {
            let mut worker = worker.lock().unwrap();
            let request = Request::new(BroadcastRequest {
                expr: bincode::serialize(&expr).unwrap(),
                variables: variables
                    .iter()
                    .map(|(name, value)| (name.clone(), bincode::serialize(value).unwrap()))
                    .collect(),
                txn,
                listeners: self.workers.len() as i32,
            });
            let stream = self
                .runtime
                .block_on(worker.broadcast(request))
                .unwrap()
                .into_inner();
            self.runtime.spawn(consume(stream, sender.clone()));
        }
        receiver
    }

    pub fn exchange(
        &self,
        expr: Expr,
        variables: &HashMap<String, AnyArray>,
        txn: i64,
        hash_column: String,
        hash_bucket: i32,
    ) -> Receiver<RecordBatch> {
        let (sender, receiver) = sync_channel(0);
        for worker in &self.workers {
            let mut worker = worker.lock().unwrap();
            let request = Request::new(ExchangeRequest {
                expr: bincode::serialize(&expr).unwrap(),
                variables: variables
                    .iter()
                    .map(|(name, value)| (name.clone(), bincode::serialize(value).unwrap()))
                    .collect(),
                txn,
                listeners: self.workers.len() as i32,
                hash_column: hash_column.clone(),
                hash_bucket,
            });
            let stream = self
                .runtime
                .block_on(worker.exchange(request))
                .unwrap()
                .into_inner();
            self.runtime.spawn(consume(stream, sender.clone()));
        }
        receiver
    }
}

async fn consume(mut stream: Streaming<Page>, sender: SyncSender<RecordBatch>) {
    match stream.message().await.unwrap() {
        Some(next) => {
            let record_batch: RecordBatch = bincode::deserialize(&next.record_batch).unwrap();
            sender.send(record_batch).unwrap();
        }
        None => return,
    }
}

fn worker(dst: String) -> WorkerClient<Channel> {
    let chan = Endpoint::new(dst).unwrap().connect_lazy().unwrap();
    WorkerClient::new(chan)
}
