use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
    thread,
};

use ast::Expr;
use execute::Node;
use globals::Global;
use kernel::{AnyArray, Next, RecordBatch};
use log::Session;
use rpc::{
    page::Part, worker_server::Worker, BroadcastRequest, CheckRequest, CheckResponse,
    ExchangeRequest, GatherRequest, Page, PageStream, StatisticsRequest, StatisticsResponse,
    TraceRequest, TraceResponse,
};
use storage::Storage;
use tokio::sync::mpsc::Sender;
use tonic::{async_trait, Request, Response, Status};

#[derive(Clone)]
pub struct WorkerNode {
    worker: i32,
    storage: Arc<Mutex<Storage>>,
    broadcast: Arc<Mutex<HashMap<(Expr, i64, i32), Broadcast>>>,
    exchange: Arc<Mutex<HashMap<(Expr, i64, i32), Exchange>>>,
}

struct Broadcast {
    listeners: Vec<Sender<Page>>,
}

struct Exchange {
    listeners: Vec<(i32, Sender<Page>)>,
}

impl Default for WorkerNode {
    fn default() -> Self {
        Self {
            worker: std::env::var("WORKER_ID").unwrap().parse().unwrap(),
            storage: Default::default(),
            broadcast: Default::default(),
            exchange: Default::default(),
        }
    }
}

#[async_trait]
impl Worker for WorkerNode {
    type BroadcastStream = PageStream;

    type ExchangeStream = PageStream;

    type GatherStream = PageStream;

    async fn check(&self, _: Request<CheckRequest>) -> Result<Response<CheckResponse>, Status> {
        Ok(Response::new(CheckResponse {}))
    }

    async fn gather(
        &self,
        request: Request<GatherRequest>,
    ) -> Result<Response<Self::GatherStream>, Status> {
        let request = request.into_inner();
        let expr = bincode::deserialize(&request.expr).unwrap();
        let storage = self.storage.clone();
        let worker = self.worker;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        thread::spawn(move || gather(&storage, request.txn, request.stage, worker, expr, sender));
        Ok(Response::new(PageStream { receiver }))
    }

    async fn broadcast(
        &self,
        request: Request<BroadcastRequest>,
    ) -> Result<Response<Self::BroadcastStream>, Status> {
        let request = request.into_inner();
        let expr = bincode::deserialize(&request.expr).unwrap();
        let listeners: usize = std::env::var("WORKER_COUNT").unwrap().parse().unwrap();
        let storage = self.storage.clone();
        let worker = self.worker;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        match self
            .broadcast
            .lock()
            .unwrap()
            .entry((expr, request.txn, request.stage))
        {
            Entry::Occupied(mut occupied) => {
                // Add a new listener to the existing list of listeners.
                occupied.get_mut().listeners.push(sender);
                // If we have reached the expected number of listeners, start the requested operation.
                if occupied.get_mut().listeners.len() == listeners {
                    let ((expr, _, _), topic) = occupied.remove_entry();
                    thread::spawn(move || {
                        broadcast(
                            &storage,
                            request.txn,
                            request.stage,
                            worker,
                            expr,
                            topic.listeners,
                        )
                    });
                }
            }
            Entry::Vacant(vacant) => {
                // If we only expect one listener, start the requested operation immediately.
                if listeners == 1 {
                    let (expr, _, _) = vacant.into_key();
                    thread::spawn(move || {
                        broadcast(
                            &storage,
                            request.txn,
                            request.stage,
                            worker,
                            expr,
                            vec![sender],
                        )
                    });
                // Otherwise, create a new topic with one listener.
                } else {
                    vacant.insert(Broadcast {
                        listeners: vec![sender],
                    });
                }
            }
        };
        Ok(Response::new(PageStream { receiver }))
    }

    async fn exchange(
        &self,
        request: Request<ExchangeRequest>,
    ) -> Result<Response<Self::ExchangeStream>, Status> {
        let request = request.into_inner();
        let expr = bincode::deserialize(&request.expr).unwrap();
        let listeners: usize = std::env::var("WORKER_COUNT").unwrap().parse().unwrap();
        let storage = self.storage.clone();
        let worker = self.worker;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        match self
            .exchange
            .lock()
            .unwrap()
            .entry((expr, request.txn, request.stage))
        {
            Entry::Occupied(mut occupied) => {
                // Add a new listener to the existing list of listeners.
                occupied
                    .get_mut()
                    .listeners
                    .push((request.hash_bucket, sender));
                // If we have reached the expected number of listeners, start the requested operation.
                if occupied.get_mut().listeners.len() == listeners {
                    let ((expr, _, _), topic) = occupied.remove_entry();
                    thread::spawn(move || {
                        exchange(
                            &storage,
                            request.txn,
                            request.stage,
                            worker,
                            expr,
                            request.hash_column,
                            topic.listeners,
                        )
                    });
                }
            }
            Entry::Vacant(vacant) => {
                // If we only expect one listener, start the requested operation immediately.
                if listeners == 1 {
                    let (expr, _, _) = vacant.into_key();
                    thread::spawn(move || {
                        exchange(
                            &storage,
                            request.txn,
                            request.stage,
                            worker,
                            expr,
                            request.hash_column,
                            vec![(request.hash_bucket, sender)],
                        )
                    });
                // Otherwise, create a new topic with one listener.
                } else {
                    vacant.insert(Exchange {
                        listeners: vec![(request.hash_bucket, sender)],
                    });
                }
            }
        };
        Ok(Response::new(PageStream { receiver }))
    }

    async fn statistics(
        &self,
        request: Request<StatisticsRequest>,
    ) -> Result<Response<StatisticsResponse>, Status> {
        let request = request.into_inner();
        let statistics = self.storage.lock().unwrap().statistics(request.table_id);
        Ok(Response::new(StatisticsResponse {
            table_statistics: bincode::serialize(&statistics).unwrap(),
        }))
    }

    async fn trace(
        &self,
        request: Request<TraceRequest>,
    ) -> Result<Response<TraceResponse>, Status> {
        let request = request.into_inner();
        let stages = log::trace(request.txn, Some(self.worker));
        Ok(Response::new(TraceResponse { stages }))
    }
}

fn gather(
    storage: &Mutex<Storage>,
    txn: i64,
    stage: i32,
    worker: i32,
    expr: Expr,
    listener: Sender<Page>,
) {
    let _unset = globals::WORKER.set(worker);
    let _session = log_session(txn, stage);
    // Send each batch of records produced by expr to each worker node in the cluster.
    let mut query = Node::compile(expr.clone());
    loop {
        let result = match query.next(storage, txn) {
            Next::Page(batch) => Part::RecordBatch(bincode::serialize(&batch).unwrap()),
            Next::End => break,
            Next::Error(message) => Part::Error(message),
        };
        listener.blocking_send(Page { part: Some(result) }).unwrap();
    }
}

fn broadcast(
    storage: &Mutex<Storage>,
    txn: i64,
    stage: i32,
    worker: i32,
    expr: Expr,
    listeners: Vec<Sender<Page>>,
) {
    let _unset = globals::WORKER.set(worker);
    let _session = log_session(txn, stage);
    // Send each batch of records produced by expr to each worker node in the cluster.
    let mut query = Node::compile(expr);
    loop {
        let result = match query.next(storage, txn) {
            Next::Page(batch) => Part::RecordBatch(bincode::serialize(&batch).unwrap()),
            Next::End => break,
            Next::Error(message) => Part::Error(message),
        };
        for sink in &listeners {
            sink.blocking_send(Page {
                part: Some(result.clone()),
            })
            .unwrap();
        }
    }
}

fn exchange(
    storage: &Mutex<Storage>,
    txn: i64,
    stage: i32,
    worker: i32,
    expr: Expr,
    hash_column: String,
    mut listeners: Vec<(i32, Sender<Page>)>,
) {
    let _unset = globals::WORKER.set(worker);
    let _session = log_session(txn, stage);
    // Order listeners by bucket.
    listeners.sort_by_key(|(hash_bucket, _)| *hash_bucket);
    // Split up each batch of records produced by expr and send the splits to the worker nodes.
    let mut query = Node::compile(expr);
    loop {
        match query.next(storage, txn) {
            Next::Page(batch) => {
                let batches = partition(&batch, &hash_column, listeners.len());
                for (hash_bucket, batch) in batches.iter().enumerate() {
                    let (_, sink) = &listeners[hash_bucket];
                    sink.blocking_send(Page {
                        part: Some(Part::RecordBatch(bincode::serialize(&batch).unwrap())),
                    })
                    .unwrap();
                }
            }
            Next::End => break,
            Next::Error(message) => {
                for (_, sink) in &listeners {
                    sink.blocking_send(Page {
                        part: Some(Part::Error(message.clone())),
                    })
                    .unwrap();
                }
            }
        }
    }
}

fn partition(batch: &RecordBatch, hash_column: &String, workers: usize) -> Vec<RecordBatch> {
    let (_, column) = batch
        .columns
        .iter()
        .find(|(name, _)| name == hash_column)
        .unwrap();
    let hashes = match column {
        AnyArray::I64(column) => column,
        _ => panic!("{} is not an I64Array", column.data_type()),
    };
    let buckets = hashes.hash_buckets(workers);
    let mut batches = vec![];
    for i in 0..workers {
        let mask = buckets.equal_scalar(Some(i as i32));
        batches.push(batch.compress(&mask));
    }
    batches
}

fn log_session(txn: i64, stage: i32) -> Session {
    let worker = globals::WORKER.get();
    log::session(txn, stage, Some(worker))
}
