use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
};

use ast::Expr;
use execute::Node;
use kernel::RecordBatch;
use rayon::{ThreadPool, ThreadPoolBuilder};
use rpc::{
    page::Part, worker_server::Worker, ApproxCardinalityRequest, ApproxCardinalityResponse,
    BroadcastRequest, CheckRequest, CheckResponse, ColumnStatisticsRequest,
    ColumnStatisticsResponse, ExchangeRequest, OutputRequest, Page, PageStream,
};
use storage::Storage;
use tokio::sync::mpsc::Sender;
use tonic::{async_trait, Request, Response, Status};

const CONCURRENT_QUERIES: usize = 10;

#[derive(Clone)]
pub struct WorkerNode {
    storage: Arc<Mutex<Storage>>,
    broadcast: Arc<Mutex<HashMap<(Expr, i64, i32), Broadcast>>>,
    exchange: Arc<Mutex<HashMap<(Expr, i64, i32), Exchange>>>,
    pool: Arc<ThreadPool>,
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
            storage: Default::default(),
            broadcast: Default::default(),
            exchange: Default::default(),
            pool: Arc::new(
                ThreadPoolBuilder::new()
                    .num_threads(CONCURRENT_QUERIES)
                    .thread_name(|i| format!("worker-{}", i))
                    .build()
                    .unwrap(),
            ),
        }
    }
}

#[async_trait]
impl Worker for WorkerNode {
    type BroadcastStream = PageStream;

    type ExchangeStream = PageStream;

    type OutputStream = PageStream;

    async fn check(&self, _: Request<CheckRequest>) -> Result<Response<CheckResponse>, Status> {
        Ok(Response::new(CheckResponse {}))
    }

    async fn output(
        &self,
        request: Request<OutputRequest>,
    ) -> Result<Response<Self::OutputStream>, Status> {
        let request = request.into_inner();
        let expr = bincode::deserialize(&request.expr).unwrap();
        let storage = self.storage.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        self.pool
            .spawn(move || broadcast(&storage, request.txn, expr, vec![sender]));
        Ok(Response::new(PageStream { receiver }))
    }

    async fn broadcast(
        &self,
        request: Request<BroadcastRequest>,
    ) -> Result<Response<Self::BroadcastStream>, Status> {
        let request = request.into_inner();
        let expr = bincode::deserialize(&request.expr).unwrap();
        let listeners = std::env::var("WORKER_COUNT").unwrap().parse().unwrap();
        let storage = self.storage.clone();
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
                    self.pool
                        .spawn(move || broadcast(&storage, request.txn, expr, topic.listeners));
                }
            }
            Entry::Vacant(vacant) => {
                // If we only expect one listener, start the requested operation immediately.
                if listeners == 1 {
                    let (expr, _, _) = vacant.into_key();
                    self.pool
                        .spawn(move || broadcast(&storage, request.txn, expr, vec![sender]));
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
        let listeners = std::env::var("WORKER_COUNT").unwrap().parse().unwrap();
        let storage = self.storage.clone();
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
                    self.pool.spawn(move || {
                        exchange(
                            &storage,
                            request.txn,
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
                    self.pool.spawn(move || {
                        exchange(
                            &storage,
                            request.txn,
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

    async fn approx_cardinality(
        &self,
        request: Request<ApproxCardinalityRequest>,
    ) -> Result<Response<ApproxCardinalityResponse>, Status> {
        let request = request.into_inner();
        let storage = self.storage.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.pool.spawn(move || {
            let cardinality = storage
                .lock()
                .unwrap()
                .statistics(request.table_id)
                .unwrap()
                .approx_cardinality() as f64;
            sender.send(cardinality).unwrap();
        });
        Ok(Response::new(ApproxCardinalityResponse {
            cardinality: receiver.await.unwrap(),
        }))
    }

    async fn column_statistics(
        &self,
        request: Request<ColumnStatisticsRequest>,
    ) -> Result<Response<ColumnStatisticsResponse>, Status> {
        let request = request.into_inner();
        let storage = self.storage.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.pool.spawn(move || {
            let bytes = storage
                .lock()
                .unwrap()
                .statistics(request.table_id)
                .unwrap()
                .column(&request.column_name)
                .map(|s| bincode::serialize(s).unwrap());
            sender.send(bytes).unwrap();
        });
        Ok(Response::new(ColumnStatisticsResponse {
            statistics: receiver.await.unwrap(),
        }))
    }
}

fn broadcast(storage: &Mutex<Storage>, txn: i64, expr: Expr, listeners: Vec<Sender<Page>>) {
    // Send each batch of records produced by expr to each worker node in the cluster.
    let mut query = Node::compile(expr);
    loop {
        let result = match query.next(storage, txn) {
            Ok(Some(batch)) => Part::RecordBatch(bincode::serialize(&batch).unwrap()),
            Ok(None) => break,
            Err(message) => Part::Error(message),
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
    expr: Expr,
    hash_column: String,
    mut listeners: Vec<(i32, Sender<Page>)>,
) {
    // Order listeners by bucket.
    listeners.sort_by_key(|(hash_bucket, _)| *hash_bucket);
    // Split up each batch of records produced by expr and send the splits to the worker nodes.
    let mut query = Node::compile(expr);
    loop {
        match query.next(storage, txn) {
            Ok(Some(batch)) => {
                for (hash_bucket, batch_part) in partition(batch, &hash_column, listeners.len())
                    .iter()
                    .enumerate()
                {
                    let (_, sink) = &listeners[hash_bucket];
                    sink.blocking_send(Page {
                        part: Some(Part::RecordBatch(bincode::serialize(&batch_part).unwrap())),
                    })
                    .unwrap();
                }
            }
            Ok(None) => break,
            Err(message) => {
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

fn partition(batch: RecordBatch, _hash_column: &str, workers: usize) -> Vec<RecordBatch> {
    if workers == 1 {
        vec![batch]
    } else {
        todo!()
    }
}
