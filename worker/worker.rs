use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
};

use ast::Expr;
use context::{env_var, Context, WORKER_COUNT_KEY, WORKER_ID_KEY};
use kernel::{Exception, RecordBatch};
use rayon::{ThreadPool, ThreadPoolBuilder};
use remote_execution::{RpcRemoteExecution, REMOTE_EXECUTION_KEY};
use rpc::{
    page::Part, worker_server::Worker, ApproxCardinalityRequest, ApproxCardinalityResponse,
    BroadcastRequest, CheckRequest, CheckResponse, ColumnStatisticsRequest,
    ColumnStatisticsResponse, ExchangeRequest, Page, PageStream,
};
use storage::{Storage, STORAGE_KEY};
use tokio::sync::mpsc::Sender;
use tonic::{async_trait, Request, Response, Status};

#[derive(Clone)]
pub struct WorkerNode {
    context: Arc<Context>,
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
        let mut context = Context::default();
        context.insert(STORAGE_KEY, Mutex::new(Storage::default()));
        context.insert(
            REMOTE_EXECUTION_KEY,
            Box::new(RpcRemoteExecution::default()),
        );
        context.insert(WORKER_ID_KEY, env_var("WORKER_ID"));
        context.insert(WORKER_COUNT_KEY, env_var("WORKER_COUNT"));
        Self {
            context: Arc::new(context),
            broadcast: Default::default(),
            exchange: Default::default(),
            pool: Arc::new(
                ThreadPoolBuilder::new()
                    .num_threads(context::CONCURRENT_QUERIES)
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

    async fn check(&self, _: Request<CheckRequest>) -> Result<Response<CheckResponse>, Status> {
        Ok(Response::new(CheckResponse {}))
    }

    async fn broadcast(
        &self,
        request: Request<BroadcastRequest>,
    ) -> Result<Response<Self::BroadcastStream>, Status> {
        let request = request.into_inner();
        let expr = bincode::deserialize(&request.expr).unwrap();
        let listeners = request.listeners as usize;
        let context = self.context.clone();
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
                    let ((expr, txn, stage), topic) = occupied.remove_entry();
                    self.pool
                        .spawn(move || broadcast(expr, txn, stage, context, topic.listeners));
                }
            }
            Entry::Vacant(vacant) => {
                // If we only expect one listener, start the requested operation immediately.
                if listeners == 1 {
                    let (expr, txn, stage) = vacant.into_key();
                    self.pool
                        .spawn(move || broadcast(expr, txn, stage, context, vec![sender]));
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
        let listeners = request.listeners as usize;
        let context = self.context.clone();
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
                    let ((expr, txn, stage), topic) = occupied.remove_entry();
                    self.pool.spawn(move || {
                        exchange(
                            expr,
                            txn,
                            stage,
                            context,
                            request.hash_column,
                            topic.listeners,
                        )
                    });
                }
            }
            Entry::Vacant(vacant) => {
                // If we only expect one listener, start the requested operation immediately.
                if listeners == 1 {
                    let (expr, txn, stage) = vacant.into_key();
                    self.pool.spawn(move || {
                        exchange(
                            expr,
                            txn,
                            stage,
                            context,
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
        let context = self.context.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.pool.spawn(move || {
            let cardinality = context[STORAGE_KEY]
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
        let context = self.context.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.pool.spawn(move || {
            let bytes = context[STORAGE_KEY]
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

fn broadcast(
    expr: Expr,
    txn: i64,
    stage: i32,
    context: Arc<Context>,
    listeners: Vec<Sender<Page>>,
) {
    // Send each batch of records produced by expr to each worker node in the cluster.
    let mut query = execute::execute(expr, txn, &context);
    loop {
        let result = match query.next() {
            Ok(batch) => Part::RecordBatch(bincode::serialize(&batch).unwrap()),
            Err(Exception::Error(message)) => Part::Error(message),
            Err(Exception::End) => break,
        };
        for sink in &listeners {
            sink.blocking_send(Page {
                part: Some(result.clone()),
            })
            .unwrap();
        }
    }
    // Send trace events to coordinator.
    context[REMOTE_EXECUTION_KEY].trace(query.trace_events(), txn, stage, context[WORKER_ID_KEY]);
}

fn exchange(
    expr: Expr,
    txn: i64,
    stage: i32,
    context: Arc<Context>,
    hash_column: String,
    mut listeners: Vec<(i32, Sender<Page>)>,
) {
    // Order listeners by bucket.
    listeners.sort_by_key(|(hash_bucket, _)| *hash_bucket);
    // Split up each batch of records produced by expr and send the splits to the worker nodes.
    let mut query = execute::execute(expr, txn, &context);
    loop {
        match query.next() {
            Ok(batch) => {
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
            Err(Exception::Error(message)) => {
                for (_, sink) in &listeners {
                    sink.blocking_send(Page {
                        part: Some(Part::Error(message.clone())),
                    })
                    .unwrap();
                }
            }
            Err(Exception::End) => break,
        }
    }
    // Send trace events to coordinator.
    context[REMOTE_EXECUTION_KEY].trace(query.trace_events(), txn, stage, context[WORKER_ID_KEY]);
}

fn partition(batch: RecordBatch, _hash_column: &str, workers: usize) -> Vec<RecordBatch> {
    if workers == 1 {
        vec![batch]
    } else {
        todo!()
    }
}
