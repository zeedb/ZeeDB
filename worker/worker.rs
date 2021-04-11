use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
};

use ast::Expr;
use context::{Context, WORKER_COUNT_KEY, WORKER_ID_KEY};
use futures::{executor::block_on, SinkExt};
use grpcio::{RpcContext, ServerStreamingSink, UnarySink, WriteFlags};
use kernel::{AnyArray, RecordBatch};
use protos::{
    ApproxCardinalityRequest, ApproxCardinalityResponse, BroadcastRequest, ColumnStatisticsRequest,
    ColumnStatisticsResponse, ExchangeRequest, Page, Worker,
};
use remote_execution::{RemoteExecution, RpcRemoteExecution, REMOTE_EXECUTION_KEY};
use storage::{Storage, STORAGE_KEY};

#[derive(Clone)]
pub struct WorkerNode {
    context: Arc<Context>,
    broadcast: Arc<Mutex<HashMap<(Expr, i64), Broadcast>>>,
    exchange: Arc<Mutex<HashMap<(Expr, i64), Exchange>>>,
}

struct Broadcast {
    listeners: Vec<ServerStreamingSink<Page>>,
}

struct Exchange {
    listeners: Vec<(i32, ServerStreamingSink<Page>)>,
}

impl Default for WorkerNode {
    fn default() -> Self {
        Self::new(Storage::default())
    }
}

impl WorkerNode {
    pub fn new(storage: Storage) -> Self {
        let mut context = Context::default();
        context.insert(STORAGE_KEY, Mutex::new(storage));
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
        }
    }

    pub fn unwrap(self) -> Storage {
        std::mem::take(&mut self.context[STORAGE_KEY].lock().unwrap())
    }
}

impl Worker for WorkerNode {
    fn broadcast(
        &mut self,
        ctx: RpcContext,
        mut req: BroadcastRequest,
        sink: ServerStreamingSink<Page>,
    ) {
        let expr = bincode::deserialize(&req.expr).unwrap();
        let variables: HashMap<String, AnyArray> = req
            .variables
            .drain()
            .map(|(name, value)| (name, bincode::deserialize(&value).unwrap()))
            .collect();
        let listeners = req.listeners as usize;
        match self.broadcast.lock().unwrap().entry((expr, req.txn)) {
            Entry::Occupied(mut occupied) => {
                // Add a new listener to the existing list of listeners.
                occupied.get_mut().listeners.push(sink);
                // If we have reached the expected number of listeners, start the requested operation.
                if occupied.get_mut().listeners.len() == listeners {
                    let ((expr, txn), topic) = occupied.remove_entry();
                    let context = self.context.clone();
                    rayon::spawn(move || broadcast(expr, txn, variables, context, topic.listeners));
                }
            }
            Entry::Vacant(vacant) => {
                // If we only expect one listener, start the requested operation immediately.
                if listeners == 1 {
                    let (expr, txn) = vacant.into_key();
                    let context = self.context.clone();
                    rayon::spawn(move || broadcast(expr, txn, variables, context, vec![sink]));
                // Otherwise, create a new topic with one listener.
                } else {
                    vacant.insert(Broadcast {
                        listeners: vec![sink],
                    });
                }
            }
        };
    }

    fn exchange(
        &mut self,
        ctx: RpcContext,
        mut req: ExchangeRequest,
        sink: ServerStreamingSink<Page>,
    ) {
        let expr = bincode::deserialize(&req.expr).unwrap();
        let variables: HashMap<String, AnyArray> = req
            .variables
            .drain()
            .map(|(name, value)| (name, bincode::deserialize(&value).unwrap()))
            .collect();
        let listeners = req.listeners as usize;
        match self.exchange.lock().unwrap().entry((expr, req.txn)) {
            Entry::Occupied(mut occupied) => {
                // Add a new listener to the existing list of listeners.
                occupied.get_mut().listeners.push((req.hash_bucket, sink));
                // If we have reached the expected number of listeners, start the requested operation.
                if occupied.get_mut().listeners.len() == listeners {
                    let ((expr, txn), topic) = occupied.remove_entry();
                    let context = self.context.clone();
                    rayon::spawn(move || {
                        exchange(
                            expr,
                            txn,
                            variables,
                            context,
                            req.hash_column,
                            topic.listeners,
                        )
                    });
                }
            }
            Entry::Vacant(vacant) => {
                // If we only expect one listener, start the requested operation immediately.
                if listeners == 1 {
                    let (expr, txn) = vacant.into_key();
                    let context = self.context.clone();
                    rayon::spawn(move || {
                        exchange(
                            expr,
                            txn,
                            variables,
                            context,
                            req.hash_column,
                            vec![(req.hash_bucket, sink)],
                        )
                    });
                // Otherwise, create a new topic with one listener.
                } else {
                    vacant.insert(Exchange {
                        listeners: vec![(req.hash_bucket, sink)],
                    });
                }
            }
        };
    }

    fn approx_cardinality(
        &mut self,
        ctx: RpcContext,
        req: ApproxCardinalityRequest,
        sink: grpcio::UnarySink<ApproxCardinalityResponse>,
    ) {
        let cardinality = self.context[STORAGE_KEY]
            .lock()
            .unwrap()
            .statistics(req.table_id)
            .unwrap()
            .approx_cardinality() as f64;
        ctx.spawn(async move {
            sink.success(ApproxCardinalityResponse { cardinality })
                .await
                .unwrap()
        })
    }

    fn column_statistics(
        &mut self,
        ctx: RpcContext,
        req: ColumnStatisticsRequest,
        sink: UnarySink<ColumnStatisticsResponse>,
    ) {
        let lock = self.context[STORAGE_KEY].lock().unwrap();
        let statistics = lock
            .statistics(req.table_id)
            .unwrap()
            .column(&req.column_name)
            .map(|s| bincode::serialize(s).unwrap());
        ctx.spawn(async move {
            sink.success(ColumnStatisticsResponse { statistics })
                .await
                .unwrap()
        })
    }
}

fn broadcast(
    expr: Expr,
    txn: i64,
    variables: HashMap<String, AnyArray>,
    context: Arc<Context>,
    mut listeners: Vec<ServerStreamingSink<Page>>,
) {
    // Send each batch of records produced by expr to each worker node in the cluster.
    for batch in execute::execute(expr, txn, &variables, &context) {
        for sink in &mut listeners {
            block_on(sink.send((
                Page {
                    record_batch: bincode::serialize(&batch).unwrap(),
                },
                WriteFlags::default(),
            )))
            .unwrap();
        }
    }
    // Close the stream to each worker node in the cluster.
    for sink in &mut listeners {
        block_on(sink.close()).unwrap();
    }
}

fn exchange(
    expr: Expr,
    txn: i64,
    variables: HashMap<String, AnyArray>,
    context: Arc<Context>,
    hash_column: String,
    mut listeners: Vec<(i32, ServerStreamingSink<Page>)>,
) {
    // Order listeners by bucket.
    listeners.sort_by_key(|(hash_bucket, _)| *hash_bucket);
    // Split up each batch of records produced by expr and send the splits to the worker nodes.
    for batch in execute::execute(expr, txn, &variables, &context) {
        for (hash_bucket, batch_part) in partition(batch, &hash_column, listeners.len())
            .iter()
            .enumerate()
        {
            let (_, sink) = &mut listeners[hash_bucket];
            block_on(sink.send((
                Page {
                    record_batch: bincode::serialize(&batch_part).unwrap(),
                },
                WriteFlags::default(),
            )))
            .unwrap();
        }
    }
    // Close the stream to each worker node in the cluster.
    for (_, sink) in &mut listeners {
        block_on(sink.close()).unwrap();
    }
}

fn partition(batch: RecordBatch, hash_column: &str, workers: usize) -> Vec<RecordBatch> {
    if workers == 1 {
        vec![batch]
    } else {
        todo!()
    }
}

fn env_var(key: &str) -> i32 {
    std::env::var(key).expect(key).parse().unwrap()
}
