use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use ast::Expr;
use context::Context;
use kernel::{AnyArray, RecordBatch};
use protos::{worker_server::Worker, BroadcastRequest, ExchangeRequest, RecordStream};
use rayon::{ThreadPool, ThreadPoolBuilder};
use remote_execution::{RemoteExecution, REMOTE_EXECUTION_KEY};
use storage::{Storage, STORAGE_KEY};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex,
};
use tonic::{Request, Response, Status};

pub struct WorkerNode {
    threads: ThreadPool,
    context: Arc<Context>,
    broadcast: Mutex<HashMap<(Expr, i64), Broadcast>>,
    exchange: Mutex<HashMap<(Expr, i64), Exchange>>,
}

struct Broadcast {
    listeners: Vec<Sender<RecordBatch>>,
}

struct Exchange {
    listeners: Vec<(i32, Sender<RecordBatch>)>,
}

impl WorkerNode {
    fn execute(
        &self,
        expr: Expr,
        txn: i64,
        variables: &HashMap<String, AnyArray>,
    ) -> Receiver<RecordBatch> {
        let (sender, receiver) = channel(1);
        let context = self.context.clone();
        let variables = variables.clone();
        self.threads.spawn(move || {
            let running = execute::execute(expr, txn, &variables, &context);
            for batch in running {
                sender.blocking_send(batch).unwrap();
            }
        });
        receiver
    }
}

impl Default for WorkerNode {
    fn default() -> Self {
        let mut context = Context::default();
        context.insert(STORAGE_KEY, std::sync::Mutex::new(Storage::default()));
        context.insert(REMOTE_EXECUTION_KEY, RemoteExecution::default());
        Self {
            threads: ThreadPoolBuilder::new().build().unwrap(),
            context: Arc::new(context),
            broadcast: Default::default(),
            exchange: Default::default(),
        }
    }
}

#[tonic::async_trait]
impl Worker for WorkerNode {
    type BroadcastStream = RecordStream;

    async fn broadcast(
        &self,
        request: Request<BroadcastRequest>,
    ) -> Result<Response<Self::BroadcastStream>, Status> {
        let mut request = request.into_inner();
        let expr = bincode::deserialize(&request.expr).unwrap();
        let variables: HashMap<String, AnyArray> = request
            .variables
            .drain()
            .map(|(name, value)| (name, bincode::deserialize(&value).unwrap()))
            .collect();
        let listeners = request.listeners as usize;
        let receiver = match self.broadcast.lock().await.entry((expr, request.txn)) {
            Entry::Occupied(mut occupied) => {
                // Add a new listener to the existing list of listeners.
                let (sender, receiver) = channel(1);
                occupied.get_mut().listeners.push(sender);
                // If we have reached the expected number of listeners, start the requested operation.
                if occupied.get_mut().listeners.len() == listeners {
                    let ((expr, txn), topic) = occupied.remove_entry();
                    broadcast(self.execute(expr, txn, &variables), topic.listeners);
                }
                receiver
            }
            Entry::Vacant(vacant) => {
                let (sender, receiver) = channel(1);
                // If we only expect one listener, start the requested operation immediately.
                if listeners == 1 {
                    let (expr, txn) = vacant.into_key();
                    broadcast(self.execute(expr, txn, &variables), vec![sender]);
                // Otherwise, create a new topic with one listener.
                } else {
                    vacant.insert(Broadcast {
                        listeners: vec![sender],
                    });
                }
                receiver
            }
        };
        Ok(Response::new(RecordStream::new(receiver)))
    }

    type ExchangeStream = RecordStream;

    async fn exchange(
        &self,
        request: Request<ExchangeRequest>,
    ) -> Result<Response<Self::ExchangeStream>, Status> {
        let request = request.into_inner();
        let expr = bincode::deserialize(&request.expr).unwrap();
        let variables = HashMap::new(); // TODO
        let listeners = request.listeners as usize;
        let receiver = match self.exchange.lock().await.entry((expr, request.txn)) {
            Entry::Occupied(mut occupied) => {
                // Add a new listener to the existing list of listeners.
                let (sender, receiver) = channel(1);
                occupied
                    .get_mut()
                    .listeners
                    .push((request.hash_bucket, sender));
                // If we have reached the expected number of listeners, start the requested operation.
                if occupied.get_mut().listeners.len() == listeners {
                    let ((expr, txn), topic) = occupied.remove_entry();
                    exchange(
                        self.execute(expr, txn, &variables),
                        request.hash_column,
                        topic.listeners,
                    );
                }
                receiver
            }
            Entry::Vacant(vacant) => {
                let (sender, receiver) = channel(1);
                // If we only expect one listener, start the requested operation immediately.
                if listeners == 1 {
                    let (expr, txn) = vacant.into_key();
                    exchange(
                        self.execute(expr, txn, &variables),
                        request.hash_column,
                        vec![(request.hash_bucket, sender)],
                    );
                // Otherwise, create a new topic with one listener.
                } else {
                    vacant.insert(Exchange {
                        listeners: vec![(request.hash_bucket, sender)],
                    });
                }
                receiver
            }
        };
        Ok(Response::new(RecordStream::new(receiver)))
    }
}

fn broadcast(mut results: Receiver<RecordBatch>, listeners: Vec<Sender<RecordBatch>>) {
    tokio::spawn(async move {
        loop {
            match results.recv().await {
                Some(next) => {
                    for sender in &listeners {
                        sender.send(next.clone()).await.unwrap();
                    }
                }
                None => break,
            }
        }
    });
}

fn exchange(
    _results: Receiver<RecordBatch>,
    _hash_column: String,
    _listeners: Vec<(i32, Sender<RecordBatch>)>,
) {
    todo!()
}
