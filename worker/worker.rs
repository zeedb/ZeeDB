use ast::Expr;
use catalog::CATALOG_KEY;
use context::Context;
use execute::MetadataCatalog;
use kernel::{AnyArray, RecordBatch};
use parser::{Parser, PARSER_KEY};
use protos::{worker_server::Worker, BroadcastRequest, ExchangeRequest, RecordStream};
use statistics::{Statistics, STATISTICS_KEY};
use std::{
    collections::{hash_map::Entry, HashMap},
    thread,
};
use storage::{Storage, STORAGE_KEY};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex,
};
use tonic::{Request, Response, Status};

#[derive(Default)]
pub struct WorkerNode {
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
        variables: HashMap<String, AnyArray>,
    ) -> Receiver<RecordBatch> {
        let (sender, receiver) = channel(1);
        // TODO this should be run on a thread pool with a single writer thread with access to &mut Context, and N_CPUS-1 reader threads with access to &Context.
        thread::spawn(move || {
            // TODO use real shared context
            let mut context = Context::default();
            context.insert(STORAGE_KEY, Storage::default());
            context.insert(STATISTICS_KEY, Statistics::default());
            let running = execute::execute_mut(expr, txn, variables, &mut context);
            for batch in running {
                sender.blocking_send(batch).unwrap();
            }
        });
        receiver
    }
}

#[tonic::async_trait]
impl Worker for WorkerNode {
    type BroadcastStream = RecordStream;

    async fn broadcast(
        &self,
        request: Request<BroadcastRequest>,
    ) -> Result<Response<Self::BroadcastStream>, Status> {
        let request = request.into_inner();
        let expr = bincode::deserialize(&request.expr).unwrap();
        let variables = HashMap::new(); // TODO
        let listeners = request.listeners as usize;
        let receiver = match self.broadcast.lock().await.entry((expr, request.txn)) {
            Entry::Occupied(mut occupied) => {
                // Add a new listener to the existing list of listeners.
                let (sender, receiver) = channel(1);
                occupied.get_mut().listeners.push(sender);
                // If we have reached the expected number of listeners, start the requested operation.
                if occupied.get_mut().listeners.len() == listeners {
                    let ((expr, txn), topic) = occupied.remove_entry();
                    broadcast(self.execute(expr, txn, variables), topic.listeners);
                }
                receiver
            }
            Entry::Vacant(vacant) => {
                let (sender, receiver) = channel(1);
                // If we only expect one listener, start the requested operation immediately.
                if listeners == 1 {
                    let (expr, txn) = vacant.into_key();
                    broadcast(self.execute(expr, txn, variables), vec![sender]);
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
                        self.execute(expr, txn, variables),
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
                        self.execute(expr, txn, variables),
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
    mut results: Receiver<RecordBatch>,
    hash_column: String,
    listeners: Vec<(i32, Sender<RecordBatch>)>,
) {
    todo!()
}
