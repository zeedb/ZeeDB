use crate::record_stream::RecordStream;
use ast::Expr;
use kernel::RecordBatch;
use protos::worker::{worker_server::Worker, BroadcastRequest, ExchangeRequest, SubmitRequest};
use std::collections::{hash_map::Entry, HashMap};
use storage::Storage;
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex,
};
use tonic::{Request, Response, Status};

/// WorkerNode is the entry point for queries.
/// Every worker can accept queries from clients, and partial queries from other workers.
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
    fn execute(&self, expr: Expr, txn: i64) -> Receiver<RecordBatch> {
        let (sender, receiver) = channel(1);
        tokio::spawn(async move {
            let mut storage = Storage::new(); // TODO
            let program = execute::compile(expr);
            let execute = program.execute(&mut storage, txn);
            for batch in execute {
                sender.send(batch).await.unwrap();
            }
        });
        receiver
    }
}

#[tonic::async_trait]
impl Worker for WorkerNode {
    type SubmitStream = RecordStream;

    async fn submit(
        &self,
        request: Request<SubmitRequest>,
    ) -> Result<Response<Self::SubmitStream>, Status> {
        let request = request.into_inner();
        let sql = request.sql;
        let txn = 100; // TODO
        let mut storage = Storage::new(); // TODO
        let catalog = execute::catalog(&mut storage, txn);
        let indexes = execute::indexes(&mut storage, txn);
        let expr = parser::analyze(catalog::ROOT_CATALOG_ID, &catalog, &sql).expect(&sql);
        let expr = planner::optimize(catalog::ROOT_CATALOG_ID, &catalog, &indexes, &storage, expr);
        let receiver = self.execute(expr, txn);
        Ok(Response::new(RecordStream::new(receiver)))
    }

    type BroadcastStream = RecordStream;

    async fn broadcast(
        &self,
        request: Request<BroadcastRequest>,
    ) -> Result<Response<Self::BroadcastStream>, Status> {
        let request = request.into_inner();
        let expr = bincode::deserialize(&request.expr).unwrap();
        let listeners = request.listeners as usize;
        let receiver = match self.broadcast.lock().await.entry((expr, request.txn)) {
            Entry::Occupied(mut occupied) => {
                // Add a new listener to the existing list of listeners.
                let (sender, receiver) = channel(1);
                occupied.get_mut().listeners.push(sender);
                // If we have reached the expected number of listeners, start the requested operation.
                if occupied.get_mut().listeners.len() == listeners {
                    let ((expr, txn), topic) = occupied.remove_entry();
                    broadcast(self.execute(expr, txn), topic.listeners).await;
                }
                receiver
            }
            Entry::Vacant(vacant) => {
                let (sender, receiver) = channel(1);
                // If we only expect one listener, start the requested operation immediately.
                if listeners == 1 {
                    let (expr, txn) = vacant.into_key();
                    broadcast(self.execute(expr, txn), vec![sender]).await;
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
                        self.execute(expr, txn),
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
                        self.execute(expr, txn),
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

async fn broadcast(mut results: Receiver<RecordBatch>, listeners: Vec<Sender<RecordBatch>>) {
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
}

fn exchange(
    results: Receiver<RecordBatch>,
    hash_column: String,
    listeners: Vec<(i32, Sender<RecordBatch>)>,
) {
    todo!()
}
