use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};

use ast::Value;
use kernel::RecordBatch;
use rayon::{ThreadPool, ThreadPoolBuilder};
use rpc::{
    coordinator_server::Coordinator, CheckRequest, CheckResponse, SubmitRequest, SubmitResponse,
};
use tonic::{async_trait, Request, Response, Status};

const CONCURRENT_QUERIES: usize = 10;

#[derive(Clone)]
pub struct CoordinatorNode {
    txn: Arc<AtomicI64>,
    pool: Arc<ThreadPool>,
}

impl Default for CoordinatorNode {
    fn default() -> Self {
        Self {
            txn: Default::default(),
            pool: Arc::new(
                ThreadPoolBuilder::new()
                    .num_threads(CONCURRENT_QUERIES)
                    .thread_name(|i| format!("coordinator-{}", i))
                    .build()
                    .unwrap(),
            ),
        }
    }
}

#[async_trait]
impl Coordinator for CoordinatorNode {
    async fn check(&self, _: Request<CheckRequest>) -> Result<Response<CheckResponse>, Status> {
        Ok(Response::new(CheckResponse {}))
    }

    async fn submit(
        &self,
        request: Request<SubmitRequest>,
    ) -> Result<Response<SubmitResponse>, Status> {
        let request = request.into_inner();
        let txn = request
            .txn
            .unwrap_or_else(|| self.txn.fetch_add(1, Ordering::Relaxed));
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.pool
            .spawn(move || sender.send(submit(request, txn)).unwrap());
        let record_batch = receiver.await.unwrap()?;
        Ok(Response::new(SubmitResponse {
            txn,
            record_batch: bincode::serialize(&record_batch).unwrap(),
        }))
    }
}

fn submit(request: SubmitRequest, txn: i64) -> Result<RecordBatch, Status> {
    let variables = request
        .variables
        .iter()
        .map(|(name, parameter)| (name.clone(), Value::from_proto(parameter)))
        .collect();
    let expr = match parser::analyze(&request.sql, &variables, request.catalog_id, txn) {
        Ok(expr) => expr,
        Err(message) => {
            return Err(Status::invalid_argument(message));
        }
    };
    let expr = planner::optimize(expr, request.catalog_id, txn);
    let schema = expr.schema();
    let mut stream = remote_execution::output(expr, txn);
    let mut batches = vec![];
    loop {
        match stream.next() {
            Ok(Some(record_batch)) => batches.push(record_batch),
            Ok(None) => break,
            Err(message) => return Err(Status::internal(message)),
        }
    }
    let record_batch = RecordBatch::cat(batches).unwrap_or_else(|| RecordBatch::empty(schema));
    Ok(record_batch)
}
