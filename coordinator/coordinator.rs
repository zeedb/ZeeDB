use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};

use ast::{Expr, Value};
use kernel::RecordBatch;
use rpc::{
    coordinator_server::Coordinator, CheckRequest, CheckResponse, SubmitRequest, SubmitResponse,
    TraceRequest, TraceResponse,
};
use tonic::{async_trait, Request, Response, Status};

#[derive(Clone, Default)]
pub struct CoordinatorNode {
    txn: Arc<AtomicI64>,
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
        rayon::spawn(move || sender.send(submit(request, txn)).unwrap());
        let batch = receiver.await.unwrap()?;
        Ok(Response::new(SubmitResponse {
            txn,
            record_batch: bincode::serialize(&batch).unwrap(),
        }))
    }

    async fn trace(
        &self,
        request: Request<TraceRequest>,
    ) -> Result<Response<TraceResponse>, Status> {
        let request = request.into_inner();
        let mut stages = log::trace(request.txn, None);
        for mut worker in remote_execution::workers().await {
            let mut response = worker.trace(request.clone()).await.unwrap().into_inner();
            stages.append(&mut response.stages);
        }
        Ok(Response::new(TraceResponse { stages }))
    }
}

fn submit(request: SubmitRequest, txn: i64) -> Result<RecordBatch, Status> {
    let _session = log::session(txn, 0, None);
    let _span = log::enter(&request.sql);
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
    let expr = planner::optimize(expr, txn);
    gather(expr, txn)
}

#[log::trace]
fn gather(expr: Expr, txn: i64) -> Result<RecordBatch, Status> {
    let schema = expr.schema();
    let mut stream = remote_execution::gather(expr, txn, 0);
    let mut batches = vec![];
    loop {
        match stream.next() {
            Ok(Some(batch)) => batches.push(batch),
            Ok(None) => break,
            Err(message) => return Err(Status::internal(message)),
        }
    }
    let batch = RecordBatch::cat(batches).unwrap_or_else(|| RecordBatch::empty(schema));
    Ok(batch)
}
