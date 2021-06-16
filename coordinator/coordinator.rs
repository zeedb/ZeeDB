use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, Mutex,
    },
};

use catalog::MetadataCatalog;
use catalog_types::{CATALOG_KEY, ROOT_CATALOG_ID};
use context::{env_var, Context, ContextKey, WORKER_COUNT_KEY};
use kernel::{AnyArray, Exception, RecordBatch};
use parser::{Parser, PARSER_KEY};
use rayon::{ThreadPool, ThreadPoolBuilder};
use remote_execution::{RpcRemoteExecution, REMOTE_EXECUTION_KEY};
use rpc::{
    coordinator_server::Coordinator, CheckRequest, CheckResponse, SubmitRequest, SubmitResponse,
    TraceRequest, TraceResponse,
};
use tonic::{async_trait, Request, Response, Status};

const TRACES_KEY: ContextKey<Mutex<HashMap<i64, Vec<TraceRequest>>>> = ContextKey::new("TRACES");

#[derive(Clone)]
pub struct CoordinatorNode {
    context: Arc<Context>,
    txn: Arc<AtomicI64>,
    pool: Arc<ThreadPool>,
}

impl Default for CoordinatorNode {
    fn default() -> Self {
        let mut context = Context::default();
        context.insert(PARSER_KEY, Parser::default());
        context.insert(CATALOG_KEY, Box::new(MetadataCatalog));
        context.insert(
            REMOTE_EXECUTION_KEY,
            Box::new(RpcRemoteExecution::default()),
        );
        context.insert(WORKER_COUNT_KEY, env_var("WORKER_COUNT"));
        context.insert(TRACES_KEY, Mutex::new(HashMap::default()));
        Self {
            context: Arc::new(context),
            txn: Arc::new(AtomicI64::default()),
            pool: Arc::new(
                ThreadPoolBuilder::new()
                    .num_threads(context::CONCURRENT_QUERIES)
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
        let txn = self.txn.fetch_add(1, Ordering::Relaxed);
        let context = self.context.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.pool
            .spawn(move || sender.send(submit(request, txn, &context)).unwrap());
        let response = receiver.await.unwrap()?;
        Ok(Response::new(response))
    }

    async fn trace(
        &self,
        request: Request<TraceRequest>,
    ) -> Result<Response<TraceResponse>, Status> {
        let request = request.into_inner();
        self.context[TRACES_KEY]
            .lock()
            .unwrap()
            .entry(request.txn)
            .or_default()
            .push(request);
        Ok(Response::new(TraceResponse {}))
    }
}

fn submit(
    mut request: SubmitRequest,
    txn: i64,
    context: &Context,
) -> Result<SubmitResponse, Status> {
    let variables: HashMap<String, AnyArray> = request
        .variables
        .drain()
        .map(|(name, value)| (name, bincode::deserialize(&value).unwrap()))
        .collect();
    let types = variables
        .iter()
        .map(|(name, value)| (name.clone(), value.data_type()))
        .collect();
    let expr = match context[PARSER_KEY].analyze(&request.sql, ROOT_CATALOG_ID, txn, types, context)
    {
        Ok(expr) => expr,
        Err(message) => {
            return Err(Status::invalid_argument(message));
        }
    };
    let expr = planner::optimize(expr, txn, context);
    let schema = expr.schema();
    let mut stream = context[REMOTE_EXECUTION_KEY].submit(expr, variables, txn);
    let mut batches = vec![];
    loop {
        match stream.next() {
            Some(Ok(record_batch)) => batches.push(record_batch),
            Some(Err(Exception::Error(message))) => return Err(Status::internal(message)),
            Some(Err(Exception::End)) | None => break,
        }
    }
    let record_batch = RecordBatch::cat(batches).unwrap_or_else(|| RecordBatch::empty(schema));
    let trace = context[TRACES_KEY]
        .lock()
        .unwrap()
        .remove(&txn)
        .unwrap_or_default();
    Ok(SubmitResponse {
        txn,
        trace,
        record_batch: bincode::serialize(&record_batch).unwrap(),
    })
}
