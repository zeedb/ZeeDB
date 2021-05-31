use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

use catalog::MetadataCatalog;
use catalog_types::{CATALOG_KEY, ROOT_CATALOG_ID};
use context::{env_var, Context, WORKER_COUNT_KEY};
use kernel::{AnyArray, Exception};
use parser::{Parser, PARSER_KEY};
use rayon::{ThreadPool, ThreadPoolBuilder};
use remote_execution::{RpcRemoteExecution, REMOTE_EXECUTION_KEY};
use rpc::{
    coordinator_server::Coordinator, CheckRequest, CheckResponse, Page, PageStream, SubmitRequest,
};
use tonic::{async_trait, Request, Response, Status};

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
    type SubmitStream = PageStream;

    async fn check(&self, _: Request<CheckRequest>) -> Result<Response<CheckResponse>, Status> {
        Ok(Response::new(CheckResponse {}))
    }

    async fn submit(
        &self,
        request: Request<SubmitRequest>,
    ) -> Result<Response<Self::SubmitStream>, Status> {
        let mut request = request.into_inner();
        let context = self.context.clone();
        let variables: HashMap<String, AnyArray> = request
            .variables
            .drain()
            .map(|(name, value)| (name, bincode::deserialize(&value).unwrap()))
            .collect();
        let txn = self.txn.fetch_add(1, Ordering::Relaxed);
        let types = variables
            .iter()
            .map(|(name, value)| (name.clone(), value.data_type()))
            .collect();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        self.pool.spawn(move || {
            let expr = match context[PARSER_KEY].analyze(
                &request.sql,
                ROOT_CATALOG_ID,
                txn,
                types,
                &context,
            ) {
                Ok(expr) => expr,
                Err(message) => {
                    sender
                        .blocking_send(Page {
                            result: Some(rpc::page::Result::Error(message)),
                        })
                        .unwrap();
                    return;
                }
            };
            let expr = planner::optimize(expr, txn, &context);
            let mut stream = context[REMOTE_EXECUTION_KEY].submit(expr, variables, txn);
            loop {
                match stream.next() {
                    Some(Ok(record_batch)) => sender
                        .blocking_send(Page {
                            result: Some(rpc::page::Result::RecordBatch(
                                bincode::serialize(&record_batch).unwrap(),
                            )),
                        })
                        .unwrap(),
                    Some(Err(Exception::Error(message))) => sender
                        .blocking_send(Page {
                            result: Some(rpc::page::Result::Error(message)),
                        })
                        .unwrap(),
                    Some(Err(Exception::End)) | None => break,
                }
            }
        });
        Ok(Response::new(PageStream { receiver }))
    }
}
