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
use futures::StreamExt;
use kernel::AnyArray;
use parser::{Parser, PARSER_KEY};
use protos::{coordinator_server::Coordinator, Page, RecordStream, SubmitRequest};
use remote_execution::{RpcRemoteExecution, REMOTE_EXECUTION_KEY};
use tonic::{async_trait, Request, Response, Status};

#[derive(Clone)]
pub struct CoordinatorNode {
    context: Arc<Context>,
    txn: Arc<AtomicI64>,
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
        }
    }
}

#[async_trait]
impl Coordinator for CoordinatorNode {
    type SubmitStream = RecordStream;

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
        rayon::spawn(move || {
            let expr =
                context[PARSER_KEY].analyze(&request.sql, ROOT_CATALOG_ID, txn, types, &context);
            let expr = planner::optimize(expr, txn, &context);
            let mut stream = context[REMOTE_EXECUTION_KEY].submit(expr, variables, txn);
            loop {
                match protos::runtime().block_on(stream.next()) {
                    Some(record_batch) => sender
                        .blocking_send(Page {
                            record_batch: bincode::serialize(&record_batch).unwrap(),
                        })
                        .unwrap(),
                    None => break,
                }
            }
        });
        Ok(Response::new(RecordStream { receiver }))
    }
}
