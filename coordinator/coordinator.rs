use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, Mutex,
    },
};

use catalog::CATALOG_KEY;
use context::Context;
use execute::{MetadataCatalog, SingleNodeRemoteExecution};
use kernel::AnyArray;
use parser::{Parser, PARSER_KEY};
use protos::{coordinator_server::Coordinator, RecordStream, SubmitRequest};
use rayon::{ThreadPool, ThreadPoolBuilder};
use remote_execution::{RpcRemoteExecution, REMOTE_EXECUTION_KEY};
use statistics::{Statistics, STATISTICS_KEY};
use storage::{Storage, STORAGE_KEY};
use tokio::sync::mpsc::channel;
use tonic::{Request, Response, Status};

pub struct CoordinatorNode {
    threads: ThreadPool,
    context: Arc<Context>,
    txn: AtomicI64,
}

impl CoordinatorNode {
    pub fn testing() -> Self {
        // Set up local, single-node worker context.
        let mut worker = Context::default();
        worker.insert(STORAGE_KEY, Mutex::new(Storage::default()));
        // Set up coordinator node that calls the local worker.
        let mut coordinator = Context::default();
        coordinator.insert(STATISTICS_KEY, std::sync::Mutex::new(Statistics::default()));
        coordinator.insert(PARSER_KEY, Parser::default());
        coordinator.insert(CATALOG_KEY, Box::new(MetadataCatalog));
        coordinator.insert(
            REMOTE_EXECUTION_KEY,
            Box::new(SingleNodeRemoteExecution::new(worker)),
        );
        Self::new(coordinator)
    }

    pub fn production() -> Self {
        let mut context = Context::default();
        context.insert(STATISTICS_KEY, std::sync::Mutex::new(Statistics::default()));
        context.insert(PARSER_KEY, Parser::default());
        context.insert(CATALOG_KEY, Box::new(MetadataCatalog));
        context.insert(
            REMOTE_EXECUTION_KEY,
            Box::new(RpcRemoteExecution::default()),
        );
        Self::new(context)
    }

    fn new(context: Context) -> Self {
        Self {
            threads: ThreadPoolBuilder::new().build().unwrap(),
            context: Arc::new(context),
            txn: Default::default(),
        }
    }
}

#[tonic::async_trait]
impl Coordinator for CoordinatorNode {
    type SubmitStream = RecordStream;

    async fn submit(
        &self,
        request: Request<SubmitRequest>,
    ) -> Result<Response<Self::SubmitStream>, Status> {
        let request = request.into_inner();
        let variables: HashMap<String, AnyArray> = HashMap::new(); // TODO
        let txn = self.txn.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = channel(1);
        let context = self.context.clone();
        self.threads.spawn(move || {
            let types = variables
                .iter()
                .map(|(name, value)| (name.clone(), value.data_type()))
                .collect();
            let expr = context[PARSER_KEY].analyze(
                &request.sql,
                catalog::ROOT_CATALOG_ID,
                txn,
                types,
                &context,
            );
            let expr = planner::optimize(expr, txn, &context);
            let execute = execute::execute(expr, txn, &variables, &context);
            for batch in execute {
                sender.blocking_send(batch).unwrap();
            }
        });
        Ok(Response::new(RecordStream::new(receiver)))
    }
}
