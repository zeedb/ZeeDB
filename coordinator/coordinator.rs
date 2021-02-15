use std::{
    collections::HashMap,
    sync::atomic::{AtomicI64, Ordering},
};

use catalog::CATALOG_KEY;
use context::Context;
use execute::MetadataCatalog;
use kernel::AnyArray;
use parser::{Parser, PARSER_KEY};
use protos::{coordinator_server::Coordinator, RecordStream, SubmitRequest};
use rayon::{ThreadPool, ThreadPoolBuilder};
use remote_execution::{RpcRemoteExecution, REMOTE_EXECUTION_KEY};
use statistics::{Statistics, STATISTICS_KEY};
use tokio::sync::mpsc::channel;
use tonic::{Request, Response, Status};

pub struct CoordinatorNode {
    threads: ThreadPool,
    context: Context,
    txn: AtomicI64,
}

impl Default for CoordinatorNode {
    fn default() -> Self {
        let mut context = Context::default();
        context.insert(STATISTICS_KEY, std::sync::Mutex::new(Statistics::default()));
        context.insert(PARSER_KEY, Parser::default());
        context.insert(CATALOG_KEY, Box::new(MetadataCatalog));
        Self {
            threads: ThreadPoolBuilder::new().build().unwrap(),
            context,
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
        self.threads.install(|| {
            let types = variables
                .iter()
                .map(|(name, value)| (name.clone(), value.data_type()))
                .collect();
            let expr = self.context[PARSER_KEY].analyze(
                &request.sql,
                catalog::ROOT_CATALOG_ID,
                txn,
                types,
                &self.context,
            );
            let expr = planner::optimize(expr, txn, &self.context);
            let execute = execute::execute(expr, txn, &variables, &self.context);
            for batch in execute {
                sender.blocking_send(batch).unwrap();
            }
        });
        Ok(Response::new(RecordStream::new(receiver)))
    }
}
