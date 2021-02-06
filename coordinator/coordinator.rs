use catalog::CATALOG_KEY;
use context::Context;
use execute::MetadataCatalog;
use kernel::{AnyArray, RecordBatch};
use parser::{Parser, PARSER_KEY};
use protos::{coordinator_server::Coordinator, RecordStream, SubmitRequest};
use statistics::{Statistics, STATISTICS_KEY};
use std::{
    collections::HashMap,
    sync::atomic::{AtomicI64, Ordering},
    thread,
};
use storage::{Storage, STORAGE_KEY};
use tokio::sync::mpsc::{channel, Receiver};
use tonic::{Request, Response, Status};

#[derive(Default)]
pub struct CoordinatorNode {
    txn: AtomicI64,
}

impl CoordinatorNode {
    fn execute(
        &self,
        sql: String,
        variables: HashMap<String, AnyArray>,
        txn: i64,
    ) -> Receiver<RecordBatch> {
        let (sender, receiver) = channel(1);
        thread::spawn(move || {
            let mut context = Context::default();
            context.insert(STORAGE_KEY, Storage::default());
            context.insert(STATISTICS_KEY, Statistics::default());
            context.insert(PARSER_KEY, Parser::default());
            context.insert(CATALOG_KEY, Box::new(MetadataCatalog));
            let types = variables
                .iter()
                .map(|(name, value)| (name.clone(), value.data_type()))
                .collect();
            let expr =
                context[PARSER_KEY].analyze(&sql, catalog::ROOT_CATALOG_ID, txn, types, &context);
            let expr = planner::optimize(expr, txn, &context);
            let execute = execute::execute_mut(expr, txn, variables, &mut context);
            for batch in execute {
                sender.blocking_send(batch).unwrap();
            }
        });
        receiver
    }
}

// TODO:
// Storage layer needs to actually be shared, which probably means interior mutability.
// Stats need to be combined from all nodes. One solution is to have Insert produce stats updates as output, which coordinator node adds to a global stats map.
// Transaction numbers need to be generated by a global oracle. One solution is to use a coordinator node.
#[tonic::async_trait]
impl Coordinator for CoordinatorNode {
    type SubmitStream = RecordStream;

    async fn submit(
        &self,
        request: Request<SubmitRequest>,
    ) -> Result<Response<Self::SubmitStream>, Status> {
        let request = request.into_inner();
        let sql = request.sql;
        let variables = HashMap::new(); // TODO
        let txn = self.txn.fetch_add(1, Ordering::Relaxed);
        let receiver = self.execute(sql, variables, txn);
        Ok(Response::new(RecordStream::new(receiver)))
    }
}
