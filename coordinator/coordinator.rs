use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

use catalog::MetadataCatalog;
use catalog_types::{CATALOG_KEY, ROOT_CATALOG_ID};
use context::Context;
use futures::{executor::block_on, SinkExt, StreamExt};
use grpcio::{RpcContext, ServerStreamingSink, WriteFlags};
use kernel::AnyArray;
use parser::{Parser, PARSER_KEY};
use protos::{Coordinator, Page, SubmitRequest};
use remote_execution::{RpcRemoteExecution, REMOTE_EXECUTION_KEY};

#[derive(Clone)]
pub struct CoordinatorNode {
    context: Arc<Context>,
    txn: Arc<AtomicI64>,
}

impl CoordinatorNode {
    pub fn new(txn: i64) -> Self {
        let mut context = Context::default();
        context.insert(PARSER_KEY, Parser::default());
        context.insert(CATALOG_KEY, Box::new(MetadataCatalog));
        context.insert(
            REMOTE_EXECUTION_KEY,
            Box::new(RpcRemoteExecution::default()),
        );
        Self {
            context: Arc::new(context),
            txn: Arc::new(AtomicI64::new(txn)),
        }
    }
}

impl Default for CoordinatorNode {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Coordinator for CoordinatorNode {
    fn submit(
        &mut self,
        _ctx: RpcContext,
        mut req: SubmitRequest,
        mut sink: ServerStreamingSink<Page>,
    ) {
        let variables: HashMap<String, AnyArray> = req
            .variables
            .drain()
            .map(|(name, value)| (name, bincode::deserialize(&value).unwrap()))
            .collect();
        let txn = self.txn.fetch_add(1, Ordering::Relaxed);
        let types = variables
            .iter()
            .map(|(name, value)| (name.clone(), value.data_type()))
            .collect();
        let expr =
            self.context[PARSER_KEY].analyze(&req.sql, ROOT_CATALOG_ID, txn, types, &self.context);
        let expr = planner::optimize(expr, txn, &self.context);
        let mut stream = self.context[REMOTE_EXECUTION_KEY].submit(expr, &variables, txn);
        rayon::spawn(move || {
            // Send each batch of records produced by expr to the client.
            loop {
                match block_on(stream.next()) {
                    Some(batch) => {
                        block_on(sink.send((
                            Page {
                                record_batch: bincode::serialize(&batch).unwrap(),
                            },
                            WriteFlags::default(),
                        )))
                        .unwrap();
                    }
                    None => break,
                }
            }
            // Close the stream to the client.
            block_on(sink.close()).unwrap();
        });
    }
}
