use std::{
    collections::HashMap,
    sync::{
        mpsc::{sync_channel, Receiver},
        Arc,
    },
};

use ast::Expr;
use context::Context;
use kernel::{AnyArray, RecordBatch};
use rayon::{ThreadPool, ThreadPoolBuilder};
use remote_execution::RemoteExecution;

pub struct SingleNodeRemoteExecution {
    threads: ThreadPool,
    context: Arc<Context>,
}

impl SingleNodeRemoteExecution {
    pub fn new(context: Context) -> Self {
        Self {
            threads: ThreadPoolBuilder::new().build().unwrap(),
            context: Arc::new(context),
        }
    }
}

impl RemoteExecution for SingleNodeRemoteExecution {
    fn submit(
        &self,
        expr: Expr,
        variables: &HashMap<String, AnyArray>,
        txn: i64,
    ) -> Receiver<RecordBatch> {
        let (sender, receiver) = sync_channel(0);
        let context = self.context.clone();
        let variables = variables.clone();
        self.threads.spawn(move || {
            for batch in crate::execute::execute(expr, txn, &variables, &context) {
                sender.send(batch).unwrap();
            }
        });
        receiver
    }

    fn broadcast(
        &self,
        expr: Expr,
        variables: &HashMap<String, AnyArray>,
        txn: i64,
    ) -> Receiver<RecordBatch> {
        self.submit(expr, variables, txn)
    }

    fn exchange(
        &self,
        expr: Expr,
        variables: &HashMap<String, AnyArray>,
        txn: i64,
        _hash_column: String,
        _hash_bucket: i32,
    ) -> Receiver<RecordBatch> {
        self.submit(expr, variables, txn)
    }
}
