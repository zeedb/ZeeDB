use ast::Expr;
use context::ContextKey;
use futures::{Stream, StreamExt};
use kernel::{Exception, RecordBatch};
use rpc::TraceEvent;
use statistics::ColumnStatistics;

pub const REMOTE_EXECUTION_KEY: ContextKey<Box<dyn RemoteExecution>> =
    ContextKey::new("REMOTE_EXECUTION");

pub struct RecordStream {
    inner: Box<dyn Stream<Item = Result<RecordBatch, Exception>> + Send + Unpin>,
}

impl RecordStream {
    pub fn new(
        inner: Box<dyn Stream<Item = Result<RecordBatch, Exception>> + Send + Unpin>,
    ) -> Self {
        Self { inner }
    }
}

impl Iterator for RecordStream {
    type Item = Result<RecordBatch, Exception>;

    fn next(&mut self) -> Option<Self::Item> {
        rpc::runtime().block_on(self.inner.next())
    }
}

pub trait RemoteExecution: Send + Sync {
    fn submit(&self, expr: Expr, txn: i64) -> RecordStream;

    fn trace(&self, events: Vec<TraceEvent>, txn: i64, stage: i32, worker: i32);

    fn broadcast(&self, expr: Expr, txn: i64, stage: i32) -> RecordStream;

    fn exchange(
        &self,
        expr: Expr,
        txn: i64,
        stage: i32,
        hash_column: String,
        hash_bucket: i32,
    ) -> RecordStream;

    fn approx_cardinality(&self, table_id: i64) -> f64;

    fn column_statistics(&self, table_id: i64, column_name: &str) -> Option<ColumnStatistics>;
}
