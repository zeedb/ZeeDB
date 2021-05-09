use std::collections::HashMap;

use ast::Expr;
use context::ContextKey;
use futures::Stream;
use kernel::{AnyArray, RecordBatch};
use statistics::ColumnStatistics;

pub const REMOTE_EXECUTION_KEY: ContextKey<Box<dyn RemoteExecution>> =
    ContextKey::new("REMOTE_EXECUTION");

pub type RecordStream = Box<dyn Stream<Item = RecordBatch> + Send + Unpin>;

pub trait RemoteExecution: Send + Sync {
    fn submit(&self, expr: Expr, variables: HashMap<String, AnyArray>, txn: i64) -> RecordStream;

    fn broadcast(&self, expr: Expr, variables: HashMap<String, AnyArray>, txn: i64)
        -> RecordStream;

    fn exchange(
        &self,
        expr: Expr,
        variables: HashMap<String, AnyArray>,
        txn: i64,
        hash_column: String,
        hash_bucket: i32,
    ) -> RecordStream;

    fn approx_cardinality(&self, table_id: i64) -> f64;

    fn column_statistics(&self, table_id: i64, column_name: &str) -> Option<ColumnStatistics>;
}
