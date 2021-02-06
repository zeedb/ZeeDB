use crate::worker::*;
use catalog::CATALOG_KEY;
use context::Context;
use execute::MetadataCatalog;
use kernel::{AnyArray, Array, I64Array, RecordBatch};
use parser::{Parser, PARSER_KEY};
use protos::{worker_client::WorkerClient, worker_server::WorkerServer, BroadcastRequest};
use statistics::{Statistics, STATISTICS_KEY};
use std::{collections::HashMap, error::Error};
use storage::{Storage, STORAGE_KEY};
use tonic::{
    transport::{Channel, Endpoint, Server},
    Request,
};

#[tokio::test]
async fn test_broadcast() {
    tokio::spawn(server());
    let mut client = client().await.unwrap();
    let mut context = Context::default();
    context.insert(STORAGE_KEY, Storage::default());
    context.insert(STATISTICS_KEY, Statistics::default());
    context.insert(PARSER_KEY, Parser::default());
    context.insert(CATALOG_KEY, Box::new(MetadataCatalog));
    let sql = "select 1";
    let expr = context[PARSER_KEY].analyze(sql, catalog::ROOT_CATALOG_ID, 100, vec![], &context);
    let expr = planner::optimize(expr, 100, &context);
    let mut stream = client
        .broadcast(Request::new(BroadcastRequest {
            expr: bincode::serialize(&expr).unwrap(),
            txn: 100,
            listeners: 1,
        }))
        .await
        .unwrap()
        .into_inner();
    let page = stream.message().await.unwrap().unwrap();
    let actual: RecordBatch = bincode::deserialize(&page.record_batch).unwrap();
    let expected = RecordBatch::new(vec![(
        "$col1".to_string(),
        AnyArray::I64(I64Array::from_values(vec![1])),
    )]);
    assert_eq!(format!("{:?}", expected), format!("{:?}", actual));
}

async fn server() {
    let addr = "[::1]:50052".parse().unwrap();
    Server::builder()
        .add_service(WorkerServer::new(WorkerNode::default()))
        .serve(addr)
        .await
        .unwrap()
}

async fn client() -> Result<WorkerClient<Channel>, Box<dyn Error>> {
    let chan = Endpoint::new("http://[::1]:50052")?.connect_lazy()?;
    Ok(WorkerClient::new(chan))
}
