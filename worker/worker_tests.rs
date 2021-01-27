use crate::worker::*;
use kernel::{AnyArray, Array, I64Array, RecordBatch};
use protos::worker::{
    worker_client::WorkerClient, worker_server::WorkerServer, BroadcastRequest, SubmitRequest,
};
use std::error::Error;
use storage::Storage;
use tonic::{
    transport::{Channel, Endpoint, Server},
    Request,
};

#[tokio::test]
async fn test_submit() {
    tokio::spawn(server());
    let mut client = client().await.unwrap();
    let mut stream = client
        .submit(Request::new(SubmitRequest {
            sql: "select 1".to_string(),
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

#[tokio::test]
async fn test_broadcast() {
    tokio::spawn(server());
    let mut client = client().await.unwrap();
    let mut storage = Storage::new(); // TODO
    let catalog = execute::catalog(&mut storage, 100);
    let indexes = execute::indexes(&mut storage, 100);
    let sql = "select 1";
    let expr = parser::analyze(catalog::ROOT_CATALOG_ID, &catalog, sql).expect(sql);
    let expr = planner::optimize(catalog::ROOT_CATALOG_ID, &catalog, &indexes, &storage, expr);
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
