use kernel::{AnyArray, Array, I64Array, RecordBatch};
use protos::{
    coordinator_client::CoordinatorClient, coordinator_server::CoordinatorServer, SubmitRequest,
};
use std::error::Error;
use tonic::{
    transport::{Channel, Endpoint, Server},
    Request,
};

use crate::coordinator::CoordinatorNode;

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

async fn server() {
    let addr = "[::1]:50052".parse().unwrap();
    Server::builder()
        .add_service(CoordinatorServer::new(CoordinatorNode::testing()))
        .serve(addr)
        .await
        .unwrap()
}

async fn client() -> Result<CoordinatorClient<Channel>, Box<dyn Error>> {
    let chan = Endpoint::new("http://[::1]:50052")?.connect_lazy()?;
    Ok(CoordinatorClient::new(chan))
}
