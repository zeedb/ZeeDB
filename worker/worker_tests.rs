use std::collections::HashMap;

use ast::*;
use kernel::DataType;
use protos::{worker_client::WorkerClient, worker_server::WorkerServer, BroadcastRequest};
use tonic::{
    transport::{Endpoint, Server},
    Request,
};

use crate::WorkerNode;

#[test]
fn test_broadcast() {
    // Create an empty 1-worker cluster.
    let port = 50051;
    std::env::set_var("WORKER_0", format!("http://[::1]:{}", port));
    std::env::set_var("WORKER_ID", "0");
    std::env::set_var("WORKER_COUNT", "1");
    let worker = WorkerNode::default();
    // Connect to the cluster and run a command.
    protos::runtime().block_on(async move {
        tokio::spawn(
            Server::builder()
                .add_service(WorkerServer::new(worker))
                .serve(format!("[::1]:{}", port).parse().unwrap()),
        );
        let mut client = WorkerClient::new(
            Endpoint::new(format!("http://[::1]:{}", port))
                .unwrap()
                .connect_lazy()
                .unwrap(),
        );
        let column = Column::computed("column", &None, DataType::I64);
        let expr = Out {
            projects: vec![column.clone()],
            input: Box::new(Map {
                projects: vec![(Scalar::Literal(Value::I64(Some(1))), column.clone())],
                include_existing: false,
                input: Box::new(TableFreeScan),
            }),
        };
        let _response = client
            .broadcast(Request::new(BroadcastRequest {
                expr: bincode::serialize(&expr).unwrap(),
                variables: HashMap::new(),
                txn: 0,
                listeners: 1,
            }))
            .await
            .unwrap()
            .into_inner();
    });
}
