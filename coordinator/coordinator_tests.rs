use std::collections::HashMap;

use protos::{
    coordinator_client::CoordinatorClient, coordinator_server::CoordinatorServer,
    worker_server::WorkerServer, SubmitRequest,
};
use tonic::{
    transport::{Endpoint, Server},
    Request,
};
use worker::WorkerNode;

use crate::CoordinatorNode;

#[test]
fn test_submit() {
    // Create an empty 1-worker cluster.
    let port = 50051;
    std::env::set_var("WORKER_0", format!("http://[::1]:{}", port));
    std::env::set_var("WORKER_ID", "0");
    std::env::set_var("WORKER_COUNT", "1");
    let worker = WorkerNode::default();
    let coordinator = CoordinatorNode::default();
    // Connect to the cluster and run a command.
    protos::runtime().block_on(async move {
        tokio::spawn(
            Server::builder()
                .add_service(WorkerServer::new(worker))
                .add_service(CoordinatorServer::new(coordinator))
                .serve(format!("[::1]:{}", port).parse().unwrap()),
        );
        let mut client = CoordinatorClient::new(
            Endpoint::new(format!("http://[::1]:{}", port))
                .unwrap()
                .connect_lazy()
                .unwrap(),
        );
        let _response = client
            .submit(Request::new(SubmitRequest {
                sql: "select 1".to_string(),
                variables: HashMap::new(),
            }))
            .await
            .unwrap()
            .into_inner();
    });
}