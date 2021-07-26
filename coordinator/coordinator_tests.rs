use std::collections::HashMap;

use catalog::ROOT_CATALOG_ID;
use rpc::{
    coordinator_client::CoordinatorClient, coordinator_server::CoordinatorServer,
    worker_server::WorkerServer, QueryRequest,
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
    let port = 50052;
    std::env::set_var("COORDINATOR", format!("http://127.0.0.1:{}", port));
    std::env::set_var("WORKER_0", format!("http://127.0.0.1:{}", port));
    std::env::set_var("WORKER_ID", "0");
    std::env::set_var("WORKER_COUNT", "1");
    let worker = WorkerNode::default();
    let coordinator = CoordinatorNode::default();
    // Connect to the cluster and run a command.
    rpc::runtime().block_on(async move {
        tokio::spawn(async move {
            Server::builder()
                .add_service(WorkerServer::new(worker))
                .add_service(CoordinatorServer::new(coordinator))
                .serve(format!("127.0.0.1:{}", port).parse().unwrap())
                .await
                .unwrap()
        });
        let mut client = CoordinatorClient::new(
            Endpoint::new(format!("http://127.0.0.1:{}", port))
                .unwrap()
                .connect_lazy()
                .unwrap(),
        );
        let _response = client
            .query(Request::new(QueryRequest {
                sql: "select 1".to_string(),
                variables: HashMap::new(),
                catalog_id: ROOT_CATALOG_ID,
                txn: None,
            }))
            .await
            .unwrap()
            .into_inner();
    });
}
