mod coordinator;

pub use crate::coordinator::CoordinatorNode;

pub fn main() {
    use rpc::coordinator_server::CoordinatorServer;
    use tonic::transport::Server;

    rpc::runtime().block_on(async move {
        let port: usize = std::env::var("COORDINATOR_PORT")
            .expect("COORDINATOR_PORT is not set")
            .parse()
            .unwrap();
        let coordinator = CoordinatorNode::default();
        let addr = format!("127.0.0.1:{}", port).parse().unwrap();
        eprintln!("Starting coordinator on {}", addr);
        Server::builder()
            .add_service(CoordinatorServer::new(coordinator))
            .serve(addr)
            .await
            .unwrap()
    })
}
