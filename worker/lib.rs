mod worker;
#[cfg(test)]
mod worker_tests;

pub use crate::worker::WorkerNode;

pub fn main() {
    use rpc::worker_server::WorkerServer;
    use tonic::transport::Server;

    rpc::runtime().block_on(async move {
        let worker_id: usize = std::env::var("WORKER_ID")
            .expect("WORKER_ID is not set")
            .parse()
            .unwrap();
        let port: usize = std::env::var("WORKER_PORT")
            .expect("WORKER_PORT is not set")
            .parse()
            .unwrap();
        let worker = WorkerNode::default();
        let addr = format!("127.0.0.1:{}", port).parse().unwrap();
        eprintln!("Starting worker {} on {}", worker_id, addr);
        Server::builder()
            .add_service(WorkerServer::new(worker))
            .serve(addr)
            .await
            .unwrap()
    })
}
