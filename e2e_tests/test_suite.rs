use std::{fmt::Write, sync::Mutex};

use coordinator::CoordinatorNode;
use once_cell::sync::Lazy;
use protos::{
    coordinator_client::CoordinatorClient, coordinator_server::CoordinatorServer,
    worker_server::WorkerServer, SubmitRequest,
};
use regex::Regex;
use storage::Storage;
use tonic::{
    transport::{Channel, Server},
    Request,
};
use worker::WorkerNode;

pub struct TestSuite {
    log: String,
    cluster: TestCluster,
}

impl TestSuite {
    pub fn empty() -> Self {
        Self {
            log: "".to_string(),
            cluster: TestCluster::empty(),
        }
    }

    pub fn setup(&mut self, sql: &str) {
        self.cluster.run(&sql);
        writeln!(&mut self.log, "setup: {}", trim(&sql)).unwrap();
    }

    pub fn comment(&mut self, comment: &str) {
        writeln!(&mut self.log, "# {}", &comment).unwrap();
    }

    pub fn ok(&mut self, sql: &str) {
        let result = self.cluster.run(&sql);
        writeln!(&mut self.log, "ok: {}\n{}\n", trim(&sql), result).unwrap();
    }

    pub fn finish(&self, output: &str) {
        if !test_fixtures::matches_expected(output, &self.log) {
            panic!("{}", output)
        }
    }
}

fn trim(sql: &str) -> String {
    let trim = Regex::new(r"(?m)^\s+").unwrap();
    let mut trimmed = trim.replace_all(sql, "").trim().to_string();
    if trimmed.len() > 200 {
        trimmed.truncate(197);
        trimmed.push_str("...");
    }
    trimmed
}

pub struct TestCluster {
    pub client: CoordinatorClient<Channel>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl TestCluster {
    fn new(storage: Storage, txn: i64) -> Self {
        // Take a global lock, so we only initialize 1 cluster at a time.
        static GLOBAL: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
        let _lock = GLOBAL.lock().unwrap();
        // Find a free port.
        static NEXT_PORT: Lazy<Mutex<u16>> = Lazy::new(|| Mutex::new(50052));
        let port = {
            let mut lock = NEXT_PORT.lock().unwrap();
            *lock = *lock + 1;
            *lock - 1
        };
        // Set configuration environment variables that will be picked up by various services in Context.
        std::env::set_var("WORKER_0", format!("http://[::1]:{}", port).as_str());
        std::env::set_var("WORKER_ID", "0");
        std::env::set_var("WORKER_COUNT", "1");
        // Create an empty 1-worker cluster.
        let worker = WorkerNode::new(storage);
        let coordinator = CoordinatorNode::new(txn);
        // Connect to the cluster.
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let client = protos::runtime().block_on(async move {
            let addr = format!("[::1]:{}", port).parse().unwrap();
            let signal = async { receiver.await.unwrap() };
            tokio::spawn(
                Server::builder()
                    .add_service(CoordinatorServer::new(coordinator))
                    .add_service(WorkerServer::new(worker))
                    .serve_with_shutdown(addr, signal),
            );
            CoordinatorClient::connect(format!("http://[::1]:{}", port))
                .await
                .unwrap()
        });
        Self {
            client,
            shutdown: Some(sender),
        }
    }

    pub fn empty() -> Self {
        Self::new(Storage::default(), 0)
    }

    fn run(&mut self, sql: &str) -> String {
        let request = SubmitRequest {
            sql: sql.to_string(),
            variables: Default::default(),
        };
        let response = self.client.submit(Request::new(request));
        protos::runtime().block_on(async {
            let mut stream = response.await.unwrap().into_inner();
            let mut batches = vec![];
            loop {
                match stream.message().await.unwrap() {
                    Some(page) => batches.push(bincode::deserialize(&page.record_batch).unwrap()),
                    None => break,
                }
            }
            if batches.is_empty() {
                "EMPTY".to_string()
            } else {
                kernel::fixed_width(&batches)
            }
        })
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        std::mem::take(&mut self.shutdown)
            .unwrap()
            .send(())
            .unwrap();
    }
}
