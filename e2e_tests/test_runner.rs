use std::{
    fs,
    io::{Read, Write},
    net::TcpListener,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use ast::Value;
use catalog::{RESERVED_IDS, ROOT_CATALOG_ID};
use coordinator::CoordinatorNode;
use fs::File;
use kernel::RecordBatch;
use once_cell::sync::Lazy;
use rpc::{
    coordinator_client::CoordinatorClient, coordinator_server::CoordinatorServer,
    worker_server::WorkerServer, CheckRequest, SubmitRequest,
};
use tonic::transport::{Channel, Endpoint, Server};
use worker::WorkerNode;

pub struct TestRunner {
    client: Arc<Mutex<CoordinatorClient<Channel>>>,
    catalog_id: i64,
}

impl Default for TestRunner {
    fn default() -> Self {
        // Take a global lock, so we only initialize 1 cluster at a time.
        static CREATE_CLUSTER: Lazy<Arc<Mutex<CoordinatorClient<Channel>>>> =
            Lazy::new(|| Arc::new(Mutex::new(create_cluster())));
        let client = CREATE_CLUSTER.clone();
        // Find a free database.
        static NEXT_CATALOG: Lazy<AtomicI64> = Lazy::new(|| AtomicI64::new(RESERVED_IDS));
        let catalog_id = NEXT_CATALOG.fetch_add(1, Ordering::Relaxed);
        // Switch to the free database.
        let mut runner = Self {
            client,
            catalog_id: ROOT_CATALOG_ID,
        };
        runner.run(&format!("create database test{}", catalog_id), vec![]);
        // TODO this is an evil trick that just happens to match the catalog id we just created.
        // We should query this value from the metadata schema.
        runner.catalog_id = catalog_id;

        runner
    }
}

impl TestRunner {
    pub fn rewrite(&mut self, path: &str) -> bool {
        let mut file = File::open(path).unwrap();
        let mut expect = String::new();
        file.read_to_string(&mut expect).unwrap();
        let mut found = String::new();
        for line in expect.lines() {
            if line.starts_with("<") {
                found.push_str(line);
                found.push('\n');
                self.run(line.strip_prefix("<").unwrap(), vec![]);
            } else if line.starts_with(">") {
                found.push_str(line);
                found.push('\n');
                found.push_str(&self.run(line.strip_prefix(">").unwrap(), vec![]));
                found.push('\n');
                found.push('\n');
            } else if line.starts_with("#") {
                found.push_str(line);
                found.push('\n');
            }
        }
        if expect != found {
            if std::env::var("REWRITE") == Ok("1".to_string()) {
                File::create(path)
                    .unwrap()
                    .write_all(found.as_bytes())
                    .unwrap();
                false
            } else {
                println!(
                    "\x1b[0;31mSet environment variables REWRITE=1 to rewrite {}\x1b[0m",
                    path
                );
                true
            }
        } else {
            false
        }
    }

    pub fn run(&mut self, sql: &str, variables: Vec<(String, Value)>) -> String {
        let request = SubmitRequest {
            sql: sql.to_string(),
            variables: variables
                .iter()
                .map(|(k, v)| (k.clone(), v.into_proto()))
                .collect(),
            catalog_id: self.catalog_id,
            txn: None,
        };
        match rpc::runtime().block_on(self.client.lock().unwrap().submit(request)) {
            Ok(response) => {
                let batch: RecordBatch =
                    bincode::deserialize(&response.into_inner().record_batch).unwrap();
                if batch.len() == 0 {
                    "EMPTY".to_string()
                } else {
                    kernel::fixed_width(&vec![batch])
                }
            }
            Err(status) => format!("ERROR: {}", status.message()),
        }
    }
}

fn create_cluster() -> CoordinatorClient<Channel> {
    // Find a free port.
    let port = free_port();
    // Set configuration environment variables that will be picked up by various services.
    std::env::set_var("COORDINATOR", format!("http://127.0.0.1:{}", port).as_str());
    std::env::set_var("WORKER_0", format!("http://127.0.0.1:{}", port).as_str());
    std::env::set_var("WORKER_ID", "0");
    std::env::set_var("WORKER_COUNT", "1");
    // Create an empty 1-worker cluster.
    let worker = WorkerNode::default();
    let coordinator = CoordinatorNode::default();
    // Connect to the cluster.
    rpc::runtime().block_on(async move {
        let addr = format!("127.0.0.1:{}", port).parse().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(CoordinatorServer::new(coordinator))
                .add_service(WorkerServer::new(worker))
                .serve(addr)
                .await
                .unwrap()
        });
        let mut client = CoordinatorClient::new(
            Endpoint::new(format!("http://127.0.0.1:{}", port))
                .unwrap()
                .connect_lazy()
                .unwrap(),
        );
        // Check that coordinator is running.
        for _ in 0..10usize {
            match client.check(CheckRequest {}).await {
                Ok(_) => return client,
                Err(_) => std::thread::sleep(Duration::from_millis(1)),
            }
        }
        panic!("Coordinator failed to start on port {}", port)
    })
}

fn free_port() -> u16 {
    const MIN: u16 = 50100;
    const MAX: u16 = 51100;
    for port in MIN..MAX {
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!("Could not find a free port between {} and {}", MIN, MAX)
}
