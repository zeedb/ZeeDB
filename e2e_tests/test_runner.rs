use std::{
    fs,
    io::{Read, Write},
    net::TcpListener,
    sync::Mutex,
    time::{Duration, Instant},
};

use ast::Value;
use coordinator::CoordinatorNode;
use fs::File;
use kernel::RecordBatch;
use once_cell::sync::Lazy;
use rpc::{
    coordinator_client::CoordinatorClient, coordinator_server::CoordinatorServer,
    worker_server::WorkerServer, CheckRequest, SubmitRequest, TraceRequest,
};
use tonic::{
    transport::{Channel, Endpoint, Server},
    Request,
};
use worker::WorkerNode;

pub struct TestRunner {
    pub client: CoordinatorClient<Channel>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Default for TestRunner {
    fn default() -> Self {
        // Take a global lock, so we only initialize 1 cluster at a time.
        static GLOBAL: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
        let _lock = GLOBAL.lock().unwrap();
        // Find a free port.
        let port = free_port();
        // Set configuration environment variables that will be picked up by various services in Context.
        std::env::set_var("COORDINATOR", format!("http://127.0.0.1:{}", port).as_str());
        std::env::set_var("WORKER_0", format!("http://127.0.0.1:{}", port).as_str());
        std::env::set_var("WORKER_ID", "0");
        std::env::set_var("WORKER_COUNT", "1");
        // Create an empty 1-worker cluster.
        let worker = WorkerNode::default();
        let coordinator = CoordinatorNode::default();
        // Connect to the cluster.
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let client = rpc::runtime().block_on(async move {
            let addr = format!("127.0.0.1:{}", port).parse().unwrap();
            let signal = async { receiver.await.unwrap() };
            tokio::spawn(async move {
                Server::builder()
                    .add_service(CoordinatorServer::new(coordinator))
                    .add_service(WorkerServer::new(worker))
                    .serve_with_shutdown(addr, signal)
                    .await
                    .unwrap()
            });
            let mut client = CoordinatorClient::new(
                Endpoint::new(format!("http://127.0.0.1:{}", port))
                    .unwrap()
                    .connect_lazy()
                    .unwrap(),
            );
            for _ in 0..10usize {
                match client.check(CheckRequest {}).await {
                    Ok(_) => return client,
                    Err(_) => std::thread::sleep(Duration::from_millis(1)),
                }
            }
            panic!("Coordinator failed to start on port {}", port)
        });
        Self {
            client,
            shutdown: Some(sender),
        }
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
        };
        match rpc::runtime().block_on(self.client.submit(Request::new(request))) {
            Ok(response) => {
                let batch: RecordBatch =
                    bincode::deserialize(&response.into_inner().record_batch).unwrap();
                kernel::fixed_width(&vec![batch])
            }
            Err(status) => format!("ERROR: {}", status.message()),
        }
    }

    pub fn benchmark(&mut self, sql: &str, variables: Vec<(String, Value)>) -> TestBenchmark {
        let request = SubmitRequest {
            sql: sql.to_string(),
            variables: variables
                .iter()
                .map(|(k, v)| (k.clone(), v.into_proto()))
                .collect(),
        };
        let start = Instant::now();
        let trace = rpc::runtime()
            .block_on(self.client.submit(Request::new(request)))
            .unwrap()
            .into_inner()
            .trace;
        let elapsed = Instant::now().duration_since(start);
        TestBenchmark { elapsed, trace }
    }
}

pub struct TestBenchmark {
    pub elapsed: Duration,
    pub trace: Vec<TraceRequest>,
}

impl Drop for TestRunner {
    fn drop(&mut self) {
        std::mem::take(&mut self.shutdown)
            .unwrap()
            .send(())
            .unwrap();
    }
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
