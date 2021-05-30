use std::sync::Mutex;

use coordinator::CoordinatorNode;
use fs::File;
use once_cell::sync::Lazy;
use protos::{
    coordinator_client::CoordinatorClient, coordinator_server::CoordinatorServer,
    worker_server::WorkerServer, SubmitRequest,
};
use std::{
    fs,
    io::{Read, Write},
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
        static NEXT_PORT: Lazy<Mutex<u16>> = Lazy::new(|| Mutex::new(50054));
        let port = {
            let mut lock = NEXT_PORT.lock().unwrap();
            *lock = *lock + 1;
            *lock - 1
        };
        // Set configuration environment variables that will be picked up by various services in Context.
        std::env::set_var("WORKER_0", format!("http://127.0.0.1:{}", port).as_str());
        std::env::set_var("WORKER_ID", "0");
        std::env::set_var("WORKER_COUNT", "1");
        // Create an empty 1-worker cluster.
        let worker = WorkerNode::default();
        let coordinator = CoordinatorNode::default();
        // Connect to the cluster.
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let client = protos::runtime().block_on(async move {
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
            CoordinatorClient::new(
                Endpoint::new(format!("http://127.0.0.1:{}", port))
                    .unwrap()
                    .connect_lazy()
                    .unwrap(),
            )
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
                self.run(line.strip_prefix("<").unwrap());
            } else if line.starts_with(">") {
                found.push_str(line);
                found.push('\n');
                found.push_str(&self.run(line.strip_prefix(">").unwrap()));
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

    pub fn run(&mut self, sql: &str) -> String {
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
                    Some(page) => match page.result.unwrap() {
                        protos::page::Result::RecordBatch(bytes) => {
                            batches.push(bincode::deserialize(&bytes).unwrap())
                        }
                        protos::page::Result::Error(message) => {
                            return format!("ERROR: {}", message)
                        }
                    },
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

impl Drop for TestRunner {
    fn drop(&mut self) {
        std::mem::take(&mut self.shutdown)
            .unwrap()
            .send(())
            .unwrap();
    }
}
