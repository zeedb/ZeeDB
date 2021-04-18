use std::{
    fmt::Write,
    net::TcpListener,
    sync::{Arc, Mutex},
};

use coordinator::CoordinatorNode;
use futures::{executor::block_on, StreamExt};
use grpcio::{ChannelBuilder, EnvBuilder, Server, ServerBuilder};
use once_cell::sync::{Lazy, OnceCell};
use protos::{create_coordinator, create_worker, CoordinatorClient, SubmitRequest};
use regex::Regex;
use storage::Storage;
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

    pub fn adventure_works() -> Self {
        Self {
            log: "".to_string(),
            cluster: TestCluster::adventure_works(),
        }
    }

    pub fn setup(&mut self, sql: &str) {
        run(&sql, &mut self.cluster.client);
        writeln!(&mut self.log, "setup: {}", trim(&sql)).unwrap();
    }

    pub fn comment(&mut self, comment: &str) {
        writeln!(&mut self.log, "# {}", &comment).unwrap();
    }

    pub fn ok(&mut self, sql: &str) {
        let result = run(&sql, &mut self.cluster.client);
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

fn run(sql: &str, client: &mut CoordinatorClient) -> String {
    let mut stream = client
        .submit(&SubmitRequest {
            sql: sql.to_string(),
            variables: Default::default(),
        })
        .unwrap();
    let mut batches = vec![];
    loop {
        match block_on(stream.next()) {
            Some(Ok(page)) => batches.push(bincode::deserialize(&page.record_batch).unwrap()),
            Some(Err(err)) => panic!("{}", err),
            None => break,
        }
    }
    if batches.is_empty() {
        "EMPTY".to_string()
    } else {
        kernel::fixed_width(&batches)
    }
}

pub struct TestCluster {
    pub coordinator: CoordinatorNode,
    pub worker: WorkerNode,
    pub server: Server,
    pub client: CoordinatorClient,
}

impl TestCluster {
    fn new(storage: Storage, txn: i64) -> Self {
        // Take a global lock, so we only initialize 1 cluster at a time.
        static GLOBAL: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
        let _lock = GLOBAL.lock().unwrap();
        // Find a free port.
        let port = free_port();
        // Set configuration environment variables that will be picked up by various services in Context.
        std::env::set_var("WORKER_0", format!("localhost:{}", port).as_str());
        std::env::set_var("WORKER_ID", "0");
        std::env::set_var("WORKER_COUNT", "1");
        // Create an empty 1-worker cluster.
        let worker = WorkerNode::new(storage);
        let coordinator = CoordinatorNode::new(txn);
        // Start the server.
        let mut server = ServerBuilder::new(Arc::new(EnvBuilder::new().build()))
            .bind("127.0.0.1", port)
            .register_service(create_coordinator(coordinator.clone()))
            .register_service(create_worker(worker.clone()))
            .build()
            .unwrap();
        server.start();
        // Create a client.
        let ch = ChannelBuilder::new(Arc::new(EnvBuilder::new().build()))
            .connect(format!("127.0.0.1:{}", port).as_str());
        let client = CoordinatorClient::new(ch);
        Self {
            coordinator,
            worker,
            server,
            client,
        }
    }

    pub fn empty() -> Self {
        Self::new(Storage::default(), 0)
    }

    pub fn adventure_works() -> Self {
        static CACHE: OnceCell<Storage> = OnceCell::new();
        let storage = CACHE.get_or_init(|| {
            let cluster = TestCluster::empty();
            crate::adventure_works::populate_adventure_works(1_000, cluster.client);
            cluster.worker.unwrap()
        });
        Self::new(storage.clone(), 100)
    }
}

fn free_port() -> u16 {
    (50052..65535)
        .find(|port| match TcpListener::bind(("127.0.0.1", *port)) {
            Ok(_) => true,
            Err(_) => false,
        })
        .unwrap()
}
