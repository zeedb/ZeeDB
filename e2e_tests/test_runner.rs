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
use difference::Changeset;
use fs::File;
use kernel::RecordBatch;
use log::JsonTraceEvent;
use once_cell::sync::Lazy;
use rpc::{
    coordinator_client::CoordinatorClient, coordinator_server::CoordinatorServer,
    worker_server::WorkerServer, CheckRequest, QueryRequest, QueryResponse, TraceRequest,
};
use tonic::{
    transport::{Channel, Endpoint, Server},
    Status,
};
use worker::WorkerNode;

pub struct TestRunner {
    client: Arc<Mutex<CoordinatorClient<Channel>>>,
    catalog_id: i64,
}

impl Default for TestRunner {
    fn default() -> Self {
        // Take a global lock, so we only initialize 1 cluster at a time.
        static CREATE_CLUSTER: Lazy<Arc<Mutex<CoordinatorClient<Channel>>>> =
            Lazy::new(|| Arc::new(Mutex::new(connect_to_cluster())));
        let client = CREATE_CLUSTER.clone();
        // Find a free database.
        static NEXT_CATALOG: Lazy<AtomicI64> = Lazy::new(|| AtomicI64::new(RESERVED_IDS));
        let catalog_id = NEXT_CATALOG.fetch_add(1, Ordering::Relaxed);
        // Switch to the free database.
        let mut runner = Self {
            client,
            catalog_id: ROOT_CATALOG_ID,
        };
        runner.test(&format!("create database test{}", catalog_id), vec![]);
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
                self.test(line.strip_prefix("<").unwrap(), vec![]);
            } else if line.starts_with(">") {
                found.push_str(line);
                found.push('\n');
                found.push_str(&self.test(line.strip_prefix(">").unwrap(), vec![]));
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
                let changes = Changeset::new(&expect, &found, " ");
                println!("{}", changes);
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

    pub fn test(&mut self, sql: &str, variables: Vec<(String, Value)>) -> String {
        match self.query(sql, &variables) {
            Ok(response) => {
                let batch: RecordBatch = bincode::deserialize(&response.record_batch).unwrap();
                if batch.len() == 0 {
                    "EMPTY".to_string()
                } else {
                    kernel::fixed_width(&batch)
                }
            }
            Err(status) => format!("ERROR: {}", status.message()),
        }
    }

    pub fn bench(&mut self, sql: &str, variables: Vec<(String, Value)>) -> Vec<JsonTraceEvent> {
        let query_response = self.query(sql, &variables).unwrap();
        let trace_request = TraceRequest {
            txn: query_response.txn,
        };
        let trace_response = rpc::runtime()
            .block_on(self.client.lock().unwrap().trace(trace_request))
            .unwrap()
            .into_inner();
        log::to_json(trace_response.stages)
    }

    pub fn query(
        &mut self,
        sql: &str,
        variables: &Vec<(String, Value)>,
    ) -> Result<QueryResponse, Status> {
        let request = QueryRequest {
            sql: sql.to_string(),
            variables: variables
                .iter()
                .map(|(k, v)| (k.clone(), v.into_proto()))
                .collect(),
            catalog_id: self.catalog_id,
            txn: None,
        };
        rpc::runtime()
            .block_on(self.client.lock().unwrap().query(request))
            .map(|response| response.into_inner())
    }
}

fn connect_to_cluster() -> CoordinatorClient<Channel> {
    if !std::env::var("COORDINATOR").is_ok() {
        create_cluster()
    }
    rpc::runtime().block_on(async move {
        let coordinator = std::env::var("COORDINATOR").unwrap();
        let mut client = CoordinatorClient::new(
            Endpoint::new(coordinator.clone())
                .unwrap()
                .connect_lazy()
                .unwrap(),
        );
        // Check that coordinator is running.
        for _ in 0..10usize {
            match client.check(CheckRequest {}).await {
                Ok(_) => return client,
                Err(_) => std::thread::sleep(Duration::from_millis(100)),
            }
        }
        panic!("Failed to connect to coordinator at {}", coordinator)
    })
}

const N_WORKERS: usize = 2;

fn create_cluster() {
    let (coordinator_port, worker_ports) = find_cluster_ports();
    set_common_env_variables(coordinator_port, &worker_ports);
    rpc::runtime().block_on(async move {
        spawn_coordinator(coordinator_port);
        for worker_id in 0..N_WORKERS {
            spawn_worker(worker_id, worker_ports[worker_id]);
        }
    })
}

fn find_cluster_ports() -> (u16, Vec<u16>) {
    let coordinator_port = free_port(MIN_PORT);
    let mut worker_ports: Vec<u16> = vec![];
    let mut worker_port = coordinator_port;
    for _ in 0..N_WORKERS {
        worker_port = free_port(worker_port + 1);
        worker_ports.push(worker_port);
    }
    (coordinator_port, worker_ports)
}

fn set_common_env_variables(coordinator_port: u16, worker_ports: &Vec<u16>) {
    std::env::set_var("WORKER_COUNT", N_WORKERS.to_string());
    std::env::set_var(
        "COORDINATOR",
        format!("http://127.0.0.1:{}", coordinator_port).as_str(),
    );
    for worker_id in 0..N_WORKERS {
        std::env::set_var(
            format!("WORKER_{}", worker_id),
            format!("http://127.0.0.1:{}", worker_ports[worker_id]).as_str(),
        );
    }
}

fn spawn_coordinator(coordinator_port: u16) {
    std::env::set_var("COORDINATOR_PORT", coordinator_port.to_string());
    let coordinator = CoordinatorNode::default();
    let addr = format!("127.0.0.1:{}", coordinator_port).parse().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(CoordinatorServer::new(coordinator))
            .serve(addr)
            .await
            .unwrap()
    });
}

fn spawn_worker(worker_id: usize, worker_port: u16) {
    std::env::set_var("WORKER_ID", worker_id.to_string());
    std::env::set_var("WORKER_PORT", worker_port.to_string());
    let worker = WorkerNode::default();
    let addr = format!("127.0.0.1:{}", worker_port).parse().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerServer::new(worker))
            .serve(addr)
            .await
            .unwrap()
    });
}

const MIN_PORT: u16 = 50100;
const MAX_PORT: u16 = 51100;

fn free_port(min: u16) -> u16 {
    for port in min..MAX_PORT {
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!(
        "Could not find a free port between {} and {}",
        min, MAX_PORT
    )
}
