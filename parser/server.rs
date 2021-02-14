use std::{
    error::Error,
    process::Command,
    sync::mpsc::{sync_channel, Receiver, SyncSender},
    thread,
};

use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;
use tonic::{
    transport::{Channel, Endpoint},
    Request, Response, Status,
};
use zetasql::{
    zeta_sql_local_service_client::ZetaSqlLocalServiceClient, AnalyzeRequest, AnalyzeResponse,
    ExtractTableNamesFromStatementRequest, ExtractTableNamesFromStatementResponse,
    FormatSqlRequest, FormatSqlResponse,
};

const SCRIPT: &str = r"
if [[ `docker ps --filter name=test-zetasql-server -q` ]]; then 
    echo 'ZetaSQL server is already running'
    exit 0
fi

stopped=`docker ps --all --filter name=test-zetasql-server -q`

if [[ $stopped ]]; then 
    echo 'ZetaSQL server is stopped'
    docker restart $stopped
    exit 0
fi

docker run \
    --publish 127.0.0.1:50051:50051 \
    --name test-zetasql-server \
    --detach \
    gcr.io/analog-delight-604/zetasql-server@sha256:62325de833178537b23ae83c8bb8dce6da51112491f3e7e9f101a262533b9df7

until nc -z 127.0.0.1 50051
do
    echo 'waiting for zetasql container...'
    sleep 0.5
done
";

pub(crate) struct ParseClient {
    sender: SyncSender<ParseRequest>,
}

enum ParseRequest {
    FormatSql {
        request: Request<FormatSqlRequest>,
        response: SyncSender<Result<Response<FormatSqlResponse>, Status>>,
    },
    Analyze {
        request: Request<AnalyzeRequest>,
        response: SyncSender<Result<Response<AnalyzeResponse>, Status>>,
    },
    ExtractTableNamesFromStatement {
        request: Request<ExtractTableNamesFromStatementRequest>,
        response: SyncSender<Result<Response<ExtractTableNamesFromStatementResponse>, Status>>,
    },
}

impl ParseClient {
    pub(crate) fn format_sql(
        &mut self,
        request: Request<FormatSqlRequest>,
    ) -> Result<Response<FormatSqlResponse>, Status> {
        let (sender, receiver) = sync_channel(0);
        self.sender
            .send(ParseRequest::FormatSql {
                request,
                response: sender,
            })
            .unwrap();
        receiver.recv().unwrap()
    }

    pub(crate) fn analyze(
        &mut self,
        request: Request<AnalyzeRequest>,
    ) -> Result<Response<AnalyzeResponse>, Status> {
        let (sender, receiver) = sync_channel(0);
        self.sender
            .send(ParseRequest::Analyze {
                request,
                response: sender,
            })
            .unwrap();
        receiver.recv().unwrap()
    }

    pub(crate) fn extract_table_names_from_statement(
        &mut self,
        request: Request<ExtractTableNamesFromStatementRequest>,
    ) -> Result<Response<ExtractTableNamesFromStatementResponse>, Status> {
        let (sender, receiver) = sync_channel(0);
        self.sender
            .send(ParseRequest::ExtractTableNamesFromStatement {
                request,
                response: sender,
            })
            .unwrap();
        receiver.recv().unwrap()
    }
}

impl Default for ParseClient {
    fn default() -> Self {
        static SERVER: OnceCell<SyncSender<ParseRequest>> = OnceCell::new();
        let sender = SERVER.get_or_init(start_server_thread).clone();
        Self { sender }
    }
}

/// Start a server thread, that processes ParseRequests one-at-a-time, and forwards them to the external ZetaSQL server process.
/// This extra layer of indirection is necessary because we want to use the parse service from within the async context of our own Worker gRPC service.
/// Tokio doesn't allow you to call runtime.block_on(_) from an async context.
/// So we have to send our requests to be processed on a separate thread.
fn start_server_thread() -> SyncSender<ParseRequest> {
    let (sender, receiver) = sync_channel(0);
    // Wait for the server process to start, so there is someone to accept the requests.
    start_server_process();
    // Process ParseRequests on a separate thread.
    thread::spawn(move || {
        process_requests(receiver);
    });
    // We will use this channel to communicate with the parser thread.
    sender
}

/// Run the startup script for the local ZetaSQL server process.
fn start_server_process() {
    Command::new("sh")
        .arg("-c")
        .arg(SCRIPT)
        .output()
        .expect("failed to start docker");
}

/// Process parse requests inside the parser thread, and send the results back to the requester thread.
fn process_requests(receiver: Receiver<ParseRequest>) {
    let runtime = Runtime::new().unwrap();
    let mut client = runtime.block_on(client()).unwrap();
    loop {
        match receiver.recv().unwrap() {
            ParseRequest::FormatSql { request, response } => response
                .send(runtime.block_on(client.format_sql(request)))
                .unwrap(),
            ParseRequest::Analyze { request, response } => response
                .send(runtime.block_on(client.analyze(request)))
                .unwrap(),
            ParseRequest::ExtractTableNamesFromStatement { request, response } => response
                .send(runtime.block_on(client.extract_table_names_from_statement(request)))
                .unwrap(),
        }
    }
}

async fn client() -> Result<ZetaSqlLocalServiceClient<Channel>, Box<dyn Error>> {
    let chan = Endpoint::new("http://127.0.0.1:50051")?.connect_lazy()?;
    Ok(ZetaSqlLocalServiceClient::new(chan))
}
