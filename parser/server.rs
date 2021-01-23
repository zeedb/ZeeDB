use once_cell::sync::Lazy;
use std::{process::Command, sync::Mutex};
use tokio::runtime::Runtime;
use tonic::transport::channel::Channel;
use zetasql::zeta_sql_local_service_client::ZetaSqlLocalServiceClient;

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

pub static ZETASQL_SERVER: Lazy<Mutex<(Runtime, ZetaSqlLocalServiceClient<Channel>)>> =
    Lazy::new(|| {
        Command::new("sh")
            .arg("-c")
            .arg(SCRIPT)
            .output()
            .expect("failed to start docker");
        let runtime = Runtime::new().expect("runtime failed to start");
        let client = runtime
            .block_on(ZetaSqlLocalServiceClient::connect("http://127.0.0.1:50051"))
            .expect("client failed to connect");
        Mutex::new((runtime, client))
    });
