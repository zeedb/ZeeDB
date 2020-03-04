use std::process::Command;
use tokio::runtime::Runtime;
use zetasql::local_service::zeta_sql_local_service_client::ZetaSqlLocalServiceClient;
use zetasql::local_service::FormatSqlRequest;

fn format(sql: String) -> String {
    let mut runtime = Runtime::new().unwrap();
    let mut client = runtime.block_on(ZetaSqlLocalServiceClient::connect("http://127.0.0.1:50051")).unwrap();
    let request = tonic::Request::new(FormatSqlRequest {
        sql: Some(String::from(sql)),
    });
    let response = runtime.block_on(client.format_sql(request)).unwrap().into_inner();
    response.sql.unwrap()
}

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
    gcr.io/analog-delight-604/zetasql-server@sha256:93ba0c9ad4bbddd50a2b3ed1722fb81f31263afc8c28c23e7e7be61158b0e7ab
";

fn create_zetasql_server() {
    Command::new("sh")
            .arg("-c")
            .arg(SCRIPT)
            .output()
            .expect("failed to start docker");
}

#[test]
fn test_create_server() {
    create_zetasql_server();
}

#[test]
fn test_format() {
    create_zetasql_server();
    assert_eq!("SELECT\n  foo;", format(String::from("select foo")));
}