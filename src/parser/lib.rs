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

// You need to run docker run -p 50051:50051 gcr.io/analog-delight-604/zetasql-server@sha256:93ba0c9ad4bbddd50a2b3ed1722fb81f31263afc8c28c23e7e7be61158b0e7ab
#[test]
fn test_format() {
    assert_eq!("SELECT\n  foo;", format(String::from("select foo")));
}