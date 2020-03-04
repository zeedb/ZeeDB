

use zetasql::local_service::zeta_sql_local_service_client::ZetaSqlLocalServiceClient;
use zetasql::local_service::FormatSqlRequest;

use tonic::transport::channel::Channel;

use futures::executor::block_on;

async fn format(sql: String) -> String {
    let mut client = ZetaSqlLocalServiceClient::connect("http://127.0.0.1:50051").await.unwrap();
    let request = tonic::Request::new(FormatSqlRequest {
        sql: Some(String::from("select foo")),
    });
    let response = client.format_sql(request).await.unwrap().into_inner();
    response.sql.unwrap()
}

#[tokio::test]
async fn test_format() {
    assert_eq!("SELECT\n  foo;", format(String::from("select foo")).await);
}