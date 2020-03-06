use tokio::runtime::Runtime;
use zetasql::local_service::zeta_sql_local_service_client::ZetaSqlLocalServiceClient;
use zetasql::local_service::FormatSqlRequest;

mod lib_tests;

pub fn format(sql: String) -> String {
    let mut runtime = Runtime::new().unwrap();
    let mut client = runtime.block_on(ZetaSqlLocalServiceClient::connect("http://127.0.0.1:50051")).unwrap();
    let request = tonic::Request::new(FormatSqlRequest {
        sql: Some(String::from(sql)),
    });
    let response = runtime.block_on(client.format_sql(request)).unwrap().into_inner();
    response.sql.unwrap()
}