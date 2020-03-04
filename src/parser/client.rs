
use zetasql::local_service::zeta_sql_local_service_client::ZetaSqlLocalServiceClient;
use zetasql::local_service::FormatSqlRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = ZetaSqlLocalServiceClient::connect("http://127.0.0.1:50051").await?;

    let request = tonic::Request::new(FormatSqlRequest {
        sql: Some(String::from("select foo")),
    });

    let response = client.format_sql(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
