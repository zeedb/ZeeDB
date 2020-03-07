use tonic::Status;
use zetasql::local_service::zeta_sql_local_service_client::ZetaSqlLocalServiceClient;
use zetasql::local_service::{AnalyzeRequest, analyze_request, analyze_response};
use zetasql::{AnyResolvedStatementProto, ParseResumeLocationProto, SimpleCatalogProto};

mod lib_tests;

pub async fn analyze(sql: &str, offset: i32, catalog: SimpleCatalogProto) -> Result<(i32, AnyResolvedStatementProto), Status> {
    let mut client = ZetaSqlLocalServiceClient::connect("http://127.0.0.1:50051").await.expect("client failed to connect");
    let request = tonic::Request::new(AnalyzeRequest {
        simple_catalog: Some(catalog),
        target: Some(analyze_request::Target::ParseResumeLocation(ParseResumeLocationProto{
            input: Some(String::from(sql)),
            byte_position: Some(offset),
            .. Default::default()
        })),
        .. Default::default()
    });
    let response = client.analyze(request).await?.into_inner();
    let offset = response.resume_byte_position.unwrap_or(-1);
    let statement = match response.result.unwrap() {
        analyze_response::Result::ResolvedStatement(statement) => statement,
        _ => panic!("expression"),
    };
    Ok((offset, statement))
}