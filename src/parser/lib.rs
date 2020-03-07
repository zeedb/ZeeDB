use tokio::runtime::Runtime;
use zetasql::local_service::zeta_sql_local_service_client::ZetaSqlLocalServiceClient;
use zetasql::local_service::{AnalyzeRequest, analyze_request, analyze_response};
use zetasql::{AnyResolvedStatementProto, ParseResumeLocationProto, SimpleCatalogProto};

mod lib_tests;

pub fn analyze(sql: String, offset: i32, catalog: SimpleCatalogProto) -> Result<(i32, AnyResolvedStatementProto), String> {
    let mut runtime = Runtime::new().expect("runtime failed to start");
    let mut client = runtime.block_on(ZetaSqlLocalServiceClient::connect("http://127.0.0.1:50051")).expect("client failed to connect");
    let request = tonic::Request::new(AnalyzeRequest {
        simple_catalog: Some(catalog),
        target: Some(analyze_request::Target::ParseResumeLocation(ParseResumeLocationProto{
            input: Some(sql),
            byte_position: Some(offset),
            .. Default::default()
        })),
        .. Default::default()
    });
    let response = match runtime.block_on(client.analyze(request)) {
        Ok(response) => response.into_inner(),
        Err(status) => return Err(String::from(status.message())),
    };
    let offset = response.resume_byte_position.unwrap_or(-1);
    let statement = match response.result.unwrap() {
        analyze_response::Result::ResolvedStatement(statement) => statement,
        _ => panic!("expression"),
    };
    Ok((offset, statement))
}