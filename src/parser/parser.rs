use crate::convert::convert;
use crate::server::create_zetasql_server;
use tokio::runtime::Runtime;
use tonic::Status;
use zetasql::local_service::zeta_sql_local_service_client::ZetaSqlLocalServiceClient;
use zetasql::local_service::{analyze_request, analyze_response, AnalyzeRequest};
use zetasql::{AnyResolvedStatementProto, ParseResumeLocationProto, SimpleCatalogProto};

pub struct ParseProvider {
    runtime: Runtime,
}

impl ParseProvider {
    pub fn new() -> ParseProvider {
        create_zetasql_server();
        let mut runtime = Runtime::new().expect("runtime failed to start");
        ParseProvider { runtime }
    }

    pub fn parse(
        &mut self,
        sql: &str,
        offset: i32,
        catalog: SimpleCatalogProto, // TODO eliminate catalog in favor of rocksdb reference
    ) -> Result<(i32, node::Plan), String> {
        let future = ParseProvider::analyze(sql, offset, catalog);
        let result = self.runtime.block_on(future);
        match result {
            Ok((offset, stmt)) => Ok((offset, convert(stmt))),
            Err(status) => Err(String::from(status.message())),
        }
    }

    async fn analyze(
        sql: &str,
        offset: i32,
        catalog: SimpleCatalogProto,
    ) -> Result<(i32, AnyResolvedStatementProto), Status> {
        let mut client = ZetaSqlLocalServiceClient::connect("http://127.0.0.1:50051")
            .await
            .expect("client failed to connect");
        let request = tonic::Request::new(AnalyzeRequest {
            simple_catalog: Some(catalog),
            target: Some(analyze_request::Target::ParseResumeLocation(
                ParseResumeLocationProto {
                    input: Some(String::from(sql)),
                    byte_position: Some(offset),
                    ..Default::default()
                },
            )),
            ..Default::default()
        });
        let response = client.analyze(request).await?.into_inner();
        let offset = response.resume_byte_position.unwrap_or(-1);
        let statement = match response.result.unwrap() {
            analyze_response::Result::ResolvedStatement(statement) => statement,
            _ => panic!("expression"),
        };
        Ok((offset, statement))
    }
}
