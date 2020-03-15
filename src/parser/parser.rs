use crate::convert::convert;
use crate::server::create_zetasql_server;
use tokio::runtime::Runtime;
use tonic::transport::channel::Channel;
use tonic::{Response, Status};
use zetasql::local_service::analyze_response::Result::*;
use zetasql::local_service::zeta_sql_local_service_client::ZetaSqlLocalServiceClient;
use zetasql::local_service::*;
use zetasql::*;

pub struct ParseProvider {
    runtime: Runtime,
    client: ZetaSqlLocalServiceClient<Channel>,
}

impl ParseProvider {
    pub fn new() -> ParseProvider {
        create_zetasql_server();
        let mut runtime = Runtime::new().expect("runtime failed to start");
        let client = runtime
            .block_on(ZetaSqlLocalServiceClient::connect("http://127.0.0.1:50051"))
            .expect("client failed to connect");
        ParseProvider { runtime, client }
    }

    pub fn parse(
        &mut self,
        sql: &str,
        offset: i32,
        catalog: &SimpleCatalogProto, // TODO eliminate catalog in favor of rocksdb reference
    ) -> Result<(i32, node::Plan), String> {
        match self.analyze(sql, offset, catalog) {
            Ok(response) => {
                let response = response.into_inner();
                let offset = response.resume_byte_position.unwrap();
                let plan = match response.result.unwrap() {
                    ResolvedStatement(stmt) => convert(&stmt),
                    ResolvedExpression(_) => panic!("expected statement but found expression"),
                };
                Ok((offset, plan))
            }
            Err(status) => Err(String::from(status.message())),
        }
    }

    fn analyze(
        &mut self,
        sql: &str,
        offset: i32,
        catalog: &SimpleCatalogProto,
    ) -> Result<Response<AnalyzeResponse>, Status> {
        let request = tonic::Request::new(AnalyzeRequest {
            simple_catalog: Some(catalog.clone()),
            target: Some(analyze_request::Target::ParseResumeLocation(
                ParseResumeLocationProto {
                    input: Some(String::from(sql)),
                    byte_position: Some(offset),
                    ..Default::default()
                },
            )),
            ..Default::default()
        });
        self.runtime.block_on(self.client.analyze(request))
    }
}
