use crate::convert::convert;
use crate::server::create_zetasql_server;
use ast::Expr;
use tokio::runtime::Runtime;
use tonic::transport::channel::Channel;
use tonic::{Response, Status};
use zetasql::analyze_response::Result::*;
use zetasql::zeta_sql_local_service_client::ZetaSqlLocalServiceClient;
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

    pub fn format(&mut self, sql: &String) -> Result<String, String> {
        let request = tonic::Request::new(FormatSqlRequest {
            sql: Some(sql.clone()),
        });
        match self.runtime.block_on(self.client.format_sql(request)) {
            Ok(response) => Ok(response.into_inner().sql.unwrap()),
            Err(status) => Err(String::from(status.message())),
        }
    }

    pub fn parse(
        &mut self,
        sql: &String,
        offset: i32,
        catalog: SimpleCatalogProto,
    ) -> Result<(i32, Expr), String> {
        match self.analyze(sql, offset, catalog) {
            Ok(response) => {
                let response = response.into_inner();
                let offset = response.resume_byte_position.unwrap();
                let expr = match response.result.unwrap() {
                    ResolvedStatement(stmt) => convert(&stmt),
                    ResolvedExpression(_) => panic!("expected statement but found expression"),
                };
                Ok((offset, expr))
            }
            Err(status) => Err(String::from(status.message())),
        }
    }

    fn analyze(
        &mut self,
        sql: &String,
        offset: i32,
        catalog: SimpleCatalogProto,
    ) -> Result<Response<AnalyzeResponse>, Status> {
        let enabled_language_features = catalog
            .builtin_function_options
            .as_ref()
            .unwrap()
            .language_options
            .as_ref()
            .unwrap()
            .enabled_language_features
            .clone();
        let request = tonic::Request::new(AnalyzeRequest {
            simple_catalog: Some(catalog),
            options: Some(AnalyzerOptionsProto {
                default_timezone: Some("UTC".to_string()),
                language_options: Some(LanguageOptionsProto {
                    enabled_language_features,
                    ..Default::default()
                }),
                prune_unused_columns: Some(true),
                ..Default::default()
            }),
            target: Some(analyze_request::Target::ParseResumeLocation(
                ParseResumeLocationProto {
                    input: Some(sql.clone()),
                    byte_position: Some(offset),
                    ..Default::default()
                },
            )),
            ..Default::default()
        });
        self.runtime.block_on(self.client.analyze(request))
    }
}
