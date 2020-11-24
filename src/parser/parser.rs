use crate::convert::convert;
use crate::server::create_zetasql_server;
use arrow::datatypes::DataType;
use ast::Expr;
use tokio::runtime::Runtime;
use tonic::transport::channel::Channel;
use zetasql::analyze_response::Result::*;
use zetasql::analyzer_options_proto::QueryParameterProto;
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

    pub fn analyze(
        &mut self,
        sql: &String,
        catalog: (i64, SimpleCatalogProto),
    ) -> Result<Expr, String> {
        let mut offset = 0;
        let mut exprs = vec![];
        let mut variables = vec![];
        loop {
            let (next_offset, next_expr) =
                self.analyze_next(sql, offset, catalog.clone(), &variables)?;
            // If we detected a SET _ = _ statement, add it to the query scope.
            if let Expr::LogicalAssign {
                variable, value, ..
            } = &next_expr
            {
                if let Some(i) = variables.iter().position(|(name, _)| name == variable) {
                    variables.remove(i);
                }
                variables.push((variable.clone(), value.data()))
            }
            // Add next_expr to list and prepare to continue parsing.
            offset = next_offset;
            exprs.push(next_expr);
            // If we've parsed the entire expression, return.
            if offset as usize == sql.as_bytes().len() {
                if exprs.len() == 1 {
                    return Ok(exprs.pop().unwrap());
                } else {
                    return Ok(Expr::LogicalScript { statements: exprs });
                }
            }
        }
    }

    fn analyze_next(
        &mut self,
        sql: &String,
        offset: i32,
        catalog: (i64, SimpleCatalogProto),
        variables: &Vec<(String, DataType)>,
    ) -> Result<(i32, Expr), String> {
        let request = tonic::Request::new(AnalyzeRequest {
            simple_catalog: Some(catalog.1),
            options: Some(AnalyzerOptionsProto {
                default_timezone: Some("UTC".to_string()),
                language_options: Some(LanguageOptionsProto {
                    enabled_language_features: bootstrap::enabled_language_features(),
                    supported_statement_kinds: bootstrap::supported_statement_kinds(),
                    ..Default::default()
                }),
                prune_unused_columns: Some(true),
                query_parameters: variables
                    .iter()
                    .map(|(name, data)| QueryParameterProto {
                        name: Some(name.clone()),
                        r#type: Some(ast::data_type::to_proto(data)),
                    })
                    .collect(),
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
        match self.runtime.block_on(self.client.analyze(request)) {
            Ok(response) => {
                let response = response.into_inner();
                let offset = response.resume_byte_position.unwrap();
                let expr = match response.result.unwrap() {
                    ResolvedStatement(stmt) => convert(catalog.0, &stmt),
                    ResolvedExpression(_) => panic!("expected statement but found expression"),
                };
                Ok((offset, expr))
            }
            Err(status) => Err(String::from(status.message())),
        }
    }
}
