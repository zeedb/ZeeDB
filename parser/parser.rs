use std::{collections::HashMap, net::TcpListener, sync::Mutex, time::Duration};

use ast::{Expr, Value};
use catalog_types::{enabled_language_features, supported_statement_kinds, CATALOG_KEY};
use context::{Context, ContextKey};
use tonic::{transport::Channel, Request};
use zetasql::{
    analyze_request::Target::ParseResumeLocation,
    analyze_response::Result::{ResolvedExpression, ResolvedStatement},
    analyzer_options_proto::QueryParameterProto,
    zeta_sql_local_service_client::ZetaSqlLocalServiceClient,
    *,
};

use crate::convert::convert;

pub const MAX_QUERY: usize = 4_194_304;
pub const PARSER_KEY: ContextKey<Parser> = ContextKey::new("PARSER");

pub struct Parser {
    client: Mutex<ZetaSqlLocalServiceClient<Channel>>,
}

impl Default for Parser {
    fn default() -> Self {
        rpc::runtime().block_on(async {
            let zetasql = std::env::var("ZETASQL").unwrap_or_else(|_| {
                if TcpListener::bind(("127.0.0.1", 50051)).is_ok() {
                    println!("\x1b[0;31mEnvironment variable ZETASQL is not set and a local server was not detected at localhost:50051.\x1b[0m");
                    println!("\x1b[0;31mIf you are running on CI or in the linux devcontainer, run zetasql_server &\x1b[0m");
                    println!("\x1b[0;31mIf you are running on a non-linux system, run:\x1b[0m");
                    println!("\x1b[0;31m\tdocker run --publish 127.0.0.1:50051:50051 --name zetasql --detach gcr.io/zeedeebee/zetasql\x1b[0m");
                    panic!("Missing ZetaSQL parser/analyzer service")
                }
                "http://localhost:50051".to_string()
            });
            let mut client = ZetaSqlLocalServiceClient::connect(zetasql.clone()).await.unwrap();
            for _ in 0..10usize {
                match client.format_sql(FormatSqlRequest {
                    sql: Some("select 1".to_string())
                }).await {
                    Ok(_) => return Self { client: Mutex::new(client) },
                    Err(_) => std::thread::sleep(Duration::from_millis(1)),
                }
            }
            panic!("Failed to connect to ZetaSQL parser services at {}", &zetasql)
        })
    }
}

impl Parser {
    pub fn format(&self, sql: &str) -> String {
        rpc::runtime().block_on(async {
            self.client
                .lock()
                .unwrap()
                .format_sql(Request::new(FormatSqlRequest {
                    sql: Some(sql.to_string()),
                }))
                .await
                .unwrap()
                .into_inner()
                .sql
                .unwrap()
        })
    }

    pub fn split(&self, sql: &str) -> Vec<String> {
        rpc::runtime().block_on(async move {
            let mut statements = vec![];
            let mut offset = 0usize;
            while offset < sql.len() {
                let request = ExtractTableNamesFromNextStatementRequest {
                    parse_resume_location: ParseResumeLocationProto {
                        input: Some(sql.to_string()),
                        byte_position: Some(offset as i32),
                        ..Default::default()
                    },
                    options: Some(language_options()),
                };
                let response = self
                    .client
                    .lock()
                    .unwrap()
                    .extract_table_names_from_next_statement(request)
                    .await
                    .unwrap()
                    .into_inner();
                let next_offset = response.resume_byte_position.unwrap() as usize;
                statements.push(sql[offset..next_offset].to_string());
                offset = next_offset;
            }
            statements
        })
    }

    pub fn analyze(
        &self,
        sql: &str,
        catalog_id: i64,
        txn: i64,
        variables: &HashMap<String, Value>,
        context: &Context,
    ) -> Result<Expr, String> {
        // Extract table names from script.
        let table_names = rpc::runtime().block_on(async move {
            self.client
                .lock()
                .unwrap()
                .extract_table_names_from_statement(Request::new(
                    ExtractTableNamesFromStatementRequest {
                        sql_statement: Some(sql.to_string()),
                        allow_script: Some(true),
                        options: Some(language_options()),
                    },
                ))
                .await
                .unwrap()
                .into_inner()
                .table_name
                .drain(..)
                .map(|name| name.table_name_segment)
                .collect()
        });
        // Construct a minimal catalog containing just the referenced tables.
        let catalog = context[CATALOG_KEY].catalog(catalog_id, table_names, txn, context);
        // Parse each statement in the script, one at a time, in a loop.
        let mut offset = 0;
        let mut exprs = vec![];
        loop {
            // Parse the next statement.
            let request = AnalyzeRequest {
                simple_catalog: Some(catalog.clone()),
                options: Some(AnalyzerOptionsProto {
                    default_timezone: Some("UTC".to_string()),
                    language_options: Some(language_options()),
                    prune_unused_columns: Some(true),
                    query_parameters: variables
                        .iter()
                        .map(|(name, value)| QueryParameterProto {
                            name: Some(name.clone()),
                            r#type: Some(value.data_type().to_proto()),
                        })
                        .collect(),
                    ..Default::default()
                }),
                target: Some(ParseResumeLocation(ParseResumeLocationProto {
                    input: Some(sql.to_string()),
                    byte_position: Some(offset),
                    ..Default::default()
                })),
                ..Default::default()
            };
            let response = match rpc::runtime().block_on(async move {
                self.client
                    .lock()
                    .unwrap()
                    .analyze(Request::new(request))
                    .await
            }) {
                Ok(response) => response.into_inner(),
                Err(status) => return Err(status.message().to_string()),
            };
            let expr = match response.result.unwrap() {
                ResolvedStatement(stmt) => convert(catalog_id, variables, &stmt, context),
                ResolvedExpression(_) => {
                    panic!("expected statement but found expression")
                }
            };
            // Add expr to list and prepare to continue parsing.
            offset = response.resume_byte_position.unwrap();
            exprs.push(expr);
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
}

fn language_options() -> LanguageOptionsProto {
    LanguageOptionsProto {
        enabled_language_features: enabled_language_features(),
        supported_statement_kinds: supported_statement_kinds(),
        ..Default::default()
    }
}
