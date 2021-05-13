use std::{net::TcpListener, sync::Mutex};

use ast::Expr;
use catalog_types::{enabled_language_features, supported_statement_kinds, CATALOG_KEY};
use context::{Context, ContextKey};
use kernel::*;
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
        protos::runtime().block_on(async {
            let zetasql = std::env::var("ZETASQL").unwrap_or_else(|_| {
                if TcpListener::bind(("127.0.0.1", 50051)).is_ok() {
                    println!("\x1b[0;31mEnvironment variable ZETASQL is not set and a local server was not detected at localhost:50051.\x1b[0m");
                    println!("\x1b[0;31mIf you are running locally, run:\x1b[0m");
                    println!("\x1b[0;31m\tdocker run --publish 127.0.0.1:50051:50051 --name zetasql --detach gcr.io/zeedeebee/zetasql\x1b[0m");
                    panic!("Missing ZetaSQL parser/analyzer service")
                }
                "http://localhost:50051".to_string()
            });
            let client = Mutex::new(ZetaSqlLocalServiceClient::connect(zetasql).await.unwrap());
            Self { client }
        })
    }
}

impl Parser {
    pub fn format(&self, sql: &str) -> String {
        protos::runtime().block_on(async {
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

    pub fn analyze(
        &self,
        sql: &str,
        catalog_id: i64,
        txn: i64,
        mut variables: Vec<(String, DataType)>,
        context: &Context,
    ) -> Expr {
        // Extract table names from script.
        let table_names = protos::runtime().block_on(async move {
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
                        .map(|(name, data_type)| QueryParameterProto {
                            name: Some(name.clone()),
                            r#type: Some(data_type.to_proto()),
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
            let response = protos::runtime().block_on(async move {
                self.client
                    .lock()
                    .unwrap()
                    .analyze(Request::new(request))
                    .await
                    .unwrap()
                    .into_inner()
            });
            let expr = match response.result.unwrap() {
                ResolvedStatement(stmt) => convert(catalog_id, &stmt),
                ResolvedExpression(_) => {
                    panic!("expected statement but found expression")
                }
            };
            // If we detected a SET _ = _ statement, add it to the query scope.
            if let Expr::LogicalAssign {
                variable, value, ..
            } = &expr
            {
                if let Some(i) = variables.iter().position(|(name, _)| name == variable) {
                    variables.remove(i);
                }
                variables.push((variable.clone(), value.data_type()))
            }
            // Add expr to list and prepare to continue parsing.
            offset = response.resume_byte_position.unwrap();
            exprs.push(expr);
            // If we've parsed the entire expression, return.
            if offset as usize == sql.as_bytes().len() {
                if exprs.len() == 1 {
                    return exprs.pop().unwrap();
                } else {
                    return Expr::LogicalScript { statements: exprs };
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
