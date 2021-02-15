use std::sync::Mutex;

use ast::Expr;
use catalog::CATALOG_KEY;
use context::{Context, ContextKey};
use kernel::*;
use tokio::runtime::Runtime;
use tonic::transport::{Channel, Endpoint};
use zetasql::{
    analyze_response::Result::*, analyzer_options_proto::QueryParameterProto,
    zeta_sql_local_service_client::ZetaSqlLocalServiceClient, *,
};

pub const MAX_QUERY: usize = 4_194_304;
pub const PARSER_KEY: ContextKey<Parser> = ContextKey::new("PARSER");

pub struct Parser {
    client: Mutex<ZetaSqlLocalServiceClient<Channel>>,
    runtime: Runtime,
}

impl Default for Parser {
    fn default() -> Self {
        crate::server::start_server_process();
        // TODO this is necessary because we're mixing sync and async.
        std::thread::spawn(|| {
            let runtime = Runtime::new().unwrap();
            let client = runtime
                .block_on(async { Mutex::new(parser("http://127.0.0.1:50051".to_string())) });
            Self { client, runtime }
        })
        .join()
        .unwrap()
    }
}

fn parser(dst: String) -> ZetaSqlLocalServiceClient<Channel> {
    let chan = Endpoint::new(dst).unwrap().connect_lazy().unwrap();
    ZetaSqlLocalServiceClient::new(chan)
}

impl Parser {
    pub fn format(&self, sql: &str) -> String {
        let mut client = self.client.lock().unwrap();
        let request = tonic::Request::new(FormatSqlRequest {
            sql: Some(sql.to_string()),
        });
        self.runtime
            .block_on(client.format_sql(request))
            .unwrap()
            .into_inner()
            .sql
            .unwrap()
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
        let mut client = self.client.lock().unwrap();
        let request = tonic::Request::new(ExtractTableNamesFromStatementRequest {
            sql_statement: Some(sql.to_string()),
            allow_script: Some(true),
            options: Some(language_options()),
        });
        let table_names = self
            .runtime
            .block_on(client.extract_table_names_from_statement(request))
            .unwrap()
            .into_inner()
            .table_name
            .drain(..)
            .map(|name| name.table_name_segment)
            .collect();
        // Construct a minimal catalog containing just the referenced tables.
        let catalog = context[CATALOG_KEY].catalog(catalog_id, table_names, txn, context);
        // Parse each statement in the script, one at a time, in a loop.
        let mut offset = 0;
        let mut exprs = vec![];
        loop {
            // Parse the next statement.
            let request = tonic::Request::new(AnalyzeRequest {
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
                target: Some(analyze_request::Target::ParseResumeLocation(
                    ParseResumeLocationProto {
                        input: Some(sql.to_string()),
                        byte_position: Some(offset),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            });
            let response = self
                .runtime
                .block_on(client.analyze(request))
                .unwrap()
                .into_inner();
            let expr = match response.result.unwrap() {
                ResolvedStatement(stmt) => crate::convert::convert(catalog_id, &stmt),
                ResolvedExpression(_) => panic!("expected statement but found expression"),
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
        enabled_language_features: catalog::enabled_language_features(),
        supported_statement_kinds: catalog::supported_statement_kinds(),
        ..Default::default()
    }
}
