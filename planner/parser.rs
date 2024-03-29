use std::collections::HashMap;

use ast::Expr;
use defaults::{enabled_language_features, supported_statement_kinds};
use kernel::DataType;
use tonic::{transport::Channel, Request};
use zetasql::{
    analyze_request::Target::ParseResumeLocation,
    analyze_response::Result::{ResolvedExpression, ResolvedStatement},
    analyzer_options_proto::QueryParameterProto,
    zeta_sql_local_service_client::ZetaSqlLocalServiceClient,
    *,
};

use crate::catalog::SimpleCatalogProvider;

pub const MAX_QUERY: usize = 4_194_304;

#[log::trace]
pub fn format(sql: &str) -> String {
    log::rpc(async {
        parser()
            .await
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

#[log::trace]
pub fn split(sql: &str) -> Vec<String> {
    log::rpc(async move {
        let mut parser = parser().await;
        let mut stmts = vec![];
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
            let response = parser
                .extract_table_names_from_next_statement(request)
                .await
                .unwrap()
                .into_inner();
            let next_offset = response.resume_byte_position.unwrap() as usize;
            stmts.push(sql[offset..next_offset].to_string());
            offset = next_offset;
        }
        stmts
    })
}

#[log::trace]
pub fn analyze(
    sql: &str,
    params: &HashMap<String, DataType>,
    catalog: &SimpleCatalogProvider,
) -> Result<Expr, String> {
    // Parse each statement in the script, one at a time, in a loop.
    let simple_catalog = catalog.to_proto();
    let mut offset = 0;
    let mut stmts = vec![];
    loop {
        // Parse the next statement.
        let (stmt, next_offset) = analyze_next_statement(sql, offset, params, &simple_catalog)?;
        offset = next_offset;
        stmts.push(stmt);
        // If we've parsed the entire expression, return.
        if offset as usize == sql.as_bytes().len() {
            break;
        }
    }
    Ok(crate::convert::convert(&stmts, catalog.id()))
}

#[log::trace]
pub fn extract_table_names_from_stmt(sql: &str) -> Vec<Vec<String>> {
    log::rpc(async move {
        parser()
            .await
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
    })
}

#[log::trace]
fn analyze_next_statement(
    sql: &str,
    offset: i32,
    params: &HashMap<String, DataType>,
    simple_catalog: &SimpleCatalogProto,
) -> Result<(AnyResolvedStatementProto, i32), String> {
    let request = AnalyzeRequest {
        simple_catalog: Some(simple_catalog.clone()),
        options: Some(AnalyzerOptionsProto {
            default_timezone: Some("UTC".to_string()),
            language_options: Some(language_options()),
            prune_unused_columns: Some(true),
            query_parameters: params
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
    let response = match log::rpc(async move { parser().await.analyze(request).await }) {
        Ok(response) => response.into_inner(),
        Err(status) => return Err(status.message().to_string()),
    };
    match response.result.unwrap() {
        ResolvedStatement(stmt) => Ok((stmt, response.resume_byte_position.unwrap())),
        ResolvedExpression(_) => {
            panic!("expected statement but found expression")
        }
    }
}

async fn parser() -> ZetaSqlLocalServiceClient<Channel> {
    ZetaSqlLocalServiceClient::connect("http://localhost:50051")
        .await
        .expect(NO_PARSER)
}

const NO_PARSER: &str = "\x1b[0;31m
Failed to connect to ZetaSQL analyzer services at http://localhost:50051.
If you are running on CI or in the linux devcontainer, run:
\tzetasql_server &
If you are running on a non-linux system, run:
\tdocker run --publish 127.0.0.1:50051:50051 --name zetasql --detach gcr.io/zeedeebee/zetasql
\x1b[0m";

fn language_options() -> LanguageOptionsProto {
    LanguageOptionsProto {
        enabled_language_features: enabled_language_features(),
        supported_statement_kinds: supported_statement_kinds(),
        ..Default::default()
    }
}
