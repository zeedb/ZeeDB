use std::collections::HashMap;

use ast::{Expr, Value};
use catalog::{enabled_language_features, supported_statement_kinds};
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
            let response = parser
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

#[log::trace]
pub fn analyze(
    sql: &str,
    variables: &HashMap<String, Value>,
    catalog_id: i64,
    txn: i64,
) -> Result<Expr, String> {
    // Extract table names from script.
    let table_names = extract_table_names_from_stmt(sql);
    // Construct a minimal catalog containing just the referenced tables.
    let simple_catalog = catalog::simple_catalog(table_names, catalog_id, txn);
    // Parse each statement in the script, one at a time, in a loop.
    let mut offset = 0;
    let mut exprs = vec![];
    loop {
        // Parse the next statement.
        let (stmt, next_offset) = analyze_next_statement(sql, offset, variables, &simple_catalog)?;
        let expr = convert(&stmt, variables, catalog_id);
        offset = next_offset;
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

#[log::trace]
fn extract_table_names_from_stmt(sql: &str) -> Vec<Vec<String>> {
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
    variables: &HashMap<String, Value>,
    simple_catalog: &SimpleCatalogProto,
) -> Result<(AnyResolvedStatementProto, i32), String> {
    let request = AnalyzeRequest {
        simple_catalog: Some(simple_catalog.clone()),
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
        .unwrap()
}

fn language_options() -> LanguageOptionsProto {
    LanguageOptionsProto {
        enabled_language_features: enabled_language_features(),
        supported_statement_kinds: supported_statement_kinds(),
        ..Default::default()
    }
}
