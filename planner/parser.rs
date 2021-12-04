use std::{collections::HashMap, sync::Mutex};

use ast::Value;
use defaults::{enabled_language_features, supported_statement_kinds};
use kernel::DataType;
use once_cell::sync::Lazy;
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
) -> Result<Vec<AnyResolvedStatementProto>, String> {
    // Extract table names from script.
    let table_names = extract_table_names_from_stmt(sql);
    // Construct a minimal catalog containing just the referenced tables.
    let simple_catalog = crate::catalog::simple_catalog(table_names, catalog_id, txn);
    // Analyze the sql statement, or use the cache.
    analyze_with_catalog(sql, variables, simple_catalog)
}

#[derive(PartialEq, Eq, Hash)]
struct Key {
    sql: String,
    variables: Vec<(String, DataType)>,
    catalog: SimpleCatalogProvider,
}

// TODO this should be an LRU cache.
static ANALYZE_CACHE: Lazy<Mutex<HashMap<Key, Vec<AnyResolvedStatementProto>>>> =
    Lazy::new(Default::default);

fn analyze_with_catalog(
    sql: &str,
    variables: &HashMap<String, Value>,
    catalog: SimpleCatalogProvider,
) -> Result<Vec<AnyResolvedStatementProto>, String> {
    // Check if we have already cached this request.
    let mut cache = ANALYZE_CACHE.lock().unwrap();
    let mut key = Key {
        sql: sql.to_string(),
        variables: variables
            .iter()
            .map(|(name, value)| (name.clone(), value.data_type()))
            .collect(),
        catalog,
    };
    key.variables
        .sort_by(|(left, _), (right, _)| left.cmp(right));
    if let Some(stmts) = cache.get(&key) {
        return Ok(stmts.clone());
    }
    // Parse each statement in the script, one at a time, in a loop.
    let simple_catalog = key.catalog.to_proto();
    let mut offset = 0;
    let mut stmts = vec![];
    loop {
        // Parse the next statement.
        let (stmt, next_offset) = analyze_next_statement(sql, offset, variables, &simple_catalog)?;
        offset = next_offset;
        stmts.push(stmt);
        // If we've parsed the entire expression, return.
        if offset as usize == sql.as_bytes().len() {
            break;
        }
    }
    // Cache the request.
    cache.insert(key, stmts.clone());
    // Return the newly-cached statements.
    Ok(stmts)
}

// TODO this should be an LRU cache.
static TABLE_NAMES_CACHE: Lazy<Mutex<HashMap<String, Vec<Vec<String>>>>> =
    Lazy::new(Default::default);

#[log::trace]
fn extract_table_names_from_stmt(sql: &str) -> Vec<Vec<String>> {
    // Check if we have already cached this request.
    let mut cache = TABLE_NAMES_CACHE.lock().unwrap();
    if let Some(table_names) = cache.get(sql) {
        return table_names.clone();
    }
    // Cache the request.
    let table_names: Vec<Vec<String>> = log::rpc(async move {
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
    });
    // Cache the request.
    cache.insert(sql.to_string(), table_names.clone());
    // Return the newly-cached table names.
    table_names
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
