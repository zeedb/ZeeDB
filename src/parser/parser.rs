use crate::convert::convert;
use crate::server::ZETASQL_SERVER;
use arrow::datatypes::DataType;
use ast::Expr;
use catalog::Index;
use std::collections::HashMap;
use std::ops::DerefMut;
use zetasql::analyze_response::Result::*;
use zetasql::analyzer_options_proto::QueryParameterProto;
use zetasql::*;

pub const MAX_QUERY: usize = 4_194_304;

pub fn format(sql: &String) -> Result<String, String> {
    let mut lock = ZETASQL_SERVER.lock().unwrap();
    let (runtime, client) = lock.deref_mut();
    let request = tonic::Request::new(FormatSqlRequest {
        sql: Some(sql.clone()),
    });
    match runtime.block_on(client.format_sql(request)) {
        Ok(response) => Ok(response.into_inner().sql.unwrap()),
        Err(status) => Err(String::from(status.message())),
    }
}

pub fn analyze(catalog_id: i64, catalog: &SimpleCatalogProto, sql: &str) -> Result<Expr, String> {
    let mut offset = 0;
    let mut exprs = vec![];
    let mut variables = vec![];
    loop {
        let (next_offset, next_expr) = analyze_next(catalog_id, catalog, &variables, sql, offset)?;
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
    catalog_id: i64,
    catalog: &SimpleCatalogProto,
    variables: &Vec<(String, DataType)>,
    sql: &str,
    offset: i32,
) -> Result<(i32, Expr), String> {
    let mut lock = ZETASQL_SERVER.lock().unwrap();
    let (runtime, client) = lock.deref_mut();
    let request = tonic::Request::new(AnalyzeRequest {
        simple_catalog: Some(catalog.clone()),
        options: Some(AnalyzerOptionsProto {
            default_timezone: Some("UTC".to_string()),
            language_options: Some(LanguageOptionsProto {
                enabled_language_features: catalog::enabled_language_features(),
                supported_statement_kinds: catalog::supported_statement_kinds(),
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
                input: Some(sql.to_string()),
                byte_position: Some(offset),
                ..Default::default()
            },
        )),
        ..Default::default()
    });
    match runtime.block_on(client.analyze(request)) {
        Ok(response) => {
            let response = response.into_inner();
            let offset = response.resume_byte_position.unwrap();
            let expr = match response.result.unwrap() {
                ResolvedStatement(stmt) => convert(catalog_id, &stmt),
                ResolvedExpression(_) => panic!("expected statement but found expression"),
            };
            Ok((offset, expr))
        }
        Err(status) => Err(String::from(status.message())),
    }
}
