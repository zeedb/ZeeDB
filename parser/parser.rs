use std::sync::Mutex;

use crate::{convert::convert, server::ParseClient};
use ast::Expr;
use catalog::{Catalog, CATALOG_KEY};
use context::{Context, ContextKey};
use kernel::*;
use zetasql::{analyze_response::Result::*, analyzer_options_proto::QueryParameterProto, *};

pub const MAX_QUERY: usize = 4_194_304;
pub const PARSER_KEY: ContextKey<Parser> = ContextKey::new("PARSER");

#[derive(Default)]
pub struct Parser {
    client: Mutex<ParseClient>,
}

impl Parser {
    pub fn format(&self, sql: &str) -> String {
        let request = tonic::Request::new(FormatSqlRequest {
            sql: Some(sql.to_string()),
        });
        self.client
            .lock()
            .unwrap()
            .format_sql(request)
            .unwrap()
            .into_inner()
            .sql
            .unwrap()
    }

    pub fn analyze(&self, sql: &str, catalog_id: i64, txn: i64, context: &Context) -> Expr {
        let request = tonic::Request::new(ExtractTableNamesFromStatementRequest {
            sql_statement: Some(sql.to_string()),
            allow_script: Some(true),
            options: Some(language_options()),
        });
        let table_names = self
            .client
            .lock()
            .unwrap()
            .extract_table_names_from_statement(request)
            .unwrap()
            .into_inner()
            .table_name
            .drain(..)
            .map(|name| name.table_name_segment)
            .collect();
        let catalog = context
            .get(CATALOG_KEY)
            .catalog(catalog_id, table_names, txn, context);
        let mut offset = 0;
        let mut exprs = vec![];
        let mut variables = vec![];
        loop {
            let (next_offset, next_expr) =
                self.analyze_next(catalog_id, &catalog, &variables, sql, offset);
            // If we detected a SET _ = _ statement, add it to the query scope.
            if let Expr::LogicalAssign {
                variable, value, ..
            } = &next_expr
            {
                if let Some(i) = variables.iter().position(|(name, _)| name == variable) {
                    variables.remove(i);
                }
                variables.push((variable.clone(), value.data_type()))
            }
            // Add next_expr to list and prepare to continue parsing.
            offset = next_offset;
            exprs.push(next_expr);
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

    fn analyze_next(
        &self,
        catalog_id: i64,
        catalog: &SimpleCatalogProto,
        variables: &Vec<(String, DataType)>,
        sql: &str,
        offset: i32,
    ) -> (i32, Expr) {
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
            .client
            .lock()
            .unwrap()
            .analyze(request)
            .unwrap()
            .into_inner();
        let offset = response.resume_byte_position.unwrap();
        let expr = match response.result.unwrap() {
            ResolvedStatement(stmt) => convert(catalog_id, &stmt),
            ResolvedExpression(_) => panic!("expected statement but found expression"),
        };
        (offset, expr)
    }
}

fn language_options() -> LanguageOptionsProto {
    LanguageOptionsProto {
        enabled_language_features: catalog::enabled_language_features(),
        supported_statement_kinds: catalog::supported_statement_kinds(),
        ..Default::default()
    }
}
