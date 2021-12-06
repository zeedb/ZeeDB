#![allow(dead_code)]

use std::{collections::HashMap, sync::Mutex};

use ast::Expr;
use catalog::SimpleCatalogProvider;
use kernel::DataType;
use once_cell::sync::Lazy;

mod bootstrap;
mod cardinality_estimation;
mod catalog;
mod convert;
mod cost;
mod distribution;
mod optimize;
mod parser;
#[cfg(test)]
mod parser_tests;
mod rewrite;
mod rule;
mod search_space;
mod unnest;

pub fn plan(
    sql: String,
    params: HashMap<String, DataType>,
    catalog_id: i64,
    txn: i64,
) -> Result<Expr, String> {
    // Calling ZetaSQL is expensive so we cache it.
    let table_names = cached_table_names(&sql);
    // This step is not cached because the catalog changes when a DDL statement is executed.
    let catalog = crate::catalog::simple_catalog(table_names, catalog_id, txn);
    // Calling ZetaSQL and optimizing the expression is expensive so we cache it.
    cached_analyze_optimize(sql, params, catalog)
}

fn cached_table_names(sql: &str) -> Vec<Vec<String>> {
    // TODO this should be an LRU cache.
    static TABLE_NAMES_CACHE: Lazy<Mutex<HashMap<String, Vec<Vec<String>>>>> =
        Lazy::new(Default::default);
    TABLE_NAMES_CACHE
        .lock()
        .unwrap()
        .entry(sql.to_string())
        .or_insert_with(|| crate::parser::extract_table_names_from_stmt(&sql))
        .clone()
}

fn cached_analyze_optimize(
    sql: String,
    params: HashMap<String, DataType>,
    catalog: SimpleCatalogProvider,
) -> Result<Expr, String> {
    // TODO this should be an LRU cache.
    static ANALYZE_CACHE: Lazy<Mutex<HashMap<Key, Result<Expr, String>>>> =
        Lazy::new(Default::default);
    let mut key = Key {
        sql,
        params: params
            .iter()
            .map(|(name, data_type)| (name.clone(), data_type.clone()))
            .collect(),
        catalog,
    };
    key.params.sort_by(|(left, _), (right, _)| left.cmp(right));
    ANALYZE_CACHE
        .lock()
        .unwrap()
        .entry(key)
        .or_insert_with_key(|key| {
            let expr = crate::parser::analyze(&key.sql, &params, &key.catalog)?;
            Ok(crate::optimize::optimize(expr, key.catalog.indexes()))
        })
        .clone()
}

#[derive(PartialEq, Eq, Hash)]
struct Key {
    sql: String,
    params: Vec<(String, DataType)>,
    catalog: SimpleCatalogProvider,
}
