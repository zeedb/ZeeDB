#![allow(dead_code)]

use std::collections::HashMap;

use ast::{Expr, Value};

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
    sql: &str,
    variables: HashMap<String, Value>,
    catalog_id: i64,
    txn: i64,
) -> Result<Expr, String> {
    let analyzed = crate::parser::analyze(sql, &variables, catalog_id, txn)?;
    let expr = crate::convert::convert(&analyzed, variables, catalog_id);
    let optimized = crate::optimize::optimize(expr, txn);
    Ok(optimized)
}
