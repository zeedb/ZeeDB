use std::collections::HashMap;

use ast::{Index, *};
use kernel::{Array, DataType};
use zetasql::{SimpleCatalogProto, SimpleColumnProto, SimpleTableProto};

use crate::{defaults::*, METADATA_CATALOG_ID, RESERVED_IDS};

#[log::trace]
pub fn simple_catalog(
    table_names: Vec<Vec<String>>,
    catalog_id: i64,
    txn: i64,
) -> SimpleCatalogProto {
    if catalog_id == METADATA_CATALOG_ID {
        return crate::bootstrap::bootstrap_metadata_catalog();
    }
    let mut catalog_id_cache = HashMap::new();
    let mut root_catalog = SimpleCatalogProto {
        builtin_function_options: Some(builtin_function_options()),
        named_type: builtin_named_types(),
        ..Default::default()
    };
    for name in &table_names {
        let mut catalog_id = catalog_id;
        let mut catalog = &mut root_catalog;
        for catalog_name in &name[..name.len() - 1] {
            catalog_id = *catalog_id_cache
                .entry((catalog_id, catalog_name))
                .or_insert_with(|| catalog_name_to_id(catalog_id, catalog_name, txn));
            catalog = find_or_push_catalog(catalog, catalog_name);
        }
        let table_name = &name[name.len() - 1];
        let table_id = table_name_to_id(catalog_id, table_name, txn);
        catalog.table.push(SimpleTableProto {
            name: Some(table_name.clone()),
            serialization_id: Some(table_id),
            column: table_columns(table_id, txn),
            ..Default::default()
        })
    }
    root_catalog
}

#[log::trace]
pub fn indexes(table_id: i64, txn: i64) -> Vec<Index> {
    if table_id < RESERVED_IDS {
        return vec![];
    }
    let mut variables = HashMap::new();
    variables.insert("table_id".to_string(), Value::I64(Some(table_id)));
    let sql = "select index_id, column_name from index join index_column using (index_id) join column using (table_id, column_id) where table_id = @table_id order by index_id, index_order";
    let mut batch = remote_execution::submit(sql, &variables, METADATA_CATALOG_ID, Some(txn));
    let mut indexes: Vec<Index> = vec![];
    let (_, index_id) = batch.columns.remove(0);
    let index_id = index_id.as_i64();
    let (_, column_name) = batch.columns.remove(0);
    let column_name = column_name.as_string();
    for i in 0..index_id.len() {
        let index_id = index_id.get(i).unwrap();
        let column_name = column_name.get(i).unwrap();
        match indexes.last_mut() {
            Some(index) if index.index_id == index_id => {
                index.columns.push(column_name.to_string())
            }
            _ => indexes.push(Index {
                table_id,
                index_id,
                columns: vec![column_name.to_string()],
            }),
        }
    }
    indexes
}

#[log::trace]
fn catalog_name_to_id(parent_catalog_id: i64, catalog_name: &String, txn: i64) -> i64 {
    let mut variables = HashMap::new();
    variables.insert(
        "parent_catalog_id".to_string(),
        Value::I64(Some(parent_catalog_id)),
    );
    variables.insert(
        "catalog_name".to_string(),
        Value::String(Some(catalog_name.clone())),
    );
    let sql = "select catalog_id from catalog where parent_catalog_id = @parent_catalog_id and catalog_name = @catalog_name";
    let mut batch = remote_execution::submit(sql, &variables, METADATA_CATALOG_ID, Some(txn));
    let (_, column) = batch.columns.remove(0);
    column.as_i64().get(0).unwrap()
}

#[log::trace]
fn table_name_to_id(catalog_id: i64, table_name: &String, txn: i64) -> i64 {
    let mut variables = HashMap::new();
    variables.insert("catalog_id".to_string(), Value::I64(Some(catalog_id)));
    variables.insert(
        "table_name".to_string(),
        Value::String(Some(table_name.clone())),
    );
    let sql =
        "select table_id from table where catalog_id = @catalog_id and table_name = @table_name";
    let mut batch = remote_execution::submit(sql, &variables, METADATA_CATALOG_ID, Some(txn));
    let (_, column) = batch.columns.remove(0);
    column.as_i64().get(0).unwrap()
}

#[log::trace]
fn table_columns(table_id: i64, txn: i64) -> Vec<SimpleColumnProto> {
    let mut variables = HashMap::new();
    variables.insert("table_id".to_string(), Value::I64(Some(table_id)));
    let sql = "select column_name, column_type from column where table_id = @table_id";
    let mut batch = remote_execution::submit(sql, &variables, METADATA_CATALOG_ID, Some(txn));
    let mut columns = vec![];
    let (_, column_name) = batch.columns.remove(0);
    let column_name = column_name.as_string();
    let (_, column_type) = batch.columns.remove(0);
    let column_type = column_type.as_string();
    for i in 0..column_name.len() {
        columns.push(SimpleColumnProto {
            name: Some(column_name.get(i).unwrap()),
            r#type: Some(DataType::from(column_type.get_str(i).unwrap()).to_proto()),
            ..Default::default()
        })
    }
    columns
}

fn find_or_push_catalog<'a>(
    parent_catalog: &'a mut SimpleCatalogProto,
    catalog_name: &String,
) -> &'a mut SimpleCatalogProto {
    for i in 0..parent_catalog.catalog.len() {
        if parent_catalog.catalog[i].name.as_ref() == Some(catalog_name) {
            return &mut parent_catalog.catalog[i];
        }
    }
    parent_catalog.catalog.push(SimpleCatalogProto {
        name: Some(catalog_name.clone()),
        ..Default::default()
    });
    parent_catalog.catalog.last_mut().unwrap()
}
