use std::collections::HashMap;

use ast::{Index, *};
use defaults::{builtin_function_options, builtin_named_types, METADATA_CATALOG_ID, RESERVED_IDS};
use kernel::{Array, DataType, Next, RecordBatch};
use once_cell::sync::OnceCell;
use zetasql::{AnyResolvedStatementProto, SimpleCatalogProto, SimpleColumnProto, SimpleTableProto};

#[derive(Hash, PartialEq, Eq)]
pub enum SimpleCatalogProvider {
    MetadataCatalog,
    UserCatalog(UserCatalog),
}

#[derive(Hash, PartialEq, Eq, Default)]
pub struct UserCatalog {
    name: String,
    tables: Vec<UserTable>,
    catalogs: Vec<UserCatalog>,
}

#[derive(Hash, PartialEq, Eq)]
struct UserTable {
    id: i64,
    name: String,
    columns: Vec<UserColumn>,
}

#[derive(Hash, PartialEq, Eq)]
struct UserColumn {
    name: String,
    data_type: DataType,
}

impl SimpleCatalogProvider {
    pub fn to_proto(&self) -> SimpleCatalogProto {
        match self {
            SimpleCatalogProvider::MetadataCatalog => {
                crate::bootstrap::bootstrap_metadata_catalog()
            }
            SimpleCatalogProvider::UserCatalog(catalog) => catalog.to_proto(),
        }
    }
}

impl UserCatalog {
    fn to_proto(&self) -> SimpleCatalogProto {
        SimpleCatalogProto {
            name: Some(self.name.clone()),
            catalog: self.catalogs.iter().map(UserCatalog::to_proto).collect(),
            table: self.tables.iter().map(UserTable::to_proto).collect(),
            builtin_function_options: Some(builtin_function_options()),
            named_type: builtin_named_types(),
            ..Default::default()
        }
    }
}

impl UserTable {
    fn to_proto(&self) -> SimpleTableProto {
        SimpleTableProto {
            name: Some(self.name.clone()),
            serialization_id: Some(self.id),
            column: self.columns.iter().map(UserColumn::to_proto).collect(),
            ..Default::default()
        }
    }
}

impl UserColumn {
    fn to_proto(&self) -> SimpleColumnProto {
        SimpleColumnProto {
            name: Some(self.name.clone()),
            r#type: Some(self.data_type.to_proto()),
            ..Default::default()
        }
    }
}

#[log::trace]
pub fn simple_catalog(
    table_names: Vec<Vec<String>>,
    catalog_id: i64,
    txn: i64,
) -> SimpleCatalogProvider {
    if catalog_id == METADATA_CATALOG_ID {
        return SimpleCatalogProvider::MetadataCatalog;
    }
    let mut catalog_id_cache = HashMap::new();
    let mut root_catalog = UserCatalog::default();
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
        if let Some(table_id) = table_name_to_id(catalog_id, table_name, txn) {
            catalog.tables.push(UserTable {
                id: table_id,
                name: table_name.clone(),
                columns: table_columns(table_id, txn),
            })
        }
    }
    SimpleCatalogProvider::UserCatalog(root_catalog)
}

// TODO ideally this would include the convert and optimize phase, but it can't because right now we inline variables.
macro_rules! analyze_once {
    ($sql:literal, $variables:expr, $txn:expr) => {{
        static STATEMENTS: OnceCell<Vec<AnyResolvedStatementProto>> = OnceCell::new();
        STATEMENTS.get_or_init(|| analyze($sql, $variables, $txn))
    }};
}

fn analyze(
    sql: &str,
    variables: &HashMap<String, Value>,
    txn: i64,
) -> Vec<AnyResolvedStatementProto> {
    crate::parser::analyze(sql, variables, METADATA_CATALOG_ID, txn).unwrap()
}

#[log::trace]
pub fn indexes(table_id: i64, txn: i64) -> Vec<Index> {
    if table_id < RESERVED_IDS {
        return vec![];
    }
    let mut variables = HashMap::new();
    variables.insert("table_id".to_string(), Value::I64(Some(table_id)));
    let statements = analyze_once!("select index_id, column_name from index join index_column using (index_id) join column using (table_id, column_id) where table_id = @table_id order by index_id, index_order", &variables, txn);
    let mut batch = execute_on_coordinator(statements, variables, METADATA_CATALOG_ID, txn);
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
    let statements = analyze_once!("select catalog_id from catalog where parent_catalog_id = @parent_catalog_id and catalog_name = @catalog_name", &variables, txn);
    let mut batch = execute_on_coordinator(statements, variables, METADATA_CATALOG_ID, txn);
    let (_, column) = batch.columns.remove(0);
    column.as_i64().get(0).unwrap()
}

#[log::trace]
fn table_name_to_id(catalog_id: i64, table_name: &String, txn: i64) -> Option<i64> {
    let mut variables = HashMap::new();
    variables.insert("catalog_id".to_string(), Value::I64(Some(catalog_id)));
    variables.insert(
        "table_name".to_string(),
        Value::String(Some(table_name.clone())),
    );
    let statements = analyze_once!(
        "select table_id from table where catalog_id = @catalog_id and table_name = @table_name",
        &variables,
        txn
    );
    let mut batch = execute_on_coordinator(statements, variables, METADATA_CATALOG_ID, txn);
    let (_, column) = batch.columns.remove(0);
    column.as_i64().get(0)
}

#[log::trace]
fn table_columns(table_id: i64, txn: i64) -> Vec<UserColumn> {
    let mut variables = HashMap::new();
    variables.insert("table_id".to_string(), Value::I64(Some(table_id)));
    let statements = analyze_once!(
        "select column_name, column_type from column where table_id = @table_id",
        &variables,
        txn
    );
    let mut batch = execute_on_coordinator(statements, variables, METADATA_CATALOG_ID, txn);
    let mut columns = vec![];
    let (_, column_name) = batch.columns.remove(0);
    let column_name = column_name.as_string();
    let (_, column_type) = batch.columns.remove(0);
    let column_type = column_type.as_string();
    for i in 0..column_name.len() {
        columns.push(UserColumn {
            name: column_name.get(i).unwrap(),
            data_type: DataType::from(column_type.get_str(i).unwrap()),
        })
    }
    columns
}

fn find_or_push_catalog<'a>(
    parent_catalog: &'a mut UserCatalog,
    catalog_name: &String,
) -> &'a mut UserCatalog {
    for i in 0..parent_catalog.catalogs.len() {
        if &parent_catalog.catalogs[i].name == catalog_name {
            return &mut parent_catalog.catalogs[i];
        }
    }
    parent_catalog.catalogs.push(UserCatalog {
        name: catalog_name.clone(),
        ..Default::default()
    });
    parent_catalog.catalogs.last_mut().unwrap()
}

/// Catalog acts as its own little coordinator to avoid once RPC hop and to make caching metadata queries straightforward.
#[log::trace]
fn execute_on_coordinator(
    statements: &Vec<AnyResolvedStatementProto>,
    variables: HashMap<String, Value>,
    catalog_id: i64,
    txn: i64,
) -> RecordBatch {
    let expr = crate::convert::convert(statements, variables, catalog_id);
    let expr = crate::optimize::optimize(expr, txn);
    let schema = expr.schema();
    let batches = execute(expr, txn);
    RecordBatch::cat(batches).unwrap_or_else(|| RecordBatch::empty(schema))
}

#[log::trace]
fn execute(expr: Expr, txn: i64) -> Vec<RecordBatch> {
    let mut stream = remote_execution::gather(&expr, txn, 0);
    let mut batches = vec![];
    loop {
        match stream.next() {
            Next::Page(batch) => batches.push(batch),
            Next::Error(message) => panic!("{}", message),
            Next::End => return batches,
        }
    }
}
