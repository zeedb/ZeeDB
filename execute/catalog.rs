use std::collections::{hash_map::Entry, HashMap};

use ast::{Index, *};
use catalog::{builtin_function_options, BootstrapCatalog, Catalog, CATALOG_KEY};
use context::Context;
use kernel::*;
use once_cell::sync::OnceCell;
use parser::{Parser, PARSER_KEY};
use statistics::{Statistics, STATISTICS_KEY};
use zetasql::{SimpleCatalogProto, SimpleColumnProto, SimpleTableProto};

pub struct MetadataCatalog;

impl Catalog for MetadataCatalog {
    fn catalog(
        &self,
        catalog_id: i64,
        table_names: Vec<Vec<String>>,
        txn: i64,
        context: &Context,
    ) -> SimpleCatalogProto {
        let mut root = CatalogWithId {
            catalog_id,
            simple_catalog: SimpleCatalogProto {
                builtin_function_options: Some(builtin_function_options()),
                ..Default::default()
            },
            children: HashMap::new(),
        };
        for qualified_name in table_names {
            add_table_to_catalog(&mut root, qualified_name, txn, context);
        }
        root.simplify()
    }

    fn indexes(&self, table_id: i64, txn: i64, context: &Context) -> Vec<Index> {
        select_indexes(table_id, txn, context)
    }
}

fn add_table_to_catalog(
    parent: &mut CatalogWithId,
    mut qualified_name: Vec<String>,
    txn: i64,
    context: &Context,
) {
    match qualified_name.len() {
        0 => panic!(),
        1 => {
            let table_name = qualified_name.pop().unwrap();
            if !parent
                .simple_catalog
                .table
                .iter()
                .find(|t| t.name.as_ref() == Some(&table_name))
                .is_some()
            {
                parent.simple_catalog.table.push(select_table(
                    parent.catalog_id,
                    &table_name,
                    txn,
                    context,
                ))
            }
        }
        _ => {
            let catalog_name = qualified_name.remove(0);
            match parent.children.entry(catalog_name) {
                Entry::Occupied(mut occupied) => {
                    let parent = occupied.get_mut();
                    add_table_to_catalog(parent, qualified_name, txn, context)
                }
                Entry::Vacant(vacant) => {
                    let value =
                        select_catalog(parent.catalog_id, vacant.key().as_str(), txn, context);
                    let parent = vacant.insert(value);
                    add_table_to_catalog(parent, qualified_name, txn, context);
                }
            };
        }
    }
}

fn select_catalog(
    parent_catalog_id: i64,
    catalog_name: &str,
    txn: i64,
    context: &Context,
) -> CatalogWithId {
    if parent_catalog_id == catalog::ROOT_CATALOG_ID && catalog_name == "metadata" {
        return CatalogWithId {
            catalog_id: catalog::METADATA_CATALOG_ID,
            simple_catalog: catalog::bootstrap_metadata_catalog(),
            children: HashMap::new(),
        };
    }
    const Q: PlanCache = PlanCache::new(
        "
        select catalog_id 
        from metadata.catalog
        where parent_catalog_id = @parent_catalog_id
        and catalog_name = @catalog_name",
    );
    let mut variables = HashMap::new();
    variables.insert(
        "parent_catalog_id".to_string(),
        AnyArray::I64(I64Array::from_values(vec![parent_catalog_id])),
    );
    variables.insert(
        "catalog_name".to_string(),
        AnyArray::String(StringArray::from_values(vec![catalog_name])),
    );
    let batches = execute(Q.get(variable_types(&variables)), txn, variables, context);
    let batch = match batches.first() {
        Some(first) => first,
        None => panic!(
            "No catalog {} in parent {}",
            catalog_name, parent_catalog_id
        ),
    };
    let catalog_id = as_i64(&batch, 0).get(0).unwrap();
    CatalogWithId {
        catalog_id,
        simple_catalog: SimpleCatalogProto {
            name: Some(catalog_name.to_string()),
            // builtin_function_options: Some(builtin_function_options()),
            ..Default::default()
        },
        children: HashMap::new(),
    }
}

fn select_table(
    catalog_id: i64,
    table_name: &str,
    txn: i64,
    context: &Context,
) -> SimpleTableProto {
    const Q: PlanCache = PlanCache::new(
        "
        select table_id, column_name, column_type
        from metadata.table
        join metadata.column using (table_id) 
        where catalog_id = @catalog_id and table_name = @table_name
        order by column_id",
    );
    let mut variables = HashMap::new();
    variables.insert(
        "catalog_id".to_string(),
        AnyArray::I64(I64Array::from_values(vec![catalog_id])),
    );
    variables.insert(
        "table_name".to_string(),
        AnyArray::String(StringArray::from_values(vec![table_name])),
    );
    let mut table = SimpleTableProto {
        name: Some(table_name.to_string()),
        ..Default::default()
    };
    for batch in execute(Q.get(variable_types(&variables)), txn, variables, context) {
        for offset in 0..batch.len() {
            let table_id = as_i64(&batch, 0).get(offset).unwrap();
            let column_name = as_string(&batch, 1).get(offset).unwrap();
            let column_type = as_string(&batch, 2).get(offset).unwrap();
            table.serialization_id = Some(table_id);
            table.column.push(SimpleColumnProto {
                name: Some(column_name.to_string()),
                r#type: Some(DataType::from(column_type).to_proto()),
                ..Default::default()
            })
        }
    }
    assert!(
        table.serialization_id.is_some(),
        "Table {}.{} not found in metadata.table",
        catalog_id,
        table_name
    );
    table
}

fn select_indexes(table_id: i64, txn: i64, context: &Context) -> Vec<Index> {
    const Q: PlanCache = PlanCache::new(
        "
        select index_id, column_name
        from metadata.index
        join metadata.index_column using (index_id)
        join metadata.column using (table_id, column_id)
        where table_id = @table_id
        order by index_id, index_order",
    );
    let mut variables = HashMap::new();
    variables.insert(
        "table_id".to_string(),
        AnyArray::I64(I64Array::from_values(vec![table_id])),
    );
    let mut indexes: Vec<Index> = vec![];
    for batch in execute(Q.get(variable_types(&variables)), txn, variables, context) {
        for offset in 0..batch.len() {
            let index_id = as_i64(&batch, 0).get(offset).unwrap();
            let column_name = as_string(&batch, 1).get(offset).unwrap();
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
    }
    indexes
}

fn execute(
    expr: Expr,
    txn: i64,
    variables: HashMap<String, AnyArray>,
    context: &Context,
) -> Vec<RecordBatch> {
    crate::execute::execute(expr, txn, variables, context).collect()
}

struct PlanCache {
    query: &'static str,
    plan: OnceCell<Expr>,
}

impl PlanCache {
    const fn new(query: &'static str) -> Self {
        Self {
            query,
            plan: OnceCell::new(),
        }
    }

    fn get(&self, variables: Vec<(String, DataType)>) -> Expr {
        self.plan
            .get_or_init(|| {
                let mut context = Context::default();
                context.insert(CATALOG_KEY, Box::new(BootstrapCatalog));
                context.insert(PARSER_KEY, Parser::default());
                context.insert(STATISTICS_KEY, Statistics::default());
                let parser = &context[PARSER_KEY];
                let expr =
                    parser.analyze(self.query, catalog::ROOT_CATALOG_ID, 0, variables, &context);
                planner::optimize(expr, 0, &context)
            })
            .clone()
    }
}

struct CatalogWithId {
    catalog_id: i64,
    simple_catalog: SimpleCatalogProto,
    children: HashMap<String, CatalogWithId>,
}

impl CatalogWithId {
    fn simplify(mut self) -> SimpleCatalogProto {
        for (_, child) in self.children {
            self.simple_catalog.catalog.push(child.simplify())
        }
        self.simple_catalog
    }
}

fn variable_types(variables: &HashMap<String, AnyArray>) -> Vec<(String, DataType)> {
    variables
        .iter()
        .map(|(name, value)| (name.clone(), value.data_type()))
        .collect()
}

fn as_string(batch: &RecordBatch, column: usize) -> &StringArray {
    match &batch.columns[column] {
        (_, AnyArray::String(array)) => array,
        _ => panic!(),
    }
}

fn as_i64(batch: &RecordBatch, column: usize) -> &I64Array {
    match &batch.columns[column] {
        (_, AnyArray::I64(array)) => array,
        (_, other) => panic!("Expected I64 but found {}", other.data_type()),
    }
}
