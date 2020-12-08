use crate::execute;
use arrow::array::*;
use arrow::record_batch::*;
use ast::*;
use catalog::Index;
use once_cell::sync::OnceCell;
use planner::optimize;
use std::collections::HashMap;
use storage::Storage;
use zetasql::{SimpleCatalogProto, SimpleColumnProto, SimpleTableProto};

pub fn indexes(storage: &mut Storage, txn: u64) -> HashMap<i64, Vec<Index>> {
    let mut indexes = HashMap::new();
    for index in read_all_indexes(storage, txn) {
        if !indexes.contains_key(&index.table_id) {
            indexes.insert(index.table_id, vec![]);
        }
        indexes.get_mut(&index.table_id).unwrap().push(index);
    }
    indexes
}

pub fn catalog(storage: &mut Storage, txn: u64) -> SimpleCatalogProto {
    let mut root = catalog::default_catalog();
    // Find all tables and organize by catalog.
    let mut tables = HashMap::new();
    for (catalog_id, table) in read_all_tables(storage, txn) {
        if !tables.contains_key(&catalog_id) {
            tables.insert(catalog_id, vec![]);
        }
        tables.get_mut(&catalog_id).unwrap().push(table);
    }
    // Find all catalogs and organize by parent catalog.
    let mut catalogs = HashMap::new();
    for (parent_id, catalog_id, mut catalog) in read_all_catalogs(storage, txn) {
        if !catalogs.contains_key(&parent_id) {
            catalogs.insert(parent_id, vec![]);
        }
        // Add tables to catalog.
        catalog.table = tables.remove(&catalog_id).unwrap_or(vec![]);
        catalogs
            .get_mut(&parent_id)
            .unwrap()
            .push((catalog_id, catalog));
    }
    // Organize catalogs into a hierarchy.
    catalog_tree(catalog::ROOT_CATALOG_ID, &mut root, &mut catalogs);
    // Add tables to the root catalog.
    root.table = tables.remove(&catalog::ROOT_CATALOG_ID).unwrap_or(vec![]);

    root
}

fn catalog_tree(
    parent_catalog_id: i64,
    parent_catalog: &mut SimpleCatalogProto,
    descendents: &mut HashMap<i64, Vec<(i64, SimpleCatalogProto)>>,
) {
    for (catalog_id, mut catalog) in descendents.remove(&parent_catalog_id).unwrap_or(vec![]) {
        catalog_tree(catalog_id, &mut catalog, descendents);
        parent_catalog.catalog.push(catalog);
    }
}

fn read_all_catalogs(storage: &mut Storage, txn: u64) -> Vec<(i64, i64, SimpleCatalogProto)> {
    let expr = read_all_catalogs_query(storage);
    let program = crate::compile(expr);
    let mut catalogs = vec![];
    for batch_or_err in program.execute(storage, txn) {
        let batch = batch_or_err.unwrap();
        let mut offset = 0;
        while offset < batch.num_rows() {
            catalogs.push(read_catalog(&batch, &mut offset));
        }
    }

    catalogs
}

fn read_all_catalogs_query(storage: &mut Storage) -> Expr {
    let q = "
        select parent_catalog_id, catalog_id, catalog_name 
        from metadata.catalog";
    static CACHE: OnceCell<Expr> = OnceCell::new();
    CACHE.get_or_init(|| plan_query(storage, q)).clone()
}

fn read_catalog(batch: &RecordBatch, offset: &mut usize) -> (i64, i64, SimpleCatalogProto) {
    let parent_catalog_id = kernel::coerce::<Int64Array>(batch.column(0)).value(*offset);
    let catalog_id_column = kernel::coerce::<Int64Array>(batch.column(1));
    let catalog_id = catalog_id_column.value(*offset);
    let catalog_name = kernel::coerce::<StringArray>(batch.column(2)).value(*offset);

    let mut catalog = catalog::empty_catalog();
    catalog.name = Some(catalog_name.to_string());
    *offset += 1;

    (parent_catalog_id, catalog_id, catalog)
}

fn read_all_tables(storage: &mut Storage, txn: u64) -> Vec<(i64, SimpleTableProto)> {
    let expr = read_all_tables_query(storage);
    let program = crate::compile(expr);
    let mut tables = vec![];
    for batch_or_err in program.execute(storage, txn) {
        let batch = batch_or_err.unwrap();
        let mut offset = 0;
        while offset < batch.num_rows() {
            tables.push(read_table(&batch, &mut offset));
        }
    }

    tables
}

fn read_all_tables_query(storage: &mut Storage) -> Expr {
    let q = "
        select catalog_id, table_id, table_name, column_id, column_name, column_type
        from metadata.table
        join metadata.column using (table_id) 
        order by catalog_id, table_id, column_id";
    static CACHE: OnceCell<Expr> = OnceCell::new();
    CACHE.get_or_init(|| plan_query(storage, q)).clone()
}

fn read_table(batch: &RecordBatch, offset: &mut usize) -> (i64, SimpleTableProto) {
    let catalog_id = kernel::coerce::<Int64Array>(batch.column(0)).value(*offset);
    let table_id_column = kernel::coerce::<Int64Array>(batch.column(1));
    let table_id = table_id_column.value(*offset);
    let table_name = kernel::coerce::<StringArray>(batch.column(2)).value(*offset);

    let mut table = SimpleTableProto {
        name: Some(table_name.to_string()),
        serialization_id: Some(table_id),
        ..Default::default()
    };

    while *offset < batch.num_rows() && table_id == table_id_column.value(*offset) {
        table.column.push(read_column(batch, offset));
    }

    (catalog_id, table)
}

fn read_column(batch: &RecordBatch, offset: &mut usize) -> SimpleColumnProto {
    let _column_id = kernel::coerce::<Int64Array>(batch.column(3)).value(*offset);
    let column_name = kernel::coerce::<StringArray>(batch.column(4)).value(*offset);
    let column_type = kernel::coerce::<StringArray>(batch.column(5)).value(*offset);

    *offset += 1;

    SimpleColumnProto {
        name: Some(column_name.to_string()),
        r#type: Some(data_type::to_proto(&data_type::from_string(column_type))),
        ..Default::default()
    }
}

fn read_all_indexes(storage: &mut Storage, txn: u64) -> Vec<Index> {
    let expr = read_all_indexes_query(storage);
    let program = crate::compile(expr);
    let program: Vec<_> = program.execute(storage, txn).collect();
    let mut indexes = vec![];
    for batch_or_err in program {
        let batch = batch_or_err.unwrap();
        let mut offset = 0;
        while offset < batch.num_rows() {
            indexes.push(read_index(&batch, &mut offset));
        }
    }

    indexes
}

fn read_all_indexes_query(storage: &mut Storage) -> Expr {
    let q = "
        select index_id, table_id, column_name
        from metadata.index
        join metadata.index_column using (index_id)
        join metadata.column using (table_id, column_id)
        order by index_id, index_order";
    static CACHE: OnceCell<Expr> = OnceCell::new();
    CACHE.get_or_init(|| plan_query(storage, q)).clone()
}

fn read_index(batch: &RecordBatch, offset: &mut usize) -> Index {
    let index_id_column = kernel::coerce::<Int64Array>(batch.column(0));
    let index_id = index_id_column.value(*offset);
    let table_id = kernel::coerce::<Int64Array>(batch.column(1)).value(*offset);
    let column_name_column = kernel::coerce::<StringArray>(batch.column(2));

    let mut index = Index {
        index_id,
        table_id,
        columns: vec![],
    };

    while *offset < batch.num_rows() && index_id == index_id_column.value(*offset) {
        let column_name = column_name_column.value(*offset).to_string();
        index.columns.push(column_name);
        *offset += 1;
    }

    index
}

fn plan_query(storage: &mut Storage, q: &str) -> Expr {
    let catalog = catalog::default_catalog();
    let indexes = HashMap::new();
    let expr = parser::analyze(catalog::ROOT_CATALOG_ID, &catalog, q).unwrap();
    optimize(catalog::ROOT_CATALOG_ID, &catalog, &indexes, &storage, expr)
}
