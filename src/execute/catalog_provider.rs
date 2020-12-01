use crate::execute;
use arrow::array::*;
use arrow::record_batch::*;
use ast::data_type;
use catalog::Catalog;
use parser::ParseProvider;
use planner::optimize;
use std::collections::{BTreeMap, HashMap};
use zetasql::{SimpleCatalogProto, SimpleColumnProto, SimpleTableProto};

pub struct CatalogProvider {
    parser: ParseProvider,
}

impl CatalogProvider {
    pub fn new() -> Self {
        Self {
            parser: ParseProvider::new(),
        }
    }

    pub fn catalog(&mut self, txn: u64, storage: &mut storage::Storage) -> Catalog {
        let mut all_catalogs: BTreeMap<(i64, i64), SimpleCatalogProto> = BTreeMap::new();
        let bootstrap_catalog = catalog::bootstrap();
        // Find all tables and the catalogs they are members of.
        let q = "
            select parent_catalog_id, catalog_id, catalog_name, table_id, table_name, column_id, column_name, column_type
            from catalog 
            join table using (catalog_id) 
            join column using (table_id) 
            order by catalog_id, table_id, column_id"
            .to_string();
        let expr = optimize(
            self.parser.analyze(&q, &bootstrap_catalog).unwrap(),
            &bootstrap_catalog,
            &mut self.parser,
        );
        let program = execute(expr, txn, storage);
        for batch_or_err in program {
            let batch = batch_or_err.unwrap();
            for (parent_catalog_id, catalog_id, catalog) in read_catalogs(&batch) {
                all_catalogs.insert((parent_catalog_id, catalog_id), catalog);
            }
        }
        // Find any catalogs that have no tables in them.
        let q = "
            select parent_catalog_id, catalog_id, catalog_name
            from catalog"
            .to_string();
        let expr = optimize(
            self.parser.analyze(&q, &bootstrap_catalog).unwrap(),
            &bootstrap_catalog,
            &mut self.parser,
        );
        let program = execute(expr, txn, storage);
        for batch_or_err in program {
            let batch = batch_or_err.unwrap();
            for offset in 0..batch.num_rows() {
                let parent_catalog_id = kernel::coerce::<Int64Array>(batch.column(0)).value(offset);
                let catalog_id = kernel::coerce::<Int64Array>(batch.column(1)).value(offset);
                let catalog_name = kernel::coerce::<StringArray>(batch.column(2)).value(offset);
                if !all_catalogs.contains_key(&(parent_catalog_id, catalog_id)) {
                    let mut catalog = catalog::empty();
                    catalog.name = Some(catalog_name.to_string());
                    all_catalogs.insert((parent_catalog_id, catalog_id), catalog);
                }
            }
        }
        // Arrange catalogs into a tree data structure.
        let mut root = all_catalogs
            .remove(&(catalog::ROOT_CATALOG_PARENT_ID, catalog::ROOT_CATALOG_ID))
            .unwrap_or(catalog::empty());
        catalog_tree(catalog::ROOT_CATALOG_ID, &mut root, &mut all_catalogs);
        assert!(all_catalogs.is_empty());

        Catalog {
            catalog_id: catalog::ROOT_CATALOG_ID,
            catalog: root,
            indexes: HashMap::new(),
        }
    }
}

fn read_catalogs(batch: &RecordBatch) -> Vec<(i64, i64, SimpleCatalogProto)> {
    let mut offset = 0;
    let mut catalogs = vec![];
    while offset < batch.num_rows() {
        catalogs.push(read_catalog(batch, &mut offset));
    }
    catalogs
}

fn read_catalog(batch: &RecordBatch, offset: &mut usize) -> (i64, i64, SimpleCatalogProto) {
    let parent_catalog_id = kernel::coerce::<Int64Array>(batch.column(0)).value(*offset);
    let catalog_id_column = kernel::coerce::<Int64Array>(batch.column(1));
    let catalog_id = catalog_id_column.value(*offset);
    let catalog_name = kernel::coerce::<StringArray>(batch.column(2)).value(*offset);

    let mut catalog = catalog::empty();
    catalog.name = Some(catalog_name.to_string());

    while *offset < batch.num_rows() && catalog_id == catalog_id_column.value(*offset) {
        catalog.table.push(read_table(batch, offset))
    }

    (parent_catalog_id, catalog_id, catalog)
}

fn read_table(batch: &RecordBatch, offset: &mut usize) -> SimpleTableProto {
    let table_id_column = kernel::coerce::<Int64Array>(batch.column(3));
    let table_id = table_id_column.value(*offset);
    let table_name = kernel::coerce::<StringArray>(batch.column(4)).value(*offset);

    let mut table = SimpleTableProto {
        name: Some(table_name.to_string()),
        serialization_id: Some(table_id),
        ..Default::default()
    };

    while *offset < batch.num_rows() && table_id == table_id_column.value(*offset) {
        table.column.push(read_column(batch, offset));
    }

    table
}

fn read_column(batch: &RecordBatch, offset: &mut usize) -> SimpleColumnProto {
    let _column_id = kernel::coerce::<Int64Array>(batch.column(5)).value(*offset);
    let column_name = kernel::coerce::<StringArray>(batch.column(6)).value(*offset);
    let column_type = kernel::coerce::<StringArray>(batch.column(7)).value(*offset);

    *offset += 1;

    SimpleColumnProto {
        name: Some(column_name.to_string()),
        r#type: Some(data_type::to_proto(&data_type::from_string(column_type))),
        ..Default::default()
    }
}

fn catalog_tree(
    parent_catalog_id: i64,
    parent_catalog: &mut SimpleCatalogProto,
    descendents: &mut BTreeMap<(i64, i64), SimpleCatalogProto>,
) {
    let children: Vec<i64> = descendents
        .range((parent_catalog_id, i64::MIN)..=(parent_catalog_id, i64::MAX))
        .map(|((_, catalog_id), _)| *catalog_id)
        .collect();
    for catalog_id in children {
        let mut catalog = descendents
            .remove(&(parent_catalog_id, catalog_id))
            .unwrap();
        catalog_tree(catalog_id, &mut catalog, descendents);
        parent_catalog.catalog.push(catalog);
    }
}
