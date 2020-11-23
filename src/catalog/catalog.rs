use arrow::array::*;
use arrow::record_batch::*;
use ast::data_type;
use execute::*;
use std::collections::BTreeMap;
use zetasql::{SimpleCatalogProto, SimpleColumnProto, SimpleTableProto};

pub struct CatalogProvider {
    parser: parser::ParseProvider,
}

impl CatalogProvider {
    pub fn new() -> Self {
        Self {
            parser: parser::ParseProvider::new(),
        }
    }

    pub fn catalog(&mut self, storage: &storage::Storage) -> (i64, SimpleCatalogProto) {
        let q = "
            select parent_catalog_id, catalog_id, catalog_name, table_id, table_name, column_id, column_name, column_type
            from catalog 
            join table using (catalog_id) 
            join column using (table_id) 
            order by catalog_id, table_id, column_id"
            .to_string();
        let catalog = (bootstrap::ROOT_CATALOG_ID, bootstrap::metadata_zetasql());
        let expr = self.parser.analyze(&q, catalog).unwrap();
        let expr = planner::optimize(expr, &mut self.parser);
        let results = expr.start(storage).unwrap().next().unwrap();
        fn get_i64(results: &RecordBatch, column: usize, row: usize) -> i64 {
            results
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row)
        }
        fn get_string(results: &RecordBatch, column: usize, row: usize) -> &str {
            results
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
        }
        let mut catalogs: BTreeMap<(i64, i64), SimpleCatalogProto> = BTreeMap::new();
        let mut row = 0;
        while row < results.num_rows() {
            let parent_catalog_id = get_i64(&results, 0, row);
            let catalog_id = get_i64(&results, 1, row);
            let catalog_name = get_string(&results, 2, row);
            let mut catalog = bootstrap::catalog();
            catalog.name = Some(catalog_name.to_string());
            while row < results.num_rows() && catalog_id == get_i64(&results, 0, row) {
                let table_id = get_i64(&results, 3, row);
                let table_name = get_string(&results, 4, row);
                let mut table = SimpleTableProto {
                    name: Some(table_name.to_string()),
                    serialization_id: Some(table_id),
                    ..Default::default()
                };
                while row < results.num_rows() && table_id == get_i64(&results, 1, row) {
                    let column_id = get_i64(&results, 5, row);
                    let column_name = get_string(&results, 6, row);
                    let column_type = get_string(&results, 7, row);
                    table.column.push(SimpleColumnProto {
                        name: Some(column_name.to_string()),
                        r#type: Some(data_type::to_proto(&data_type::from_string(column_type))),
                        ..Default::default()
                    });
                    row += 1;
                }
                catalog.table.push(table);
            }
            catalogs.insert((parent_catalog_id, catalog_id), catalog);
        }
        let root_catalog = catalog_tree(
            bootstrap::ROOT_CATALOG_ID,
            bootstrap::catalog(),
            &mut catalogs,
        );
        (bootstrap::ROOT_CATALOG_ID, root_catalog)
    }
}

fn catalog_tree(
    parent_catalog_id: i64,
    mut parent_catalog: SimpleCatalogProto,
    descendents: &mut BTreeMap<(i64, i64), SimpleCatalogProto>,
) -> SimpleCatalogProto {
    let children: Vec<i64> = descendents
        .range((parent_catalog_id, i64::MIN)..=(parent_catalog_id, i64::MAX))
        .map(|((_, catalog_id), _)| *catalog_id)
        .collect();
    for catalog_id in children {
        let catalog = descendents
            .remove(&(parent_catalog_id, catalog_id))
            .unwrap();
        let child_catalog = catalog_tree(catalog_id, catalog, descendents);
        parent_catalog.catalog.push(child_catalog);
    }
    parent_catalog
}
