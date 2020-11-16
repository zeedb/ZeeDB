use arrow::array::*;
use arrow::record_batch::*;
use ast::data_type;
use execute::Execute;

pub struct CatalogProvider {
    parser: parser::ParseProvider,
}

impl CatalogProvider {
    pub fn new() -> Self {
        Self {
            parser: parser::ParseProvider::new(),
        }
    }

    pub fn catalog(
        &mut self,
        name: &String,
        storage: &storage::Storage,
    ) -> zetasql::SimpleCatalogProto {
        let mut root = fixtures::catalog();
        root.name = Some(name.clone());
        root.catalog.push(fixtures::bootstrap_metadata_catalog());
        let q = "
            select catalog_id, table_id, column_id, catalog_name, table_name, column_name, column_type
            from catalog 
            join table using (catalog_id) 
            join column using (table_id) 
            order by catalog_id, table_id, column_id"
            .to_string();
        let (_, expr) = self
            .parser
            .parse(&q, 0, fixtures::bootstrap_metadata_catalog())
            .unwrap();
        let expr = planner::optimize(expr);
        let results = expr.next(storage).unwrap();
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
        let mut row = 0;
        while row < results.num_rows() {
            let catalog_id = get_i64(&results, 0, row);
            let catalog_name = get_string(&results, 3, row);
            let mut catalog = fixtures::catalog();
            catalog.name = Some(catalog_name.to_string());
            while row < results.num_rows() && catalog_id == get_i64(&results, 0, row) {
                let table_id = get_i64(&results, 1, row);
                let table_name = get_string(&results, 4, row);
                let mut table = zetasql::SimpleTableProto {
                    name: Some(table_name.to_string()),
                    serialization_id: Some(table_id),
                    ..Default::default()
                };
                while row < results.num_rows() && table_id == get_i64(&results, 1, row) {
                    let _column_id = get_i64(&results, 2, row);
                    let column_name = get_string(&results, 5, row);
                    let column_type = get_string(&results, 6, row);
                    table.column.push(zetasql::SimpleColumnProto {
                        name: Some(column_name.to_string()),
                        r#type: Some(data_type::to_proto(&data_type::from_string(column_type))),
                        ..Default::default()
                    });
                    row += 1;
                }
                catalog.table.push(table);
            }
            root.catalog.push(catalog);
        }
        root
    }
}
