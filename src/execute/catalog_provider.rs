use crate::execute;
use arrow::array::*;
use arrow::record_batch::*;
use ast::data_type;
use catalog::{Catalog, Index, Statistics};
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
        let mut root = Catalog::empty();
        root.catalog_id = catalog::ROOT_CATALOG_ID;
        // Find all tables and organize by catalog.
        let mut tables = HashMap::new();
        for (catalog_id, table) in self.read_all_tables(txn, &root, storage) {
            if !tables.contains_key(&catalog_id) {
                tables.insert(catalog_id, vec![]);
            }
            tables.get_mut(&catalog_id).unwrap().push(table);
        }
        // Find all catalogs and organize by parent catalog.
        let mut catalogs = HashMap::new();
        for (parent_id, catalog_id, mut catalog) in self.read_all_catalogs(txn, &root, storage) {
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
        Self::catalog_tree(root.catalog_id, &mut root.catalog, &mut catalogs);
        // Add tables to the root catalog.
        root.catalog.table = tables.remove(&catalog::ROOT_CATALOG_ID).unwrap_or(vec![]);
        // Add statistics.
        for table_stats in self.read_all_statistics(txn, &root, storage) {
            root.statistics.insert(table_stats.table_id, table_stats);
        }
        // Add indexes.
        for index in self.read_all_indexes(txn, &root, storage) {
            if !root.indexes.contains_key(&index.table_id) {
                root.indexes.insert(index.table_id, vec![]);
            }
            root.indexes.get_mut(&index.table_id).unwrap().push(index);
        }
        root
    }

    fn catalog_tree(
        parent_catalog_id: i64,
        parent_catalog: &mut SimpleCatalogProto,
        descendents: &mut HashMap<i64, Vec<(i64, SimpleCatalogProto)>>,
    ) {
        for (catalog_id, mut catalog) in descendents.remove(&parent_catalog_id).unwrap_or(vec![]) {
            Self::catalog_tree(catalog_id, &mut catalog, descendents);
            parent_catalog.catalog.push(catalog);
        }
    }

    fn read_all_catalogs(
        &mut self,
        txn: u64,
        catalog: &Catalog,
        storage: &mut storage::Storage,
    ) -> Vec<(i64, i64, SimpleCatalogProto)> {
        let q = "
                select parent_catalog_id, catalog_id, catalog_name
                from metadata.catalog"
            .to_string();
        let expr = self.parser.analyze(&q, &catalog).unwrap();
        let expr = optimize(expr, catalog, &mut self.parser);
        let program = execute(expr, txn, catalog, storage);

        let mut catalogs = vec![];
        for batch_or_err in program {
            let batch = batch_or_err.unwrap();
            let mut offset = 0;
            while offset < batch.num_rows() {
                catalogs.push(Self::read_catalog(&batch, &mut offset));
            }
        }

        catalogs
    }

    fn read_catalog(batch: &RecordBatch, offset: &mut usize) -> (i64, i64, SimpleCatalogProto) {
        let parent_catalog_id = kernel::coerce::<Int64Array>(batch.column(0)).value(*offset);
        let catalog_id_column = kernel::coerce::<Int64Array>(batch.column(1));
        let catalog_id = catalog_id_column.value(*offset);
        let catalog_name = kernel::coerce::<StringArray>(batch.column(2)).value(*offset);

        let mut catalog = Catalog::zetasql();
        catalog.name = Some(catalog_name.to_string());
        *offset += 1;

        (parent_catalog_id, catalog_id, catalog)
    }

    fn read_all_tables(
        &mut self,
        txn: u64,
        catalog: &Catalog,
        storage: &mut storage::Storage,
    ) -> Vec<(i64, SimpleTableProto)> {
        let q = "
            select catalog_id, table_id, table_name, column_id, column_name, column_type
            from metadata.table
            join metadata.column using (table_id) 
            order by catalog_id, table_id, column_id"
            .to_string();
        let expr = self.parser.analyze(&q, &catalog).unwrap();
        let expr = optimize(expr, catalog, &mut self.parser);
        let program = execute(expr, txn, catalog, storage);

        let mut tables = vec![];
        for batch_or_err in program {
            let batch = batch_or_err.unwrap();
            let mut offset = 0;
            while offset < batch.num_rows() {
                tables.push(Self::read_table(&batch, &mut offset));
            }
        }

        tables
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
            table.column.push(Self::read_column(batch, offset));
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

    fn read_all_statistics(
        &mut self,
        txn: u64,
        catalog: &Catalog,
        storage: &mut storage::Storage,
    ) -> Vec<Statistics> {
        let q = "
            select table_id, table_cardinality, column_name, column_unique_cardinality
            from metadata.table
            join metadata.column using (table_id)
            order by table_id"
            .to_string();
        let expr = self.parser.analyze(&q, &catalog).unwrap();
        let expr = optimize(expr, catalog, &mut self.parser);
        let program = execute(expr, txn, catalog, storage);

        let mut statistics = vec![];
        for batch_or_err in program {
            let batch = batch_or_err.unwrap();
            let mut offset = 0;
            while offset < batch.num_rows() {
                statistics.push(Self::read_statistics(&batch, &mut offset));
            }
        }

        statistics
    }

    fn read_statistics(batch: &RecordBatch, offset: &mut usize) -> Statistics {
        let table_id_column = kernel::coerce::<Int64Array>(batch.column(0));
        let table_id = table_id_column.value(*offset);
        let table_cardinality = kernel::coerce::<Int64Array>(batch.column(1)).value(0);
        let column_name_column = kernel::coerce::<StringArray>(batch.column(2));
        let column_unique_cardinality_column = kernel::coerce::<Int64Array>(batch.column(3));

        let mut stats = Statistics {
            table_id,
            cardinality: table_cardinality as usize,
            column_unique_cardinality: HashMap::new(),
        };
        while *offset < batch.num_rows() && table_id == table_id_column.value(*offset) {
            let column_name = column_name_column.value(*offset).to_string();
            stats.column_unique_cardinality.insert(
                column_name,
                column_unique_cardinality_column.value(*offset) as usize,
            );
            *offset += 1;
        }

        stats
    }

    fn read_all_indexes(
        &mut self,
        txn: u64,
        catalog: &Catalog,
        storage: &mut storage::Storage,
    ) -> Vec<Index> {
        let q = "
            select index_id, table_id, column_name
            from metadata.index
            join metadata.index_column using (index_id)
            join metadata.column using (table_id, column_id)
            order by index_id, index_order"
            .to_string();
        let expr = self.parser.analyze(&q, &catalog).unwrap();
        let expr = optimize(expr, catalog, &mut self.parser);
        let program = execute(expr, txn, catalog, storage);

        let mut indexes = vec![];
        for batch_or_err in program {
            let batch = batch_or_err.unwrap();
            let mut offset = 0;
            while offset < batch.num_rows() {
                indexes.push(Self::read_index(&batch, &mut offset));
            }
        }

        indexes
    }

    fn read_index(batch: &RecordBatch, offset: &mut usize) -> Index {
        let index_id_column = kernel::coerce::<Int64Array>(batch.column(0));
        let index_id = index_id_column.value(*offset);
        let table_id = kernel::coerce::<Int64Array>(batch.column(1)).value(0);
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
}
