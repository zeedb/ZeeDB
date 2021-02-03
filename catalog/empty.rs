use crate::{builtin_function_options, Catalog};
use ast::Index;
use context::Context;
use zetasql::SimpleCatalogProto;

pub struct EmptyCatalog;

impl Catalog for EmptyCatalog {
    fn catalog(
        &self,
        catalog_id: i64,
        table_names: Vec<Vec<String>>,
        txn: i64,
        context: &Context,
    ) -> SimpleCatalogProto {
        SimpleCatalogProto {
            builtin_function_options: Some(builtin_function_options()),
            ..Default::default()
        }
    }

    fn indexes(&self, table_id: i64, txn: i64, context: &Context) -> Vec<Index> {
        vec![]
    }
}
