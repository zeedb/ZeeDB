use ast::Index;
use context::{Context, ContextKey};
use zetasql::*;

pub const ROOT_CATALOG_ID: i64 = 0;
pub const METADATA_CATALOG_ID: i64 = 1;
pub const CATALOG_KEY: ContextKey<Box<dyn Catalog>> = ContextKey::new("CATALOG");

pub trait Catalog: Send + Sync {
    fn catalog(
        &self,
        catalog_id: i64,
        table_names: Vec<Vec<String>>,
        txn: i64,
        context: &Context,
    ) -> SimpleCatalogProto;

    fn indexes(&self, table_id: i64, txn: i64, context: &Context) -> Vec<Index>;
}
