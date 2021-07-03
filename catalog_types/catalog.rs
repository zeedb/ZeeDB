use ast::Index;
use zetasql::*;

pub const ROOT_CATALOG_ID: i64 = 0;
pub const METADATA_CATALOG_ID: i64 = 1;

pub trait Catalog: Send + Sync {
    fn catalog_id(&self) -> i64;

    fn simple_catalog(&self, table_names: Vec<Vec<String>>) -> SimpleCatalogProto;

    fn indexes(&self, table_id: i64) -> Vec<Index>;
}
