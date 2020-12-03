use arrow::array::Array;
use catalog::Catalog;
use std::collections::HashMap;
use std::sync::Arc;
use storage::Storage;

pub struct State<'a> {
    pub txn: u64,
    pub catalog: &'a Catalog,
    pub variables: HashMap<String, Arc<dyn Array>>,
    pub storage: &'a mut Storage,
}

impl<'a> State<'a> {
    pub fn new(txn: u64, catalog: &'a Catalog, storage: &'a mut Storage) -> State<'a> {
        State {
            txn,
            variables: HashMap::new(),
            catalog,
            storage,
        }
    }
}
