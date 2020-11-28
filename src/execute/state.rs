use arrow::array::Array;
use std::collections::HashMap;
use std::sync::Arc;
use storage::Storage;

#[derive(Debug)]
pub struct State<'a> {
    pub txn: u64,
    pub variables: HashMap<String, Arc<dyn Array>>,
    pub storage: &'a mut Storage,
}

impl<'a> State<'a> {
    pub fn new(txn: u64, storage: &mut Storage) -> State<'_> {
        State {
            txn,
            variables: HashMap::new(),
            storage,
        }
    }
}
