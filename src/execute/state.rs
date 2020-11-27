use arrow::array::Array;
use std::collections::HashMap;
use std::sync::Arc;
use storage::Storage;

#[derive(Debug)]
pub struct State<'a> {
    pub storage: &'a mut Storage,
    pub variables: HashMap<String, Arc<dyn Array>>,
    pub txn: u64,
}

impl<'a> State<'a> {
    pub fn new(storage: &mut Storage) -> State<'_> {
        State {
            storage,
            variables: HashMap::new(),
            txn: 100, // TODO
        }
    }
}
