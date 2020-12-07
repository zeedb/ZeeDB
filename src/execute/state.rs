use arrow::array::Array;
use std::collections::HashMap;
use std::sync::Arc;
use storage::Storage;

pub struct State<'a> {
    pub storage: &'a mut Storage,
    pub variables: HashMap<String, Arc<dyn Array>>,
    pub txn: u64,
}
