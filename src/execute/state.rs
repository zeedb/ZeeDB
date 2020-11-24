use std::any::Any;
use std::collections::HashMap;
use storage::Storage;

pub(crate) struct State<'a> {
    pub storage: &'a mut Storage,
    pub variables: HashMap<String, Box<dyn Any>>,
}

impl<'a> State<'a> {
    pub fn new(storage: &mut Storage) -> State<'_> {
        State {
            storage,
            variables: HashMap::new(),
        }
    }
}
