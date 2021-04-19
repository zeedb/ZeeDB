use std::{any::Any, collections::HashMap, marker::PhantomData, ops::Index};

/// Context holds references to shared components that can call each other.
/// It allows us to avoid passing a zillion arguments to every top-level function.
/// The members of Context should be long-lived, globally-scoped components, for example the parser.
#[derive(Default)]
pub struct Context {
    store: HashMap<&'static str, Box<dyn Any + Send + Sync>>,
}

pub struct ContextKey<T> {
    key: &'static str,
    phantom: PhantomData<T>,
}

impl Context {
    pub fn insert<T: 'static + Send + Sync>(&mut self, key: ContextKey<T>, any: T) {
        if self.store.insert(key.key, Box::new(any)).is_some() {
            panic!("{} is already present", key.key)
        }
    }

    pub fn remove<T: 'static + Send + Sync>(&mut self, key: ContextKey<T>) -> T {
        let b: Box<dyn Any> = self.store.remove(key.key).expect(key.key);
        *b.downcast::<T>().unwrap()
    }
}

impl<T> ContextKey<T> {
    pub const fn new(key: &'static str) -> Self {
        Self {
            key,
            phantom: PhantomData,
        }
    }
}

impl<T: 'static + Send + Sync> Index<ContextKey<T>> for Context {
    type Output = T;

    fn index(&self, key: ContextKey<T>) -> &Self::Output {
        self.store
            .get(key.key)
            .expect(key.key)
            .downcast_ref::<T>()
            .expect(key.key)
    }
}

/// ID of the current worker. Dense integers in range `0..context[WORKER_COUNT_KEY]`.
pub const WORKER_ID_KEY: ContextKey<i32> = ContextKey::new("WORKER_ID_KEY");

/// Number of workers in the current cluster.
pub const WORKER_COUNT_KEY: ContextKey<i32> = ContextKey::new("WORKER_COUNT_KEY");

pub fn env_var(key: &str) -> i32 {
    std::env::var(key).expect(key).parse().unwrap()
}
