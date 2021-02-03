use std::{any::Any, collections::HashMap, marker::PhantomData};

#[derive(Default)]
pub struct Context {
    store: HashMap<&'static str, Box<dyn Any>>,
}

pub struct ContextKey<T> {
    key: &'static str,
    phantom: PhantomData<T>,
}

impl Context {
    pub fn get<T: 'static>(&self, key: ContextKey<T>) -> &T {
        self.store
            .get(key.key)
            .expect(key.key)
            .downcast_ref::<T>()
            .expect(key.key)
    }

    pub fn get_mut<T: 'static>(&mut self, key: ContextKey<T>) -> &mut T {
        self.store
            .get_mut(key.key)
            .expect(key.key)
            .downcast_mut::<T>()
            .unwrap()
    }

    pub fn insert<T: 'static>(&mut self, key: ContextKey<T>, any: T) {
        if self.store.insert(key.key, Box::new(any)).is_some() {
            panic!("{} is already present", key.key)
        }
    }

    pub fn remove<T: 'static>(&mut self, key: ContextKey<T>) -> T {
        *self.store.remove(key.key).unwrap().downcast::<T>().unwrap()
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
