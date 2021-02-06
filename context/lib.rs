use std::{
    any::Any,
    collections::HashMap,
    marker::PhantomData,
    ops::{Index, IndexMut},
};

#[derive(Default)]
pub struct Context {
    store: HashMap<&'static str, Box<dyn Any>>,
}

pub struct ContextKey<T> {
    key: &'static str,
    phantom: PhantomData<T>,
}

impl Context {
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

impl<T: 'static> Index<ContextKey<T>> for Context {
    type Output = T;

    fn index(&self, key: ContextKey<T>) -> &Self::Output {
        self.store
            .get(key.key)
            .expect(key.key)
            .downcast_ref::<T>()
            .expect(key.key)
    }
}

impl<T: 'static> IndexMut<ContextKey<T>> for Context {
    fn index_mut(&mut self, key: ContextKey<T>) -> &mut Self::Output {
        self.store
            .get_mut(key.key)
            .expect(key.key)
            .downcast_mut::<T>()
            .unwrap()
    }
}