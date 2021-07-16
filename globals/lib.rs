use std::{cell::RefCell, fmt::Debug, thread::LocalKey};

thread_local!(pub static WORKER: RefCell<Option<i32>> = Default::default());

pub trait Global<T> {
    type Session;

    fn set(&'static self, value: T) -> Self::Session;
    fn get(&'static self) -> T;
}

impl<T: 'static + Copy + Debug> Global<T> for LocalKey<RefCell<Option<T>>> {
    type Session = UnsetLocalKey<T>;

    fn set(&'static self, new: T) -> Self::Session {
        self.with(|value| {
            if let Some(replaced) = value.borrow_mut().replace(new) {
                panic!(
                    "Thread-local global variable was already set to {:?}",
                    replaced
                )
            }
        });
        UnsetLocalKey { key: self }
    }

    fn get(&'static self) -> T {
        self.with(|value| {
            value
                .borrow()
                .expect("Thread-local global variable is not set")
        })
    }
}

pub struct UnsetLocalKey<T: 'static> {
    key: &'static LocalKey<RefCell<Option<T>>>,
}

impl<T> Drop for UnsetLocalKey<T> {
    fn drop(&mut self) {
        self.key.with(|value| {
            value
                .borrow_mut()
                .take()
                .expect("Thread-local global variable cannot be unset because it is not set")
        });
    }
}

#[test]
fn test() {
    let _unset = WORKER.set(1);
    assert_eq!(1, WORKER.get());
}
