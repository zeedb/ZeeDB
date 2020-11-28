use std::ops::Deref;
use std::sync::Arc;

/// Leak an Arc into a raw pointer.
/// The reference counts of the Arc will not be changed.
pub fn leak<T>(arc: &Arc<T>) -> *const T {
    Arc::as_ptr(arc)
}

/// Read an Arc that was previously leaked.
/// You are responsible for ensuring the Arc still exists.
/// The strong count of the Arc will be incremented by 1, until the returned Arc is dropped.
pub unsafe fn read<T>(ptr: *const T) -> Arc<T> {
    let arc = std::mem::ManuallyDrop::new(Arc::from_raw(ptr));
    arc.deref().clone()
}
