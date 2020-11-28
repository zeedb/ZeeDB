use crate::arc::*;
use std::sync::Arc;

#[test]
fn test_leak() {
    let arc = Arc::new("foo");
    let ptr = leak(&arc);
    assert_eq!(Arc::strong_count(&arc), 1);
    {
        let read = unsafe { read(ptr) };
        assert_eq!(Arc::strong_count(&arc), 2);
        assert_eq!(Arc::strong_count(&read), 2);
        assert_eq!(&"foo", arc.as_ref());
        assert_eq!(&"foo", read.as_ref());
    }
    assert_eq!(Arc::strong_count(&arc), 1);
    assert_eq!(&"foo", arc.as_ref());
}

#[test]
fn test_leak_u64() {
    let arc = Arc::new("foo");
    let ptr = leak(&arc) as u64;
    assert_eq!(Arc::strong_count(&arc), 1);
    {
        let read = unsafe { read(ptr as *const &str) };
        assert_eq!(Arc::strong_count(&arc), 2);
        assert_eq!(Arc::strong_count(&read), 2);
        assert_eq!(&"foo", arc.as_ref());
        assert_eq!(&"foo", read.as_ref());
    }
    assert_eq!(Arc::strong_count(&arc), 1);
    assert_eq!(&"foo", arc.as_ref());
}
