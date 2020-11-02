use crate::trie::*;
use std::sync::Arc;

#[test]
fn test_trie() {
    let trie = Trie::new();
    let a = (1 as i64).to_be_bytes();
    let b = (1024 as i64).to_be_bytes();
    assert_eq!(None, trie.insert(a, 10));
    assert_eq!(None, trie.insert(b, 20));
    assert_eq!(
        vec![(a, 10)],
        trie.range(a..=a)
            .map(|(k, v)| (k, *v))
            .collect::<Vec<([u8; 8], i32)>>()
    );
    assert_eq!(
        vec![(b, 20)],
        trie.range(b..=b)
            .map(|(k, v)| (k, *v))
            .collect::<Vec<([u8; 8], i32)>>()
    );
    assert_eq!(Some(Arc::new(20)), trie.remove(b));
    assert_eq!(
        vec![(a, 10)],
        trie.range(a..=a)
            .map(|(k, v)| (k, *v))
            .collect::<Vec<([u8; 8], i32)>>()
    );
    assert_eq!(
        vec![] as Vec<([u8; 8], i32)>,
        trie.range(b..=b)
            .map(|(k, v)| (k, *v))
            .collect::<Vec<([u8; 8], i32)>>()
    );
    assert_eq!(None, trie.insert(b, 20));
    assert_eq!(Some(Arc::new(20)), trie.insert(b, 200));
    assert_eq!(
        vec![(a, 10), (b, 200)],
        trie.range(..)
            .map(|(k, v)| (k, *v))
            .collect::<Vec<([u8; 8], i32)>>()
    );
}
