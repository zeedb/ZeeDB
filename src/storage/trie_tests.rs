use crate::trie::*;
use std::sync::Arc;

#[test]
fn test_trie() {
    let trie = Trie::empty();
    let a = (1 as i64).to_be_bytes();
    let b = (1024 as i64).to_be_bytes();
    let trie = trie.insert(a, 10);
    let trie = trie.insert(b, 20);
    type A<'a> = Vec<([u8; 8], &'a i32)>;
    assert_eq!(vec![(a, &10)], trie.range(a..=a).collect::<A>());
    assert_eq!(vec![(b, &20)], trie.range(b..=b).collect::<A>());
    assert_eq!(vec![(a, &10), (b, &20)], trie.range(a..=b).collect::<A>());
    let trie = trie.remove(b).unwrap();
    assert_eq!(vec![(a, &10)], trie.range(a..=b).collect::<A>());
    let trie = trie.insert(b, 20);
    let trie = trie.insert(b, 200);
    assert_eq!(vec![(a, &10), (b, &200)], trie.range(a..=b).collect::<A>());
}
