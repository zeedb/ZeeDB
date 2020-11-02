use std::fmt;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, RwLock};

pub struct Trie<V> {
    root: RwLock<Node<V>>,
}

type Key = [u8; 8];

enum Node<V> {
    Empty,
    Leaf(Key, Arc<V>),
    Inner(Box<[RwLock<Node<V>>; 256]>),
}

pub struct TrieIter<'a, V> {
    parent: &'a Trie<V>,
    next: Option<Key>,
    end: Key,
}

impl<V> Trie<V> {
    pub fn new() -> Self {
        Self {
            root: RwLock::new(Node::Empty),
        }
    }

    pub fn insert(&self, key: Key, value: V) -> Option<Arc<V>> {
        self.root.write().unwrap().insert(0, key, value)
    }

    pub fn remove(&self, key: Key) -> Option<Arc<V>> {
        self.root.write().unwrap().remove(0, key)
    }

    pub fn range<'a>(&'a self, range: impl RangeBounds<Key>) -> TrieIter<'_, V> {
        let start = match range.start_bound() {
            Bound::Included(key) => *key,
            Bound::Excluded(key) if *key == [u8::MAX; 8] => return TrieIter::empty(self),
            Bound::Excluded(key) => (u64::from_be_bytes(*key) + 1).to_be_bytes(),
            Bound::Unbounded => [0; 8],
        };
        let end = match range.end_bound() {
            Bound::Included(key) => *key,
            Bound::Excluded(key) if *key == [0; 8] => return TrieIter::empty(self),
            Bound::Excluded(key) => (u64::from_be_bytes(*key) - 1).to_be_bytes(),
            Bound::Unbounded => [u8::MAX; 8],
        };
        TrieIter::new(self, start, end)
    }
}

#[rustfmt::skip]
fn array_256<V>() -> [RwLock<Node<V>>; 256] {
    fn empty<V>() -> RwLock<Node<V>> { RwLock::new(Node::Empty) }
    [
        empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), 
        empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), 
        empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), 
        empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), 
        empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), 
        empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), 
        empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), 
        empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), 
        empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), 
        empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), 
        empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), 
        empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), 
        empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), 
        empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), 
        empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), 
        empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), 
    ]
}

impl<V> Node<V> {
    fn inner() -> Self {
        Node::Inner(Box::new(array_256()))
    }

    fn insert(&mut self, depth: usize, key: Key, value: V) -> Option<Arc<V>> {
        match self {
            Node::Empty => {
                *self = Node::Leaf(key, Arc::new(value));
                None
            }
            Node::Leaf(found_key, found_value) if key == *found_key => {
                let new = Arc::new(value);
                let previous = std::mem::replace(found_value, new);
                Some(previous)
            }
            Node::Leaf(_, _) => {
                // Split Leaf into Inner.
                self.split(depth);
                // Try again.
                self.insert(depth, key, value)
            }
            Node::Inner(children) => {
                let digit = key[depth] as usize;
                children[digit]
                    .write()
                    .unwrap()
                    .insert(depth + 1, key, value)
            }
        }
    }

    fn split(&mut self, depth: usize) {
        if let Node::Leaf(key, value) = std::mem::replace(self, Node::inner()) {
            if let Node::Inner(children) = self {
                let digit = key[depth] as usize;
                *children[digit].write().unwrap() = Node::Leaf(key, value);
                return;
            }
        }
        panic!()
    }

    fn remove(&mut self, depth: usize, key: Key) -> Option<Arc<V>> {
        match self {
            Node::Empty => None,
            Node::Leaf(found_key, _) if key == *found_key => {
                if let Node::Leaf(found_key, found_value) = std::mem::replace(self, Node::Empty) {
                    Some(found_value)
                } else {
                    panic!()
                }
            }
            Node::Leaf(_, _) => None,
            Node::Inner(children) => {
                let digit = key[depth] as usize;
                children[digit].write().unwrap().remove(depth + 1, key)
            }
        }
    }

    fn seek(&self, depth: usize, key: Key) -> Option<(Key, Arc<V>)> {
        match self {
            Node::Empty => None,
            Node::Leaf(found_key, found_value) => {
                if key <= *found_key {
                    Some((*found_key, found_value.clone()))
                } else {
                    None
                }
            }
            Node::Inner(children) => {
                let digit = key[depth] as usize;
                if let Some(found) = children[digit].read().unwrap().seek(depth + 1, key) {
                    return Some(found);
                }
                for digit in (digit + 1)..children.len() {
                    if let Some(found) = children[digit].read().unwrap().seek(depth + 1, [0; 8]) {
                        return Some(found);
                    }
                }
                None
            }
        }
    }
}

impl<V: fmt::Debug> fmt::Debug for Trie<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let root = &*self.root.read().unwrap();
        root.fmt(f)
    }
}

impl<V: fmt::Debug> fmt::Debug for Node<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Node::Empty => write!(f, "∅"),
            Node::Leaf(key, value) => {
                let strings: Vec<String> = key.iter().map(|i| i.to_string()).collect();
                write!(f, "{}:{:?}", strings.join(":"), value)
            }
            Node::Inner(children) => {
                let strings: Vec<String> = children
                    .iter()
                    .enumerate()
                    .flat_map(|(i, item)| match &*item.read().unwrap() {
                        Node::Empty => None,
                        child => Some(format!("{}:{:?}", i, child)),
                    })
                    .collect();
                write!(f, "({})", strings.join(" "))
            }
        }
    }
}

impl<'a, V> TrieIter<'a, V> {
    fn new(parent: &'a Trie<V>, start: Key, end: Key) -> Self {
        Self {
            parent,
            next: Some(start),
            end,
        }
    }

    fn empty(parent: &'a Trie<V>) -> Self {
        Self {
            parent,
            next: None,
            end: [0; 8],
        }
    }
}

impl<'a, V> Iterator for TrieIter<'a, V> {
    type Item = (Key, Arc<V>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(seek_key) = std::mem::replace(&mut self.next, None) {
            if let Some((found_key, found_value)) =
                self.parent.root.read().unwrap().seek(0, seek_key)
            {
                if found_key <= self.end {
                    self.next = if found_key == [u8::MAX; 8] {
                        None
                    } else {
                        Some((u64::from_be_bytes(found_key) + 1).to_be_bytes())
                    };
                    return Some((found_key, found_value));
                }
            }
        }
        None
    }
}
