use std::fmt;
use std::mem;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, RwLock};

pub struct Trie<V> {
    root: Node<V>,
}

type Key = [u8; 8];

enum Node<V> {
    Empty,
    Leaf(Key, Arc<V>),
    Inner([Arc<Node<V>>; 256]),
}

impl<V> Trie<V> {
    pub fn empty() -> Self {
        Self { root: Node::Empty }
    }

    pub fn insert(&self, key: Key, value: V) -> Self {
        Self {
            root: self.root.insert(0, key, value),
        }
    }

    pub fn remove(&self, key: Key) -> Option<Self> {
        self.root.remove(0, key).map(|root| Self { root })
    }

    pub fn range<'a, R: RangeBounds<Key> + Clone>(&'a self, range: R) -> EntryIter<'_, V> {
        let start = match range.start_bound() {
            Bound::Included(key) => *key,
            Bound::Excluded(key) if *key == [u8::MAX; 8] => return EntryIter::empty(self),
            Bound::Excluded(key) => (u64::from_be_bytes(*key) + 1).to_be_bytes(),
            Bound::Unbounded => [0; 8],
        };
        let end = match range.end_bound() {
            Bound::Included(key) => *key,
            Bound::Excluded(key) if *key == [0; 8] => return EntryIter::empty(self),
            Bound::Excluded(key) => (u64::from_be_bytes(*key) - 1).to_be_bytes(),
            Bound::Unbounded => [u8::MAX; 8],
        };
        EntryIter::new(self, start, end)
    }

    pub fn find(&self, start: Key) -> Option<(Key, &Arc<V>)> {
        self.root.find(0, start)
    }
}

#[rustfmt::skip]
fn array_256<V>() -> [Arc<Node<V>>; 256] {
    fn empty<V>() -> Arc<Node<V>> { Arc::new(Node::Empty) }
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
    fn insert(&self, depth: usize, key: Key, value: V) -> Self {
        match self {
            Node::Empty => Node::Leaf(key, Arc::new(value)),
            Node::Leaf(found_key, found_value) if key == *found_key => {
                Node::Leaf(*found_key, Arc::new(value))
            }
            Node::Leaf(_, _) => {
                // Split Leaf into Inner.
                let inner = self.split(depth);
                // Try again.
                inner.insert(depth, key, value)
            }
            Node::Inner(children) => {
                let digit = key[depth] as usize;
                let mut children = children.clone();
                children[digit] = Arc::new(children[digit].insert(depth + 1, key, value));
                Node::Inner(children)
            }
        }
    }

    fn split(&self, depth: usize) -> Self {
        if let Node::Leaf(key, value) = self {
            let mut children = array_256();
            let digit = key[depth] as usize;
            children[digit] = Arc::new(Node::Leaf(*key, value.clone()));
            Node::Inner(children)
        } else {
            panic!()
        }
    }

    fn remove(&self, depth: usize, key: Key) -> Option<Self> {
        match self {
            Node::Empty => None,
            Node::Leaf(found_key, _) if key == *found_key => Some(Node::Empty),
            Node::Leaf(found_key, found_value) => None,
            Node::Inner(children) => {
                let digit = key[depth] as usize;
                children[digit].remove(depth + 1, key).map(|new_child| {
                    let mut children = children.clone();
                    children[digit] = Arc::new(new_child);
                    Node::Inner(children)
                })
            }
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Node::Empty => true,
            _ => false,
        }
    }

    fn find(&self, depth: usize, start: Key) -> Option<(Key, &Arc<V>)> {
        match self {
            Node::Empty => None,
            Node::Leaf(key, value) => {
                if start <= *key {
                    Some((*key, value))
                } else {
                    None
                }
            }
            Node::Inner(children) => {
                let digit = start[depth] as usize;
                if let Some(found) = children[digit].find(depth + 1, start) {
                    return Some(found);
                }
                for digit in (digit + 1)..children.len() {
                    if let Some(found) = children[digit].find(depth + 1, [0; 8]) {
                        return Some(found);
                    }
                }
                None
            }
        }
    }
}

impl<V: fmt::Display> fmt::Display for Trie<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.root.fmt(f)
    }
}

impl<V: fmt::Display> fmt::Display for Node<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Node::Empty => write!(f, "∅"),
            Node::Leaf(key, value) => {
                let strings: Vec<String> = key.iter().map(|i| i.to_string()).collect();
                write!(f, "{}:{}", strings.join(":"), value)
            }
            Node::Inner(children) => {
                let strings: Vec<String> = children
                    .iter()
                    .enumerate()
                    .flat_map(|(i, item)| {
                        if item.is_empty() {
                            None
                        } else {
                            Some(format!("{}:{}", i, item))
                        }
                    })
                    .collect();
                write!(f, "({})", strings.join(" "))
            }
        }
    }
}

pub struct EntryIter<'a, V> {
    parent: &'a Trie<V>,
    next: Option<Key>,
    end: Key,
}

impl<'a, V> EntryIter<'a, V> {
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

impl<'a, V> Iterator for EntryIter<'a, V> {
    type Item = (Key, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(start) = mem::replace(&mut self.next, None) {
            if let Some((key, value)) = self.parent.root.find(0, start) {
                if key <= self.end {
                    self.next = if key == [u8::MAX; 8] {
                        None
                    } else {
                        Some((u64::from_be_bytes(key) + 1).to_be_bytes())
                    };
                    return Some((key, value.as_ref()));
                }
            }
        }
        None
    }
}
