use std::fmt;
use std::ops::{Bound, RangeBounds};

pub struct Art {
    root: Node,
}

impl Art {
    pub fn new() -> Self {
        Art { root: Node::Null }
    }
    pub fn insert(&mut self, key: &[u8], value: Value) -> Option<Value> {
        self.root.insert(key, value)
    }

    pub fn get(&self, key: &[u8]) -> Option<Value> {
        self.root.get(key)
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<Value> {
        self.root.remove(key)
    }

    pub fn range<'a, 'b>(&'a self, bounds: impl RangeBounds<&'b [u8]>) -> RangeIter<'a> {
        let start = match bounds.start_bound() {
            Bound::Included(slice) => Bound::Included(slice.to_vec()),
            Bound::Excluded(slice) => Bound::Excluded(slice.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match bounds.end_bound() {
            Bound::Included(slice) => Bound::Included(slice.to_vec()),
            Bound::Excluded(slice) => Bound::Excluded(slice.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        RangeIter {
            start,
            end,
            // TODO: find the smallest node that includes range.
            ancestors: vec![],
            unexplored: Some(&self.root),
        }
    }
}

#[derive(Debug)]
enum Node {
    Null,
    Leaf(Box<Leaf>),
    Node4(Box<Node4>),
    Node16(Box<Node16>),
    Node48(Box<Node48>),
    Node256(Box<Node256>),
}

#[derive(Debug)]
struct Leaf {
    key: Vec<u8>,
    value: Value,
}

#[derive(Debug)]
struct Node4 {
    key: Vec<u8>,
    value: Option<Value>,
    count: u32,
    digit: [u8; 4],
    child: [Node; 4],
}

#[derive(Debug)]
struct Node16 {
    key: Vec<u8>,
    value: Option<Value>,
    count: u32,
    digit: [u8; 16],
    child: [Node; 16],
}

#[derive(Debug)]
struct Node48 {
    key: Vec<u8>,
    value: Option<Value>,
    count: u32,
    /// Initially EMPTY.
    child_index: [u8; 256],
    child: [Node; 48],
}

#[derive(Debug)]
struct Node256 {
    key: Vec<u8>,
    value: Option<Value>,
    count: u32,
    child: [Node; 256],
}

const EMPTY: u8 = 255;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Value {
    // Page ID. Page can be retrieved with Page::read(pid).
    pub pid: u64,
    // Tuple ID.
    pub tid: u32,
}

impl Node {
    fn get(&self, key: &[u8]) -> Option<Value> {
        match self {
            Node::Null => None,
            Node::Leaf(node) => {
                let key = key.strip_prefix(&node.key)?;
                if key.is_empty() {
                    return Some(node.value);
                }
                None
            }
            Node::Node4(node) => {
                let key = key.strip_prefix(&node.key)?;
                if key.is_empty() {
                    return node.value;
                }
                for i in 0..node.count as usize {
                    if node.digit[i] == key[0] {
                        return node.child[i].get(&key[1..]);
                    }
                }
                None
            }
            Node::Node16(node) => {
                let key = key.strip_prefix(&node.key)?;
                if key.is_empty() {
                    return node.value;
                }
                let byte = packed_simd::u8x16::splat(key[0]);
                let cmp = byte.eq(packed_simd::u8x16::from(node.digit));
                let mask = 1u16.checked_shl(node.count).unwrap_or(0).wrapping_sub(1);
                let bitfield = cmp.bitmask() & mask;
                if bitfield != 0 {
                    return node.child[bitfield.trailing_zeros() as usize].get(&key[1..]);
                }
                None
            }
            Node::Node48(node) => {
                let key = key.strip_prefix(&node.key)?;
                if key.is_empty() {
                    return node.value;
                }
                if node.child_index[key[0] as usize] != EMPTY {
                    let i = node.child_index[key[0] as usize] as usize;
                    return node.child[i].get(&key[1..]);
                }
                None
            }
            Node::Node256(node) => {
                let key = key.strip_prefix(&node.key)?;
                node.child[key[0] as usize].get(&key[1..])
            }
        }
    }

    fn remove(&mut self, key: &[u8]) -> Option<Value> {
        match self {
            Node::Null => None,
            Node::Leaf(node) => {
                let key = key.strip_prefix(&node.key)?;
                if key.is_empty() {
                    return self.take();
                }
                None
            }
            Node::Node4(node) => {
                let key = key.strip_prefix(&node.key)?;
                if key.is_empty() {
                    return self.take();
                }
                for i in 0..node.count as usize {
                    if node.digit[i] == key[0] {
                        return node.child[i].remove(&key[1..]);
                    }
                }
                None
            }
            Node::Node16(node) => {
                let key = key.strip_prefix(&node.key)?;
                if key.is_empty() {
                    return self.take();
                }
                let byte = packed_simd::u8x16::splat(key[0]);
                let cmp = byte.eq(packed_simd::u8x16::from(node.digit));
                let mask = 1u16.checked_shl(node.count).unwrap_or(0).wrapping_sub(1);
                let bitfield = cmp.bitmask() & mask;
                if bitfield != 0 {
                    return node.child[bitfield.trailing_zeros() as usize].remove(&key[1..]);
                }
                None
            }
            Node::Node48(node) => {
                let key = key.strip_prefix(&node.key)?;
                if key.is_empty() {
                    return self.take();
                }
                if node.child_index[key[0] as usize] != EMPTY {
                    let i = node.child_index[key[0] as usize] as usize;
                    return node.child[i].remove(&key[1..]);
                }
                None
            }
            Node::Node256(node) => {
                let key = key.strip_prefix(&node.key)?;
                node.child[key[0] as usize].remove(&key[1..])
            }
        }
        // TODO restore path compression.
    }

    fn insert(&mut self, key: &[u8], value: Value) -> Option<Value> {
        if let Some(key) = key.strip_prefix(self.key()) {
            if key.is_empty() {
                self.store(value)
            } else {
                self.set(key[0], &key[1..], value)
            }
        } else {
            self.pivot(find_pivot(key, self.key()));
            self.insert(key, value)
        }
    }

    fn first_child(&self) -> Option<&Node> {
        match self {
            Node::Null | Node::Leaf(_) => None,
            Node::Node4(node) => {
                if 0 < node.count {
                    Some(&node.child[0])
                } else {
                    None
                }
            }
            Node::Node16(node) => {
                if 0 < node.count {
                    Some(&node.child[0])
                } else {
                    None
                }
            }
            Node::Node48(node) => {
                if 0 < node.count {
                    Some(&node.child[0])
                } else {
                    None
                }
            }
            Node::Node256(node) => {
                if 0 < node.count {
                    Some(&node.child[0])
                } else {
                    None
                }
            }
        }
    }

    fn next_child(&self, child: u32) -> Option<&Node> {
        match self {
            Node::Null | Node::Leaf(_) => None,
            Node::Node4(node) => {
                if child + 1 < node.count {
                    Some(&node.child[child as usize + 1])
                } else {
                    None
                }
            }
            Node::Node16(node) => {
                if child + 1 < node.count {
                    Some(&node.child[child as usize + 1])
                } else {
                    None
                }
            }
            Node::Node48(node) => {
                if child + 1 < node.count {
                    Some(&node.child[child as usize + 1])
                } else {
                    None
                }
            }
            Node::Node256(node) => {
                if child + 1 < node.count {
                    Some(&node.child[child as usize + 1])
                } else {
                    None
                }
            }
        }
    }

    fn key(&self) -> &[u8] {
        match self {
            Node::Null => &[],
            Node::Leaf(node) => &node.key,
            Node::Node4(node) => &node.key,
            Node::Node16(node) => &node.key,
            Node::Node48(node) => &node.key,
            Node::Node256(node) => &node.key,
        }
    }

    fn store(&mut self, value: Value) -> Option<Value> {
        match self {
            Node::Null => {
                *self = Node::Leaf(Box::new(Leaf { key: vec![], value }));
                None
            }
            Node::Leaf(node) => Some(std::mem::replace(&mut node.value, value)),
            Node::Node4(node) => std::mem::replace(&mut node.value, Some(value)),
            Node::Node16(node) => std::mem::replace(&mut node.value, Some(value)),
            Node::Node48(node) => std::mem::replace(&mut node.value, Some(value)),
            Node::Node256(node) => std::mem::replace(&mut node.value, Some(value)),
        }
    }

    fn take(&mut self) -> Option<Value> {
        match self {
            Node::Null => None,
            Node::Leaf(node) => {
                if let Node::Leaf(node) = std::mem::take(self) {
                    Some(node.value)
                } else {
                    panic!()
                }
            }
            Node::Node4(node) => std::mem::take(&mut node.value),
            Node::Node16(node) => std::mem::take(&mut node.value),
            Node::Node48(node) => std::mem::take(&mut node.value),
            Node::Node256(node) => std::mem::take(&mut node.value),
        }
    }

    fn peek(&self) -> Option<Value> {
        match self {
            Node::Null => None,
            Node::Leaf(node) => Some(node.value),
            Node::Node4(node) => node.value,
            Node::Node16(node) => node.value,
            Node::Node48(node) => node.value,
            Node::Node256(node) => node.value,
        }
    }

    fn set(&mut self, digit: u8, key: &[u8], value: Value) -> Option<Value> {
        match self {
            Node::Null => {
                *self = Node::Leaf(Box::new(Leaf {
                    key: push_back(digit, key),
                    value,
                }));
                None
            }
            Node::Leaf(node) => {
                self.grow();
                self.set(digit, key, value)
            }
            Node::Node4(node) => {
                // If digit is already present, recurse.
                for i in 0..node.count as usize {
                    if node.digit[i] == digit {
                        return node.child[i].insert(key, value);
                    }
                }
                // If space is available, add digit.
                if node.count < 4 {
                    node.digit[node.count as usize] = digit;
                    node.child[node.count as usize] = Node::Leaf(Box::new(Leaf {
                        key: key.to_vec(),
                        value,
                    }));
                    node.count += 1;
                    return None;
                }
                // Otherwise, grow the node and try again.
                self.grow();
                self.set(digit, key, value)
            }
            Node::Node16(node) => {
                // If digit is already present, recurse.
                let byte = packed_simd::u8x16::splat(digit);
                let cmp = byte.eq(packed_simd::u8x16::from(node.digit));
                let mask = 1u16.checked_shl(node.count).unwrap_or(0).wrapping_sub(1);
                let bitfield = cmp.bitmask() & mask;
                if bitfield != 0 {
                    return node.child[bitfield.trailing_zeros() as usize].insert(key, value);
                }
                // If space is available, add digit.
                if node.count < 16 {
                    node.digit[node.count as usize] = digit;
                    node.child[node.count as usize] = Node::Leaf(Box::new(Leaf {
                        key: key.to_vec(),
                        value,
                    }));
                    node.count += 1;
                    return None;
                }
                // Otherwise, grow the node and try again.
                self.grow();
                self.set(digit, key, value)
            }
            Node::Node48(node) => {
                // If digit is already present, recurse.
                if node.child_index[digit as usize] != EMPTY {
                    let i = node.child_index[digit as usize] as usize;
                    return node.child[i].insert(key, value);
                }
                // If space is available, add digit.
                if node.count < 48 {
                    node.child_index[digit as usize] = node.count as u8;
                    node.child[node.count as usize] = Node::Leaf(Box::new(Leaf {
                        key: key.to_vec(),
                        value,
                    }));
                    node.count += 1;
                    return None;
                }
                // Otherwise, grow the node and try again.
                self.grow();
                self.set(digit, key, value)
            }
            Node::Node256(node) => node.child[digit as usize].insert(key, value),
        }
    }

    fn grow(&mut self) {
        *self = match std::mem::take(self) {
            Node::Null => panic!("cannot grow Null"),
            Node::Leaf(node) => Node::Node4(Box::new(Node4 {
                key: node.key,
                value: Some(node.value),
                count: 0,
                digit: Default::default(),
                child: Default::default(),
            })),
            Node::Node4(mut node) => {
                let mut digit: [u8; 16] = Default::default();
                let mut child: [Node; 16] = Default::default();
                for i in 0..node.count as usize {
                    digit[i] = node.digit[i];
                    child[i] = std::mem::take(&mut node.child[i]);
                }
                Node::Node16(Box::new(Node16 {
                    key: node.key,
                    value: node.value,
                    count: node.count,
                    digit,
                    child,
                }))
            }
            Node::Node16(mut node) => {
                let mut child_index = [EMPTY; 256];
                let mut child = nulls_48();
                for i in 0..node.count as usize {
                    child_index[node.digit[i] as usize] = i as u8;
                    child[i] = std::mem::take(&mut node.child[i]);
                }
                Node::Node48(Box::new(Node48 {
                    key: node.key,
                    value: node.value,
                    count: node.count,
                    child_index,
                    child,
                }))
            }
            Node::Node48(mut node) => {
                let mut child = nulls_256();
                for i in 0..node.child_index.len() {
                    if node.child_index[i] != EMPTY {
                        child[i] = std::mem::take(&mut node.child[node.child_index[i] as usize]);
                    }
                }
                Node::Node256(Box::new(Node256 {
                    key: node.key,
                    value: node.value,
                    count: node.count,
                    child,
                }))
            }
            Node::Node256(_) => panic!("cannot grow Node256"),
        }
    }

    fn pivot(&mut self, pivot: usize) {
        let (prefix, digit, suffix) = {
            let key = self.key();
            (key[..pivot].to_vec(), key[pivot], key[pivot + 1..].to_vec())
        };
        self.set_key(suffix);
        *self = {
            Node::Node4(Box::new(Node4 {
                key: prefix,
                value: None,
                count: 1,
                digit: [digit, 0, 0, 0],
                child: [std::mem::take(self), Node::Null, Node::Null, Node::Null],
            }))
        }
    }

    fn set_key(&mut self, key: Vec<u8>) {
        match self {
            Node::Null => panic!("cannot set key of Null"),
            Node::Leaf(node) => node.key = key,
            Node::Node4(node) => node.key = key,
            Node::Node16(node) => node.key = key,
            Node::Node48(node) => node.key = key,
            Node::Node256(node) => node.key = key,
        }
    }

    fn print(&self, f: &mut fmt::Formatter<'_>, prefix: String, indent: usize) -> fmt::Result {
        fn println(indent: usize, str: String) {
            for _ in 0..indent {
                print!("\t");
            }
            println!("{}", str)
        };
        match self {
            Node::Null => Ok(println(indent, format!("{}Null", prefix))),
            Node::Leaf(node) => Ok(println(
                indent,
                format!("{}Leaf {:?} {:?}", prefix, node.key, node.value),
            )),
            Node::Node4(node) => {
                println(
                    indent,
                    format!("{}Node4 {:?} {:?}", prefix, node.key, node.value),
                );
                for i in 0..node.count as usize {
                    node.child[i].print(f, format!("{:>3} -> ", node.digit[i]), indent + 1)?;
                }
                Ok(())
            }
            Node::Node16(node) => {
                println(
                    indent,
                    format!("{}Node16 {:?} {:?}", prefix, node.key, node.value),
                );
                for i in 0..node.count as usize {
                    node.child[i].print(f, format!("{:>3} -> ", node.digit[i]), indent + 1)?;
                }
                Ok(())
            }
            Node::Node48(node) => {
                println(
                    indent,
                    format!("{}Node48 {:?} {:?}", prefix, node.key, node.value),
                );
                for i in 0..node.child_index.len() {
                    if node.child_index[i] != EMPTY {
                        node.child[node.child_index[i] as usize].print(
                            f,
                            format!("{:>3} -> ", node.child_index[i]),
                            indent + 1,
                        )?;
                    }
                }
                Ok(())
            }
            Node::Node256(node) => {
                println(
                    indent,
                    format!("{}Node256 {:?} {:?}", prefix, node.key, node.value),
                );
                for i in 0..node.count as usize {
                    node.child[i].print(f, format!("{:>3} -> ", i), indent + 1)?;
                }
                Ok(())
            }
        }
    }
}

pub struct RangeIter<'a> {
    // TODO use start and end to filter results.
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
    ancestors: Vec<(&'a Node, u32)>,
    unexplored: Option<&'a Node>,
}

impl fmt::Debug for RangeIter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (node, child) in &self.ancestors {
            write!(f, "{:?} =={}==> ", node.key(), child)?;
        }
        if let Some(unexplored) = self.unexplored {
            write!(f, "{:?} ...", unexplored.key())
        } else {
            write!(f, "*")
        }
    }
}

impl<'a> Iterator for RangeIter<'a> {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If cursor points to an unexplored node, try to traverse down it.
            if let Some(cursor) = std::mem::take(&mut self.unexplored) {
                if let Some(next) = cursor.first_child() {
                    // Update the iterator to point to the first child.
                    self.ancestors.push((cursor, 0));
                    self.unexplored = Some(next);
                }
                // Return the value of the node we just explored.
                if let Some(value) = cursor.peek() {
                    return Some(value);
                } else {
                    continue;
                }
            }
            // Try to move to the next sibling.
            if let Some((parent, child)) = self.ancestors.last_mut() {
                if let Some(next) = parent.next_child(*child) {
                    // Update the iterator to point to the next sibline.
                    *child = *child + 1;
                    self.unexplored = Some(next);
                    // In the next iteration, we will enter the "explore child" branch.
                    continue;
                }
            }
            // Try to move upwards so that we can move to the uncle of the cursor in the next iteration.
            if let Some(_) = self.ancestors.pop() {
                // In the next iteration, we will enter the "try to move sideways" branch.
                continue;
            }
            // Give up.
            return None;
        }
    }
}

impl Default for Node {
    fn default() -> Self {
        Node::Null
    }
}

impl Default for Value {
    fn default() -> Self {
        Value { pid: 0, tid: 0 }
    }
}

impl fmt::Display for Art {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.root.print(f, "".to_string(), 0)
    }
}

#[rustfmt::skip]
fn nulls_48() -> [Node; 48] {
    [
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
    ]
}

#[rustfmt::skip]
fn nulls_256() -> [Node; 256] {
    [
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
        Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, Node::Null, 
    ]
}

fn find_pivot(new_key: &[u8], prefix: &[u8]) -> usize {
    assert!(!prefix.is_empty());
    let max = usize::min(new_key.len(), prefix.len() - 1);
    for i in 0..max {
        if new_key[i] != prefix[i] {
            return i;
        }
    }
    max
}

fn push_back(digit: u8, key: &[u8]) -> Vec<u8> {
    let mut vec = Vec::with_capacity(key.len() + 1);
    vec.push(digit);
    vec.extend_from_slice(key);
    vec
}
