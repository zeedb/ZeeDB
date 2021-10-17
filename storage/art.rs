use std::{
    cmp::Ordering,
    fmt,
    ops::{Bound, RangeBounds},
};

#[derive(Clone)]
pub struct Art {
    root: Node,
}

impl Art {
    pub fn empty() -> Self {
        Art { root: Node::Null }
    }

    pub fn truncate(&mut self) {
        self.root = Node::Null;
    }

    pub fn insert(&mut self, key: &[u8], value: i64) -> Option<i64> {
        self.root.insert(key, value)
    }

    pub fn get(&self, key: &[u8]) -> Option<i64> {
        self.root.get(key)
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<i64> {
        self.root.remove(key)
    }

    pub fn range<'a>(&self, bounds: impl RangeBounds<&'a [u8]>) -> Vec<i64> {
        fn inc(key: &[u8]) -> Vec<u8> {
            if key.is_empty() {
                vec![0]
            } else if key.last().unwrap() == &u8::MAX {
                let mut key = key.to_vec();
                key.push(0);
                key
            } else {
                let mut key = key.to_vec();
                *key.last_mut().unwrap() += 1;
                key
            }
        }
        let start_inclusive = match bounds.start_bound() {
            Bound::Included(key) => key.to_vec(),
            Bound::Excluded(key) => inc(*key),
            Bound::Unbounded => vec![],
        };
        let (end_exclusive, is_bound) = match bounds.end_bound() {
            Bound::Included(key) => (inc(*key), true),
            Bound::Excluded(key) => (key.to_vec(), true),
            Bound::Unbounded => (vec![], false),
        };
        let end_exclusive = if is_bound {
            Some(end_exclusive.as_slice())
        } else {
            None
        };
        let mut acc = vec![];
        self.root.range(
            LowerBound(&start_inclusive),
            UpperBound(end_exclusive),
            &mut acc,
        );
        acc
    }
}

#[derive(Debug, Clone)]
enum Node {
    Null,
    Leaf(Box<Leaf>),
    Node4(Box<Node4>),
    Node16(Box<Node16>),
    Node48(Box<Node48>),
    Node256(Box<Node256>),
}

#[derive(Debug, Clone)]
struct Leaf {
    key: Vec<u8>,
    value: i64,
}

#[derive(Debug, Clone)]
struct Node4 {
    key: Vec<u8>,
    value: Option<i64>,
    count: usize,
    digit: [u8; 4],
    child: [Node; 4],
}

#[derive(Debug, Clone)]
struct Node16 {
    key: Vec<u8>,
    value: Option<i64>,
    count: usize,
    digit: [u8; 16],
    child: [Node; 16],
}

#[derive(Debug, Clone)]
struct Node48 {
    key: Vec<u8>,
    value: Option<i64>,
    count: usize,
    /// Initially EMPTY.
    child_index: [u8; 256],
    child: [Node; 48],
}

#[derive(Debug, Clone)]
struct Node256 {
    key: Vec<u8>,
    value: Option<i64>,
    count: usize,
    child: [Node; 256],
}

const EMPTY: u8 = 255;

impl Node {
    fn get(&self, key: &[u8]) -> Option<i64> {
        match self {
            Node::Null => None,
            Node::Leaf(node) => {
                let key = key.strip_prefix(&node.key[..])?;
                if key.is_empty() {
                    return Some(node.value);
                }
                None
            }
            Node::Node4(node) => {
                let key = key.strip_prefix(&node.key[..])?;
                if key.is_empty() {
                    return node.value;
                }
                for i in 0..node.count {
                    if node.digit[i] == key[0] {
                        return node.child[i].get(&key[1..]);
                    }
                }
                None
            }
            Node::Node16(node) => {
                let key = key.strip_prefix(&node.key[..])?;
                if key.is_empty() {
                    return node.value;
                }
                let i = compare_16_keys(node.digit, node.count, key[0])?;
                node.child[i].get(&key[1..])
            }
            Node::Node48(node) => {
                let key = key.strip_prefix(&node.key[..])?;
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
                let key = key.strip_prefix(&node.key[..])?;
                node.child[key[0] as usize].get(&key[1..])
            }
        }
    }

    fn remove(&mut self, key: &[u8]) -> Option<i64> {
        match self {
            Node::Null => None,
            Node::Leaf(node) => {
                let key = key.strip_prefix(&node.key[..])?;
                if key.is_empty() {
                    return self.take();
                }
                None
            }
            Node::Node4(node) => {
                let key = key.strip_prefix(&node.key[..])?;
                if key.is_empty() {
                    return self.take();
                }
                for i in 0..node.count {
                    if node.digit[i] == key[0] {
                        return node.child[i].remove(&key[1..]);
                    }
                }
                None
            }
            Node::Node16(node) => {
                let key = key.strip_prefix(&node.key[..])?;
                if key.is_empty() {
                    return self.take();
                }
                let i = compare_16_keys(node.digit, node.count, key[0])?;
                node.child[i].remove(&key[1..])
            }
            Node::Node48(node) => {
                let key = key.strip_prefix(&node.key[..])?;
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
                let key = key.strip_prefix(&node.key[..])?;
                node.child[key[0] as usize].remove(&key[1..])
            }
        }
        // TODO restore path compression.
    }

    fn range(
        &self,
        start_inclusive: LowerBound,
        end_exclusive: UpperBound,
        acc: &mut Vec<i64>,
    ) -> Option<()> {
        match self {
            Node::Null => Some(()),
            Node::Leaf(node) => {
                if start_inclusive.le(&node.key[..]) && end_exclusive.gt(&node.key[..]) {
                    acc.push(node.value);
                }
                Some(())
            }
            Node::Node4(node) => {
                let start_inclusive = start_inclusive.drop_prefix(&node.key[..])?;
                let end_exclusive = end_exclusive.drop_prefix(&node.key[..])?;
                if start_inclusive.is_empty() {
                    if let Some(value) = node.value {
                        acc.push(value);
                    }
                }
                for i in 0..node.count {
                    let digit = &node.digit[i..=i];
                    if let Some(start_inclusive) = start_inclusive.drop_prefix(digit) {
                        if let Some(end_exclusive) = end_exclusive.drop_prefix(digit) {
                            node.child[i].range(start_inclusive, end_exclusive, acc);
                        }
                    }
                }
                Some(())
            }
            Node::Node16(node) => {
                let start_inclusive = start_inclusive.drop_prefix(&node.key[..])?;
                let end_exclusive = end_exclusive.drop_prefix(&node.key[..])?;
                if start_inclusive.is_empty() {
                    if let Some(value) = node.value {
                        acc.push(value);
                    }
                }
                for i in 0..node.count {
                    let digit = &node.digit[i..=i];
                    if let Some(start_inclusive) = start_inclusive.drop_prefix(digit) {
                        if let Some(end_exclusive) = end_exclusive.drop_prefix(digit) {
                            node.child[i].range(start_inclusive, end_exclusive, acc);
                        }
                    }
                }
                Some(())
            }
            Node::Node48(node) => {
                let start_inclusive = start_inclusive.drop_prefix(&node.key[..])?;
                let end_exclusive = end_exclusive.drop_prefix(&node.key[..])?;
                if start_inclusive.is_empty() {
                    if let Some(value) = node.value {
                        acc.push(value);
                    }
                }
                for digit in 0..node.child_index.len() {
                    if node.child_index[digit] != EMPTY {
                        if let Some(start_inclusive) = start_inclusive.drop_prefix(&[digit as u8]) {
                            if let Some(end_exclusive) = end_exclusive.drop_prefix(&[digit as u8]) {
                                node.child[node.child_index[digit as usize] as usize].range(
                                    start_inclusive,
                                    end_exclusive,
                                    acc,
                                );
                            }
                        }
                    }
                }
                Some(())
            }
            Node::Node256(node) => {
                let start_inclusive = start_inclusive.drop_prefix(&node.key[..])?;
                let end_exclusive = end_exclusive.drop_prefix(&node.key[..])?;
                if start_inclusive.is_empty() {
                    if let Some(value) = node.value {
                        acc.push(value);
                    }
                }
                for digit in 0..node.child.len() {
                    if let Some(start_inclusive) = start_inclusive.drop_prefix(&[digit as u8]) {
                        if let Some(end_exclusive) = end_exclusive.drop_prefix(&[digit as u8]) {
                            node.child[digit as usize].range(start_inclusive, end_exclusive, acc);
                        }
                    }
                }
                Some(())
            }
        }
    }

    fn insert(&mut self, key: &[u8], value: i64) -> Option<i64> {
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

    fn key(&self) -> &[u8] {
        match self {
            Node::Null => &[],
            Node::Leaf(node) => &node.key[..],
            Node::Node4(node) => &node.key[..],
            Node::Node16(node) => &node.key[..],
            Node::Node48(node) => &node.key[..],
            Node::Node256(node) => &node.key[..],
        }
    }

    fn store(&mut self, value: i64) -> Option<i64> {
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

    fn take(&mut self) -> Option<i64> {
        match self {
            Node::Null => None,
            Node::Leaf(_node) => {
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

    fn set(&mut self, digit: u8, key: &[u8], value: i64) -> Option<i64> {
        match self {
            Node::Null => {
                *self = Node::Leaf(Box::new(Leaf {
                    key: push_back(digit, key),
                    value,
                }));
                None
            }
            Node::Leaf(_node) => {
                self.grow();
                self.set(digit, key, value)
            }
            Node::Node4(node) => {
                // If digit is already present, recurse.
                for i in 0..node.count {
                    if node.digit[i] == digit {
                        return node.child[i].insert(key, value);
                    }
                }
                // If space is available, add digit.
                if node.count < 4 {
                    node.digit[node.count] = digit;
                    node.child[node.count] = Node::Leaf(Box::new(Leaf {
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
                if let Some(i) = compare_16_keys(node.digit, node.count, digit) {
                    return node.child[i].insert(key, value);
                }
                // If space is available, add digit.
                if node.count < 16 {
                    node.digit[node.count] = digit;
                    node.child[node.count] = Node::Leaf(Box::new(Leaf {
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
                    node.child[node.count] = Node::Leaf(Box::new(Leaf {
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
                for i in 0..node.count {
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
                for i in 0..node.count {
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
        }
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
                for i in 0..node.count {
                    node.child[i].print(f, format!("{:>3} -> ", node.digit[i]), indent + 1)?;
                }
                Ok(())
            }
            Node::Node16(node) => {
                println(
                    indent,
                    format!("{}Node16 {:?} {:?}", prefix, node.key, node.value),
                );
                for i in 0..node.count {
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
                for i in 0..node.child.len() {
                    node.child[i].print(f, format!("{:>3} -> ", i), indent + 1)?;
                }
                Ok(())
            }
        }
    }
}

struct LowerBound<'a>(&'a [u8]);
struct UpperBound<'a>(Option<&'a [u8]>);

impl<'a> LowerBound<'a> {
    fn drop_prefix(&self, prefix: &[u8]) -> Option<Self> {
        match self.head(prefix.len()).cmp(prefix) {
            Ordering::Less => Some(LowerBound(&[])),
            Ordering::Equal => Some(self.tail(prefix.len())),
            Ordering::Greater => None,
        }
    }

    fn cmp(&self, key: &[u8]) -> Ordering {
        self.0.cmp(key)
    }

    fn le(&self, key: &[u8]) -> bool {
        self.0 <= key
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn head(&self, n: usize) -> Self {
        LowerBound(head(self.0, n))
    }

    fn tail(&self, n: usize) -> Self {
        LowerBound(tail(self.0, n))
    }
}

impl<'a> UpperBound<'a> {
    fn drop_prefix(&self, prefix: &[u8]) -> Option<Self> {
        if let UpperBound(Some(greater)) = self {
            match head(greater, prefix.len()).cmp(prefix) {
                Ordering::Greater => Some(UpperBound(None)),
                Ordering::Equal => Some(UpperBound(Some(tail(greater, prefix.len())))),
                Ordering::Less => None,
            }
        } else {
            Some(UpperBound(None))
        }
    }

    fn gt(&self, key: &[u8]) -> bool {
        if let UpperBound(Some(greater)) = self {
            *greater > key
        } else {
            true
        }
    }
}

fn head(slice: &[u8], n: usize) -> &[u8] {
    if n < slice.len() {
        &slice[..n]
    } else {
        slice
    }
}

fn tail(slice: &[u8], n: usize) -> &[u8] {
    if n < slice.len() {
        &slice[n..]
    } else {
        &[]
    }
}

impl Default for Node {
    fn default() -> Self {
        Node::Null
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

fn compare_16_keys(digit: [u8; 16], count: usize, key: u8) -> Option<usize> {
    unsafe {
        use core::arch::x86_64;

        let byte = x86_64::_mm_set1_epi8(std::mem::transmute(key));
        let cmp = x86_64::_mm_cmpeq_epi8(byte, std::mem::transmute(digit));
        let mask = (1 << count) - 1;
        let bitfield = x86_64::_mm_movemask_epi8(cmp) & mask;
        if bitfield != 0 {
            Some(bitfield.trailing_zeros() as usize)
        } else {
            None
        }
    }
}

#[test]
fn test_compare_16_keys() {
    for count in 0..16 {
        for position in 0..count {
            let mut digit = [0u8; 16];
            digit[position] = 1;
            assert_eq!(Some(position), compare_16_keys(digit, count, 1));
            assert_eq!(None, compare_16_keys(digit, count, 2));
        }
    }
}
