use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasherDefault, Hash, Hasher},
};

use ast::{AggregateExpr, AggregateFunction, Value};
use kernel::*;

pub struct SimpleAggregate {
    aggregate_slots: Vec<Acc>,
}

pub struct GroupByAggregate {
    group_by_batches: Vec<Batch>,
    aggregate_slots: HashMap<Key, Vec<Acc>, BuildKeyHasher>,
    aggregate_slot_template: Vec<Acc>,
}

struct Batch {
    group_by: Vec<AnyArray>,
    hash: I64Array,
}

struct Key {
    parent: *const GroupByAggregate,
    batch: u32,
    tuple: u32,
}

#[derive(Clone)]
enum Acc {
    AnyValue(Value),
    Count(i64),
    CountDistinct(Distinct),
    LogicalAnd(Option<bool>),
    LogicalOr(Option<bool>),
    Max(Value),
    Min(Value),
    Sum(Value),
    SumDistinct(Distinct),
}

#[derive(Clone)]
enum Distinct {
    Bool(HashSet<bool>),
    I64(HashSet<i64>),
    F64(HashSet<u64>),
    Date(HashSet<i32>),
    Timestamp(HashSet<i64>),
    String(HashSet<String>),
}

impl SimpleAggregate {
    pub fn new(aggregate_fns: &Vec<AggregateExpr>) -> Self {
        Self {
            aggregate_slots: aggregate_fns.iter().map(|a| Acc::new(a)).collect(),
        }
    }

    /// Insert a batch of rows into the hash table.
    pub fn insert(&mut self, aggregate: Vec<AnyArray>) {
        let len = aggregate.first().unwrap().len();
        for tuple in 0..len as u32 {
            for i in 0..self.aggregate_slots.len() {
                self.aggregate_slots[i].update(&aggregate[i], tuple);
            }
        }
    }

    /// Return the results we've accumulated so far.
    pub fn finish(&self) -> Vec<AnyArray> {
        self.aggregate_slots.iter().map(Acc::finish).collect()
    }
}

impl GroupByAggregate {
    pub fn new(aggregate_fns: &Vec<AggregateExpr>) -> Self {
        Self {
            group_by_batches: vec![],
            aggregate_slots: HashMap::default(),
            aggregate_slot_template: aggregate_fns.iter().map(|a| Acc::new(a)).collect(),
        }
    }

    /// Insert a batch of rows into the hash table.
    pub fn insert(&mut self, group_by: Vec<AnyArray>, aggregate: Vec<AnyArray>) {
        let len = group_by.first().or(aggregate.first()).unwrap().len();
        // Add batch to the universe of tuples that we know about.
        let hash = if group_by.is_empty() {
            I64Array::zeros(len)
        } else {
            I64Array::hash_all(&group_by)
        };
        self.group_by_batches.push(Batch { group_by, hash });
        // Add each new tuple to the hash table.
        let parent = self as *const GroupByAggregate;
        let batch = self.group_by_batches.len() as u32 - 1;
        let aggregate_slot_template = self.aggregate_slot_template.clone();
        for tuple in 0..len as u32 {
            let key = Key {
                parent,
                batch,
                tuple,
            };
            let accs = self
                .aggregate_slots
                .entry(key)
                .or_insert_with(|| aggregate_slot_template.clone());
            for i in 0..accs.len() {
                accs[i].update(&aggregate[i], tuple);
            }
        }
    }

    /// Return the results we've accumulated so far.
    pub fn finish(&self) -> Vec<AnyArray> {
        let num_rows = self.aggregate_slots.len();
        let mut group_by_builders: Vec<AnyArray> = self.group_by_batches[0]
            .group_by
            .iter()
            .map(|c| AnyArray::with_capacity(c.data_type(), num_rows))
            .collect();
        let mut aggregate_builders: Vec<AnyArray> = self
            .aggregate_slot_template
            .iter()
            .map(|a| AnyArray::with_capacity(a.data_type(), num_rows))
            .collect();
        for (key, aggregate) in &self.aggregate_slots {
            let group_by = &self.group_by_batches[key.batch as usize].group_by;
            for i in 0..group_by.len() {
                push(&mut group_by_builders[i], &group_by[i], key.tuple as usize);
            }
            for i in 0..aggregate.len() {
                aggregate[i].append(&mut aggregate_builders[i])
            }
        }
        let mut columns = vec![];
        columns.extend(group_by_builders);
        columns.extend(aggregate_builders);
        columns
    }

    /// Are the group-by columns in row1 equal to the group-by columns in row2?
    fn equal(&self, row1: &Key, row2: &Key) -> bool {
        let batch1 = &self.group_by_batches[row1.batch as usize].group_by;
        let batch2 = &self.group_by_batches[row2.batch as usize].group_by;
        let tuple1 = row1.tuple as usize;
        let tuple2 = row2.tuple as usize;
        for i in 0..batch1.len() {
            match (&batch1[i], &batch2[i]) {
                (AnyArray::Bool(column1), AnyArray::Bool(column2)) => {
                    if column1.get(tuple1) != column2.get(tuple2) {
                        return false;
                    }
                }
                (AnyArray::I64(column1), AnyArray::I64(column2)) => {
                    if column1.get(tuple1) != column2.get(tuple2) {
                        return false;
                    }
                }
                (AnyArray::F64(column1), AnyArray::F64(column2)) => {
                    if column1.get(tuple1) != column2.get(tuple2) {
                        return false;
                    }
                }
                (AnyArray::Date(column1), AnyArray::Date(column2)) => {
                    if column1.get(tuple1) != column2.get(tuple2) {
                        return false;
                    }
                }
                (AnyArray::Timestamp(column1), AnyArray::Timestamp(column2)) => {
                    if column1.get(tuple1) != column2.get(tuple2) {
                        return false;
                    }
                }
                (AnyArray::String(column1), AnyArray::String(column2)) => {
                    if column1.get(tuple1) != column2.get(tuple2) {
                        return false;
                    }
                }
                (column1, column2) => panic!(
                    "expected {:?} but found {:?}",
                    column1.data_type(),
                    column2.data_type()
                ),
            }
        }
        true
    }

    fn hash(&self, row: &Key) -> u64 {
        let bytes = self.group_by_batches[row.batch as usize]
            .hash
            .get(row.tuple as usize)
            .unwrap()
            .to_ne_bytes();
        u64::from_ne_bytes(bytes)
    }
}

#[derive(Default)]
struct KeyHasher {
    value: u64,
}

impl Hasher for KeyHasher {
    fn finish(&self) -> u64 {
        self.value
    }

    fn write(&mut self, _bytes: &[u8]) {
        unimplemented!()
    }

    fn write_u64(&mut self, i: u64) {
        self.value = i;
    }
}

type BuildKeyHasher = BuildHasherDefault<KeyHasher>;

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe {
            // KeyHasher defines write_u64 as a no-op.
            state.write_u64(self.parent.as_ref().unwrap().hash(self))
        }
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        unsafe { self.parent.as_ref().unwrap().equal(self, other) }
    }
}

impl Eq for Key {}

impl Acc {
    fn new(a: &AggregateExpr) -> Self {
        match &a.function {
            AggregateFunction::AnyValue => Self::AnyValue(Value::null(a.input.data_type)),
            AggregateFunction::Count => {
                if a.distinct {
                    Self::CountDistinct(Distinct::new(a.input.data_type))
                } else {
                    Self::Count(0)
                }
            }
            AggregateFunction::LogicalAnd => Self::LogicalAnd(None),
            AggregateFunction::LogicalOr => Self::LogicalOr(None),
            AggregateFunction::Max => Self::Max(Value::null(a.input.data_type)),
            AggregateFunction::Min => Self::Min(Value::null(a.input.data_type)),
            AggregateFunction::Sum => {
                if a.distinct {
                    Self::SumDistinct(Distinct::new(a.input.data_type))
                } else {
                    Self::Sum(Value::null(a.input.data_type))
                }
            }
        }
    }

    fn update(&mut self, column: &AnyArray, tuple: u32) {
        match (self, column) {
            (Acc::AnyValue(Value::Bool(value)), AnyArray::Bool(column)) => {
                *value = column.get(tuple as usize)
            }
            (Acc::AnyValue(Value::I64(value)), AnyArray::I64(column)) => {
                *value = column.get(tuple as usize)
            }
            (Acc::AnyValue(Value::F64(value)), AnyArray::F64(column)) => {
                *value = column.get(tuple as usize)
            }
            (Acc::AnyValue(Value::Date(value)), AnyArray::Date(column)) => {
                *value = column.get(tuple as usize)
            }
            (Acc::AnyValue(Value::Timestamp(value)), AnyArray::Timestamp(column)) => {
                *value = column.get(tuple as usize)
            }
            (Acc::AnyValue(Value::String(value)), AnyArray::String(column)) => {
                *value = column.get(tuple as usize).map(|s| s.to_string())
            }
            (Acc::Count(value), AnyArray::Bool(column)) => {
                if column.get(tuple as usize).is_some() {
                    *value += 1
                }
            }
            (Acc::Count(value), AnyArray::I64(column)) => {
                if column.get(tuple as usize).is_some() {
                    *value += 1
                }
            }
            (Acc::Count(value), AnyArray::F64(column)) => {
                if column.get(tuple as usize).is_some() {
                    *value += 1
                }
            }
            (Acc::Count(value), AnyArray::Date(column)) => {
                if column.get(tuple as usize).is_some() {
                    *value += 1
                }
            }
            (Acc::Count(value), AnyArray::Timestamp(column)) => {
                if column.get(tuple as usize).is_some() {
                    *value += 1
                }
            }
            (Acc::Count(value), AnyArray::String(column)) => {
                if column.get(tuple as usize).is_some() {
                    *value += 1
                }
            }
            (Acc::CountDistinct(Distinct::Bool(hash_set)), AnyArray::Bool(column)) => {
                for i in 0..column.len() {
                    if let Some(next) = column.get(i) {
                        hash_set.insert(next);
                    }
                }
            }
            (Acc::CountDistinct(Distinct::I64(hash_set)), AnyArray::I64(column)) => {
                for i in 0..column.len() {
                    if let Some(next) = column.get(i) {
                        hash_set.insert(next);
                    }
                }
            }
            (Acc::CountDistinct(Distinct::F64(hash_set)), AnyArray::F64(column)) => {
                for i in 0..column.len() {
                    if let Some(next) = column.get(i).map(as_u64) {
                        hash_set.insert(next);
                    }
                }
            }
            (Acc::CountDistinct(Distinct::Date(hash_set)), AnyArray::Date(column)) => {
                for i in 0..column.len() {
                    if let Some(next) = column.get(i) {
                        hash_set.insert(next);
                    }
                }
            }
            (Acc::CountDistinct(Distinct::Timestamp(hash_set)), AnyArray::Timestamp(column)) => {
                for i in 0..column.len() {
                    if let Some(next) = column.get(i) {
                        hash_set.insert(next);
                    }
                }
            }
            (Acc::CountDistinct(Distinct::String(hash_set)), AnyArray::String(column)) => {
                for i in 0..column.len() {
                    if let Some(next) = column.get(i) {
                        hash_set.insert(next);
                    }
                }
            }
            (Acc::LogicalAnd(value), AnyArray::Bool(column)) => {
                let prev = value.unwrap_or(true);
                if let Some(next) = column.get(tuple as usize) {
                    *value = Some(prev && next)
                }
            }
            (Acc::LogicalOr(value), AnyArray::Bool(column)) => {
                let prev = value.unwrap_or(false);
                if let Some(next) = column.get(tuple as usize) {
                    *value = Some(prev || next)
                }
            }
            (Acc::Max(Value::Bool(value)), AnyArray::Bool(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(false);
                    *value = Some(prev.max(next));
                }
            }
            (Acc::Max(Value::I64(value)), AnyArray::I64(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(i64::MIN);
                    *value = Some(prev.max(next));
                }
            }
            (Acc::Max(Value::F64(value)), AnyArray::F64(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(f64::MIN);
                    *value = Some(prev.max(next));
                }
            }
            (Acc::Max(Value::Date(value)), AnyArray::Date(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(i32::MIN);
                    *value = Some(prev.max(next));
                }
            }
            (Acc::Max(Value::Timestamp(value)), AnyArray::Timestamp(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(i64::MIN);
                    *value = Some(prev.max(next));
                }
            }
            (Acc::Max(Value::String(value)), AnyArray::String(column)) => {
                if let Some(next) = column.get_str(tuple as usize) {
                    if let Some(prev) = value {
                        *value = Some(next.max(prev).to_string())
                    } else {
                        *value = Some(next.to_string())
                    }
                }
            }
            (Acc::Min(Value::Bool(value)), AnyArray::Bool(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(true);
                    *value = Some(prev.min(next));
                }
            }
            (Acc::Min(Value::I64(value)), AnyArray::I64(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(i64::MAX);
                    *value = Some(prev.min(next));
                }
            }
            (Acc::Min(Value::F64(value)), AnyArray::F64(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(f64::MAX);
                    *value = Some(prev.min(next));
                }
            }
            (Acc::Min(Value::Date(value)), AnyArray::Date(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(i32::MAX);
                    *value = Some(prev.min(next));
                }
            }
            (Acc::Min(Value::Timestamp(value)), AnyArray::Timestamp(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(i64::MAX);
                    *value = Some(prev.min(next));
                }
            }
            (Acc::Min(Value::String(value)), AnyArray::String(column)) => {
                if let Some(next) = column.get_str(tuple as usize) {
                    if let Some(prev) = value {
                        *value = Some(next.min(prev).to_string())
                    } else {
                        *value = Some(next.to_string())
                    }
                }
            }
            (Acc::Sum(Value::I64(value)), AnyArray::I64(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    *value = Some(value.unwrap_or(0) + next)
                }
            }
            (Acc::Sum(Value::F64(value)), AnyArray::F64(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    *value = Some(value.unwrap_or(0.0) + next)
                }
            }
            (Acc::SumDistinct(Distinct::I64(hash_set)), AnyArray::I64(column)) => {
                for i in 0..column.len() {
                    if let Some(next) = column.get(i) {
                        hash_set.insert(next);
                    }
                }
            }
            (Acc::SumDistinct(Distinct::F64(hash_set)), AnyArray::F64(column)) => {
                for i in 0..column.len() {
                    if let Some(next) = column.get(i).map(as_u64) {
                        hash_set.insert(next);
                    }
                }
            }
            (_, _) => panic!("unmatched aggregate / column"),
        }
    }

    fn append(&self, builder: &mut AnyArray) {
        match self {
            Acc::Count(value) => {
                if let AnyArray::I64(builder) = builder {
                    builder.push(Some(*value))
                } else {
                    panic!("expected i64 but found {:?}", builder.data_type())
                }
            }
            Acc::CountDistinct(distinct) => {
                if let AnyArray::I64(builder) = builder {
                    builder.push(Some(distinct.len()))
                } else {
                    panic!("expected i64 but found {:?}", builder.data_type())
                }
            }
            Acc::LogicalAnd(value) | Acc::LogicalOr(value) => {
                if let AnyArray::Bool(builder) = builder {
                    builder.push(*value)
                } else {
                    panic!("expected bool but found {:?}", builder.data_type())
                }
            }
            Acc::AnyValue(value) | Acc::Max(value) | Acc::Min(value) | Acc::Sum(value) => {
                match (value, builder) {
                    (Value::Bool(value), AnyArray::Bool(builder)) => builder.push(*value),
                    (Value::I64(value), AnyArray::I64(builder)) => builder.push(*value),
                    (Value::F64(value), AnyArray::F64(builder)) => builder.push(*value),
                    (Value::Date(value), AnyArray::Date(builder)) => builder.push(*value),
                    (Value::Timestamp(value), AnyArray::Timestamp(builder)) => builder.push(*value),
                    (Value::String(value), AnyArray::String(builder)) => {
                        if let Some(value) = value {
                            builder.push_str(Some(&value));
                        } else {
                            builder.push(None);
                        }
                    }
                    (value, builder) => panic!(
                        "expected {:?} but found {:?}",
                        value.data_type(),
                        builder.data_type()
                    ),
                }
            }
            Acc::SumDistinct(distinct) => match (distinct, builder) {
                (Distinct::I64(hash_set), AnyArray::I64(builder)) => {
                    if hash_set.is_empty() {
                        builder.push(None);
                    } else {
                        let mut total = 0;
                        for next in hash_set {
                            total += next;
                        }
                        builder.push(Some(total))
                    }
                }
                (Distinct::F64(hash_set), AnyArray::F64(builder)) => {
                    if hash_set.is_empty() {
                        builder.push(None);
                    } else {
                    let mut total = 0.0;
                        for next in hash_set {
                            total += as_f64(*next);
                        }
                        builder.push(Some(total))
                    }
                }
                (value, builder) => panic!(
                    "expected {:?} but found {:?}",
                    value.data_type(),
                    builder.data_type()
                ),
            },
        }
    }

    fn finish(&self) -> AnyArray {
        let mut array = AnyArray::with_capacity(self.data_type(), 1);
        self.append(&mut array);
        array
    }

    fn data_type(&self) -> DataType {
        match self {
            Acc::Count(_) | Acc::CountDistinct(_) => DataType::I64,
            Acc::LogicalAnd(_) | Acc::LogicalOr(_) => DataType::Bool,
            Acc::AnyValue(value) | Acc::Max(value) | Acc::Min(value) | Acc::Sum(value) => {
                value.data_type()
            }
            Acc::SumDistinct(value) => value.data_type(),
        }
    }
}

impl Distinct {
    fn new(data_type: DataType) -> Self {
        match data_type {
            DataType::Bool => Distinct::Bool(HashSet::default()),
            DataType::I64 => Distinct::I64(HashSet::default()),
            DataType::F64 => Distinct::F64(HashSet::default()),
            DataType::Date => Distinct::Date(HashSet::default()),
            DataType::Timestamp => Distinct::Timestamp(HashSet::default()),
            DataType::String => Distinct::String(HashSet::default()),
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            Distinct::Bool(_) => DataType::Bool,
            Distinct::I64(_) => DataType::I64,
            Distinct::F64(_) => DataType::F64,
            Distinct::Date(_) => DataType::Date,
            Distinct::Timestamp(_) => DataType::Timestamp,
            Distinct::String(_) => DataType::String,
        }
    }

    fn len(&self) -> i64 {
        match self {
            Distinct::Bool(hash_set) => hash_set.len() as i64,
            Distinct::I64(hash_set) => hash_set.len() as i64,
            Distinct::F64(hash_set) => hash_set.len() as i64,
            Distinct::Date(hash_set) => hash_set.len() as i64,
            Distinct::Timestamp(hash_set) => hash_set.len() as i64,
            Distinct::String(hash_set) => hash_set.len() as i64,
        }
    }
}

fn as_u64(float: f64) -> u64 {
    unsafe { std::mem::transmute(float) }
}

fn as_f64(int: u64) -> f64 {
    unsafe { std::mem::transmute(int) }
}

fn push(into: &mut AnyArray, from: &AnyArray, i: usize) {
    match (into, from) {
        (AnyArray::Bool(into), AnyArray::Bool(from)) => into.push(from.get(i)),
        (AnyArray::I64(into), AnyArray::I64(from)) => into.push(from.get(i)),
        (AnyArray::F64(into), AnyArray::F64(from)) => into.push(from.get(i)),
        (AnyArray::Date(into), AnyArray::Date(from)) => into.push(from.get(i)),
        (AnyArray::Timestamp(into), AnyArray::Timestamp(from)) => into.push(from.get(i)),
        (AnyArray::String(into), AnyArray::String(from)) => into.push(from.get(i)),
        (into, from) => panic!("{} does not match {}", into.data_type(), from.data_type()),
    }
}
