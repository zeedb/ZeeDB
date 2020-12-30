use ast::{AggregateFn, Column, Value};
use kernel::*;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

pub struct GroupByAggregate {
    group_by_batches: Vec<Batch>,
    aggregate_slots: HashMap<Key, Vec<Acc>>,
    aggregate_slot_template: Vec<Acc>,
}

struct Batch {
    group_by: Vec<Array>,
    hash: U64Array,
}

struct Key {
    parent: *const GroupByAggregate,
    batch: u32,
    tuple: u32,
}

#[derive(Clone)]
enum Acc {
    AnyValue(Value),
    Count(Option<i64>),
    LogicalAnd(Option<bool>),
    LogicalOr(Option<bool>),
    Max(Value),
    Min(Value),
    Sum(Value),
}

impl GroupByAggregate {
    pub fn new(aggregate_fns: &Vec<(AggregateFn, Column, Column)>) -> Self {
        Self {
            group_by_batches: vec![],
            aggregate_slots: HashMap::new(),
            aggregate_slot_template: aggregate_fns
                .iter()
                .map(|(operator, parameter, _)| Acc::new(operator, parameter.data_type))
                .collect(),
        }
    }

    /// Insert a batch of rows into the hash table.
    pub fn insert(&mut self, group_by: Vec<Array>, aggregate: Vec<Array>) {
        let len = group_by.first().or(aggregate.first()).unwrap().len();
        // Add batch to the universe of tuples that we know about.
        let hash = if group_by.is_empty() {
            U64Array::zeros(len)
        } else {
            U64Array::hash_all(&group_by)
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
    pub fn finish(&self) -> Vec<Array> {
        let num_rows = self.aggregate_slots.len();
        let mut group_by_builders: Vec<Array> = self.group_by_batches[0]
            .group_by
            .iter()
            .map(|c| Array::with_capacity(c.data_type(), num_rows))
            .collect();
        let mut aggregate_builders: Vec<Array> = self
            .aggregate_slot_template
            .iter()
            .map(|a| Array::with_capacity(a.data_type(), num_rows))
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
                (Array::Bool(column1), Array::Bool(column2)) => {
                    if column1.get(tuple1) != column2.get(tuple2) {
                        return false;
                    }
                }
                (Array::I64(column1), Array::I64(column2)) => {
                    if column1.get(tuple1) != column2.get(tuple2) {
                        return false;
                    }
                }
                (Array::F64(column1), Array::F64(column2)) => {
                    if column1.get(tuple1) != column2.get(tuple2) {
                        return false;
                    }
                }
                (Array::Date(column1), Array::Date(column2)) => {
                    if column1.get(tuple1) != column2.get(tuple2) {
                        return false;
                    }
                }
                (Array::Timestamp(column1), Array::Timestamp(column2)) => {
                    if column1.get(tuple1) != column2.get(tuple2) {
                        return false;
                    }
                }
                (Array::String(column1), Array::String(column2)) => {
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
        self.group_by_batches[row.batch as usize]
            .hash
            .get(row.tuple as usize)
            .unwrap()
    }
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe {
            // TODO this is going to hash something that has already been hashed, install a no-op hasher when we set up the map.
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
    fn new(operator: &AggregateFn, data_type: DataType) -> Self {
        match operator {
            AggregateFn::AnyValue => Self::AnyValue(Value::null(data_type)),
            AggregateFn::Count => Self::Count(None),
            AggregateFn::LogicalAnd => Self::LogicalAnd(None),
            AggregateFn::LogicalOr => Self::LogicalOr(None),
            AggregateFn::Max => Self::Max(Value::null(data_type)),
            AggregateFn::Min => Self::Min(Value::null(data_type)),
            AggregateFn::Sum => Self::Sum(Value::null(data_type)),
        }
    }

    fn update(&mut self, column: &Array, tuple: u32) {
        match (self, column) {
            (Acc::AnyValue(Value::Bool(value)), Array::Bool(column)) => {
                *value = column.get(tuple as usize)
            }
            (Acc::AnyValue(Value::I64(value)), Array::I64(column)) => {
                *value = column.get(tuple as usize)
            }
            (Acc::AnyValue(Value::F64(value)), Array::F64(column)) => {
                *value = column.get(tuple as usize)
            }
            (Acc::AnyValue(Value::Date(value)), Array::Date(column)) => {
                *value = column.get(tuple as usize)
            }
            (Acc::AnyValue(Value::Timestamp(value)), Array::Timestamp(column)) => {
                *value = column.get(tuple as usize)
            }
            (Acc::AnyValue(Value::String(value)), Array::String(column)) => {
                *value = column.get(tuple as usize).map(|s| s.to_string())
            }
            (Acc::Count(value), Array::Bool(column)) => {
                if column.get(tuple as usize).is_some() {
                    let prev = value.unwrap_or(0);
                    *value = Some(prev + 1)
                }
            }
            (Acc::Count(value), Array::I64(column)) => {
                if column.get(tuple as usize).is_some() {
                    let prev = value.unwrap_or(0);
                    *value = Some(prev + 1)
                }
            }
            (Acc::Count(value), Array::F64(column)) => {
                if column.get(tuple as usize).is_some() {
                    let prev = value.unwrap_or(0);
                    *value = Some(prev + 1)
                }
            }
            (Acc::Count(value), Array::Date(column)) => {
                if column.get(tuple as usize).is_some() {
                    let prev = value.unwrap_or(0);
                    *value = Some(prev + 1)
                }
            }
            (Acc::Count(value), Array::Timestamp(column)) => {
                if column.get(tuple as usize).is_some() {
                    let prev = value.unwrap_or(0);
                    *value = Some(prev + 1)
                }
            }
            (Acc::Count(value), Array::String(column)) => {
                if column.get(tuple as usize).is_some() {
                    let prev = value.unwrap_or(0);
                    *value = Some(prev + 1)
                }
            }
            (Acc::LogicalAnd(value), Array::Bool(column)) => {
                let prev = value.unwrap_or(true);
                if let Some(next) = column.get(tuple as usize) {
                    *value = Some(prev && next)
                }
            }
            (Acc::LogicalOr(value), Array::Bool(column)) => {
                let prev = value.unwrap_or(false);
                if let Some(next) = column.get(tuple as usize) {
                    *value = Some(prev || next)
                }
            }
            (Acc::Max(Value::Bool(value)), Array::Bool(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(false);
                    *value = Some(prev.max(next));
                }
            }
            (Acc::Max(Value::I64(value)), Array::I64(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(i64::MIN);
                    *value = Some(prev.max(next));
                }
            }
            (Acc::Max(Value::F64(value)), Array::F64(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(f64::MIN);
                    *value = Some(prev.max(next));
                }
            }
            (Acc::Max(Value::Date(value)), Array::Date(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(i32::MIN);
                    *value = Some(prev.max(next));
                }
            }
            (Acc::Max(Value::Timestamp(value)), Array::Timestamp(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(i64::MIN);
                    *value = Some(prev.max(next));
                }
            }
            (Acc::Max(Value::String(value)), Array::String(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    if let Some(prev) = value {
                        *value = Some(next.min(prev).to_string())
                    } else {
                        *value = Some(next.to_string())
                    }
                }
            }
            (Acc::Min(Value::Bool(value)), Array::Bool(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(true);
                    *value = Some(prev.min(next));
                }
            }
            (Acc::Min(Value::I64(value)), Array::I64(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(i64::MAX);
                    *value = Some(prev.min(next));
                }
            }
            (Acc::Min(Value::F64(value)), Array::F64(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(f64::MAX);
                    *value = Some(prev.min(next));
                }
            }
            (Acc::Min(Value::Date(value)), Array::Date(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(i32::MAX);
                    *value = Some(prev.min(next));
                }
            }
            (Acc::Min(Value::Timestamp(value)), Array::Timestamp(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    let prev = value.unwrap_or(i64::MAX);
                    *value = Some(prev.min(next));
                }
            }
            (Acc::Min(Value::String(value)), Array::String(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    if let Some(prev) = value {
                        *value = Some(next.min(prev).to_string())
                    } else {
                        *value = Some(next.to_string())
                    }
                }
            }
            (Acc::Sum(Value::I64(value)), Array::I64(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    *value = Some(value.unwrap_or(0) + next)
                }
            }
            (Acc::Sum(Value::F64(value)), Array::F64(column)) => {
                if let Some(next) = column.get(tuple as usize) {
                    *value = Some(value.unwrap_or(0.0) + next)
                }
            }
            (_, _) => panic!("unmatched aggregate / column"),
        }
    }

    fn append(&self, builder: &mut Array) {
        match self {
            Acc::Count(value) => {
                if let Array::I64(builder) = builder {
                    builder.push(*value)
                } else {
                    panic!("expected i64 but found {:?}", builder.data_type())
                }
            }
            Acc::LogicalAnd(value) | Acc::LogicalOr(value) => {
                if let Array::Bool(builder) = builder {
                    builder.push(*value)
                } else {
                    panic!("expected bool but found {:?}", builder.data_type())
                }
            }
            Acc::AnyValue(value) | Acc::Max(value) | Acc::Min(value) | Acc::Sum(value) => {
                match (value, builder) {
                    (Value::Bool(value), Array::Bool(builder)) => builder.push(*value),
                    (Value::I64(value), Array::I64(builder)) => builder.push(*value),
                    (Value::F64(value), Array::F64(builder)) => builder.push(*value),
                    (Value::Date(value), Array::Date(builder)) => builder.push(*value),
                    (Value::Timestamp(value), Array::Timestamp(builder)) => builder.push(*value),
                    (Value::String(value), Array::String(builder)) => {
                        if let Some(value) = value {
                            builder.push(Some(&value));
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
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            Acc::Count(_) => DataType::I64,
            Acc::LogicalAnd(_) | Acc::LogicalOr(_) => DataType::Bool,
            Acc::AnyValue(value) | Acc::Max(value) | Acc::Min(value) | Acc::Sum(value) => {
                value.data_type()
            }
        }
    }
}

fn push(into: &mut Array, from: &Array, i: usize) {
    match (into, from) {
        (Array::Bool(into), Array::Bool(from)) => into.push(from.get(i)),
        (Array::I64(into), Array::I64(from)) => into.push(from.get(i)),
        (Array::F64(into), Array::F64(from)) => into.push(from.get(i)),
        (Array::Date(into), Array::Date(from)) => into.push(from.get(i)),
        (Array::Timestamp(into), Array::Timestamp(from)) => into.push(from.get(i)),
        (Array::String(into), Array::String(from)) => into.push(from.get(i)),
        (into, from) => panic!("{} does not match {}", into.data_type(), from.data_type()),
    }
}
