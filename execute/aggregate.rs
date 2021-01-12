use ast::{AggregateExpr, AggregateFunction, Value};
use kernel::*;
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
};

pub struct GroupByAggregate {
    group_by_batches: Vec<Batch>,
    aggregate_slots: HashMap<Key, Vec<Acc>>,
    aggregate_slot_template: Vec<Acc>,
}

struct Batch {
    group_by: Vec<AnyArray>,
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
    pub fn new(aggregate_fns: &Vec<AggregateExpr>) -> Self {
        Self {
            group_by_batches: vec![],
            aggregate_slots: HashMap::new(),
            aggregate_slot_template: aggregate_fns
                .iter()
                .map(|a| Acc::new(&a.function, a.input.data_type))
                .collect(),
        }
    }

    /// Insert a batch of rows into the hash table.
    pub fn insert(&mut self, group_by: Vec<AnyArray>, aggregate: Vec<AnyArray>) {
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
    fn new(operator: &AggregateFunction, data_type: DataType) -> Self {
        match operator {
            AggregateFunction::AnyValue => Self::AnyValue(Value::null(data_type)),
            AggregateFunction::Count => Self::Count(None),
            AggregateFunction::LogicalAnd => Self::LogicalAnd(None),
            AggregateFunction::LogicalOr => Self::LogicalOr(None),
            AggregateFunction::Max => Self::Max(Value::null(data_type)),
            AggregateFunction::Min => Self::Min(Value::null(data_type)),
            AggregateFunction::Sum => Self::Sum(Value::null(data_type)),
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
                    let prev = value.unwrap_or(0);
                    *value = Some(prev + 1)
                }
            }
            (Acc::Count(value), AnyArray::I64(column)) => {
                if column.get(tuple as usize).is_some() {
                    let prev = value.unwrap_or(0);
                    *value = Some(prev + 1)
                }
            }
            (Acc::Count(value), AnyArray::F64(column)) => {
                if column.get(tuple as usize).is_some() {
                    let prev = value.unwrap_or(0);
                    *value = Some(prev + 1)
                }
            }
            (Acc::Count(value), AnyArray::Date(column)) => {
                if column.get(tuple as usize).is_some() {
                    let prev = value.unwrap_or(0);
                    *value = Some(prev + 1)
                }
            }
            (Acc::Count(value), AnyArray::Timestamp(column)) => {
                if column.get(tuple as usize).is_some() {
                    let prev = value.unwrap_or(0);
                    *value = Some(prev + 1)
                }
            }
            (Acc::Count(value), AnyArray::String(column)) => {
                if column.get(tuple as usize).is_some() {
                    let prev = value.unwrap_or(0);
                    *value = Some(prev + 1)
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
                if let Some(next) = column.get(tuple as usize) {
                    if let Some(prev) = value {
                        *value = Some(next.min(prev).to_string())
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
                if let Some(next) = column.get(tuple as usize) {
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
            (_, _) => panic!("unmatched aggregate / column"),
        }
    }

    fn append(&self, builder: &mut AnyArray) {
        match self {
            Acc::Count(value) => {
                if let AnyArray::I64(builder) = builder {
                    builder.push(*value)
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
