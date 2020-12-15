use arrow::array::*;
use arrow::datatypes::*;
use ast::{AggregateFn, Column};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub struct GroupByAggregate {
    group_by_batches: Vec<Batch>,
    aggregate_slots: HashMap<Key, Vec<Acc>>,
    aggregate_slot_template: Vec<Acc>,
    string_pool: Vec<String>,
}

struct Batch {
    group_by: Vec<Arc<dyn Array>>,
    hash: UInt32Array,
}

struct Key {
    parent: *const GroupByAggregate,
    batch: u32,
    tuple: u32,
}

#[derive(Clone)]
enum Acc {
    AnyValue { val: Val },
    Count { val: Option<i64> },
    LogicalAnd { val: Option<bool> },
    LogicalOr { val: Option<bool> },
    Max { val: Val },
    Min { val: Val },
    Sum { val: Val },
}

#[derive(Clone)]
enum Val {
    Boolean(Option<bool>),
    Int64(Option<i64>),
    Float64(Option<f64>),
    Timestamp(Option<i64>),
    Date(Option<i32>),
    Utf8(Option<usize>),
}

impl GroupByAggregate {
    pub fn new(aggregate_fns: &Vec<(AggregateFn, Column, Column)>) -> Self {
        Self {
            group_by_batches: vec![],
            aggregate_slots: HashMap::new(),
            aggregate_slot_template: aggregate_fns
                .iter()
                .map(|(operator, parameter, _)| Acc::new(operator, &parameter.data_type))
                .collect(),
            string_pool: vec![],
        }
    }

    /// Insert a batch of rows into the hash table.
    pub fn insert(&mut self, group_by: Vec<Arc<dyn Array>>, aggregate: Vec<Arc<dyn Array>>) {
        let len = aggregate.first().unwrap().len();
        // Add batch to the universe of tuples that we know about.
        let hash = if group_by.is_empty() {
            zeros(len)
        } else {
            kernel::hash(&group_by)
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
    pub fn finish(&self) -> Vec<Arc<dyn Array>> {
        let num_rows = self.aggregate_slots.len();
        let mut group_by_builders: Vec<Box<dyn ArrayBuilder>> = self
            .group_by_batches
            .first()
            .unwrap()
            .group_by
            .iter()
            .map(|c| array_builder(c.data_type(), num_rows))
            .collect();
        let mut aggregate_builders: Vec<Box<dyn ArrayBuilder>> = self
            .aggregate_slot_template
            .iter()
            .map(|a| array_builder(&a.data_type(), num_rows))
            .collect();
        for (key, aggregate) in &self.aggregate_slots {
            let group_by = &self.group_by_batches[key.batch as usize].group_by;
            for i in 0..group_by_builders.len() {
                append(&mut group_by_builders[i], &group_by[i], key.tuple);
            }
            for i in 0..aggregate_builders.len() {
                aggregate[i].append(&mut aggregate_builders[i]);
            }
        }
        let mut columns = vec![];
        for builder in &mut group_by_builders {
            columns.push(builder.finish());
        }
        for builder in &mut aggregate_builders {
            columns.push(builder.finish());
        }
        columns
    }

    /// Are the group-by columns in row1 equal to the group-by columns in row2?
    fn equal(&self, row1: &Key, row2: &Key) -> bool {
        let batch1 = &self.group_by_batches[row1.batch as usize].group_by;
        let batch2 = &self.group_by_batches[row2.batch as usize].group_by;
        let tuple1 = row1.tuple as usize;
        let tuple2 = row2.tuple as usize;
        for i in 0..batch1.len() {
            if !equal(&batch1[i], tuple1, &batch2[i], tuple2) {
                return false;
            }
        }
        true
    }

    fn hash(&self, row: &Key) -> u32 {
        self.group_by_batches[row.batch as usize]
            .hash
            .value(row.tuple as usize)
    }
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe {
            // TODO this is going to hash something that has already been hashed, install a no-op hasher when we set up the map.
            state.write_u32(self.parent.as_ref().unwrap().hash(self))
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
    fn new(operator: &AggregateFn, data_type: &DataType) -> Self {
        match operator {
            AggregateFn::AnyValue => Self::AnyValue {
                val: Val::new(data_type),
            },
            AggregateFn::Count => Self::Count { val: None },
            AggregateFn::LogicalAnd => Self::LogicalAnd { val: None },
            AggregateFn::LogicalOr => Self::LogicalOr { val: None },
            AggregateFn::Max => Self::Max {
                val: Val::new(data_type),
            },
            AggregateFn::Min => Self::Min {
                val: Val::new(data_type),
            },
            AggregateFn::Sum => Self::Sum {
                val: Val::new(data_type),
            },
        }
    }

    fn update(&mut self, column: &Arc<dyn Array>, tuple: u32) {
        match self {
            Acc::AnyValue { val } => val.set(column, tuple),
            Acc::Count { val } => {
                if column.is_valid(tuple as usize) {
                    let prev = val.unwrap_or(0);
                    *val = Some(prev + 1)
                }
            }
            Acc::LogicalAnd { val } => {
                if column.is_valid(tuple as usize) {
                    let prev = val.unwrap_or(true);
                    let next = as_boolean_array(column).value(tuple as usize);
                    *val = Some(prev && next)
                }
            }
            Acc::LogicalOr { val } => {
                if column.is_valid(tuple as usize) {
                    let prev = val.unwrap_or(false);
                    let next = as_boolean_array(column).value(tuple as usize);
                    *val = Some(prev || next)
                }
            }
            Acc::Max { val } => {
                if column.is_valid(tuple as usize) {
                    match val {
                        Val::Boolean(val) => {
                            let prev = val.unwrap_or(false);
                            let next = as_boolean_array(column).value(tuple as usize);
                            *val = Some(prev.max(next))
                        }
                        Val::Int64(val) => {
                            let prev = val.unwrap_or(i64::MIN);
                            let next =
                                as_primitive_array::<Int64Type>(column).value(tuple as usize);
                            *val = Some(prev.max(next))
                        }
                        Val::Float64(val) => {
                            let prev = val.unwrap_or(f64::MIN);
                            let next =
                                as_primitive_array::<Float64Type>(column).value(tuple as usize);
                            *val = Some(prev.max(next))
                        }
                        Val::Timestamp(val) => {
                            let prev = val.unwrap_or(i64::MIN);
                            let next = as_primitive_array::<TimestampMicrosecondType>(column)
                                .value(tuple as usize);
                            *val = Some(prev.max(next))
                        }
                        Val::Date(val) => {
                            let prev = val.unwrap_or(i32::MIN);
                            let next =
                                as_primitive_array::<Date32Type>(column).value(tuple as usize);
                            *val = Some(prev.max(next))
                        }
                        Val::Utf8(val) => todo!("use string pool"),
                    }
                }
            }
            Acc::Min { val } => {
                if column.is_valid(tuple as usize) {
                    match val {
                        Val::Boolean(val) => {
                            let prev = val.unwrap_or(true);
                            let next = as_boolean_array(column).value(tuple as usize);
                            *val = Some(prev.min(next))
                        }
                        Val::Int64(val) => {
                            let prev = val.unwrap_or(i64::MAX);
                            let next =
                                as_primitive_array::<Int64Type>(column).value(tuple as usize);
                            *val = Some(prev.min(next))
                        }
                        Val::Float64(val) => {
                            let prev = val.unwrap_or(f64::MAX);
                            let next =
                                as_primitive_array::<Float64Type>(column).value(tuple as usize);
                            *val = Some(prev.min(next))
                        }
                        Val::Timestamp(val) => {
                            let prev = val.unwrap_or(i64::MAX);
                            let next = as_primitive_array::<TimestampMicrosecondType>(column)
                                .value(tuple as usize);
                            *val = Some(prev.min(next))
                        }
                        Val::Date(val) => {
                            let prev = val.unwrap_or(i32::MAX);
                            let next =
                                as_primitive_array::<Date32Type>(column).value(tuple as usize);
                            *val = Some(prev.min(next))
                        }
                        Val::Utf8(val) => todo!("use string pool"),
                    }
                }
            }
            Acc::Sum { val } => {
                if column.is_valid(tuple as usize) {
                    match val {
                        Val::Int64(val) => {
                            let prev = val.unwrap_or(0);
                            let next =
                                as_primitive_array::<Int64Type>(column).value(tuple as usize);
                            *val = Some(prev + next)
                        }
                        Val::Float64(val) => {
                            let prev = val.unwrap_or(0.0);
                            let next =
                                as_primitive_array::<Float64Type>(column).value(tuple as usize);
                            *val = Some(prev + next)
                        }
                        Val::Boolean(_) | Val::Timestamp(_) | Val::Date(_) | Val::Utf8(_) => {
                            panic!("sum({:?}) is not supported", val.data_type())
                        }
                    }
                }
            }
        }
    }

    fn append(&self, builder: &mut Box<dyn ArrayBuilder>) {
        match self {
            Acc::Count { val } => Val::Int64(*val).append(builder),
            Acc::LogicalAnd { val } | Acc::LogicalOr { val } => Val::Boolean(*val).append(builder),
            Acc::AnyValue { val } | Acc::Max { val } | Acc::Min { val } | Acc::Sum { val } => {
                val.append(builder)
            }
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            Acc::Count { .. } => DataType::Int64,
            Acc::LogicalAnd { .. } | Acc::LogicalOr { .. } => DataType::Boolean,
            Acc::AnyValue { val } | Acc::Max { val } | Acc::Min { val } | Acc::Sum { val } => {
                val.data_type()
            }
        }
    }
}

impl Val {
    fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::Boolean => Self::Boolean(None),
            DataType::Int64 => Self::Int64(None),
            DataType::Float64 => Self::Float64(None),
            DataType::Date32(DateUnit::Day) => Self::Date(None),
            DataType::Timestamp(TimeUnit::Microsecond, None) => Self::Timestamp(None),
            DataType::Utf8 => Self::Utf8(None),
            other => panic!("{:?} not supported", other),
        }
    }

    fn set(&mut self, column: &Arc<dyn Array>, tuple: u32) {
        if column.is_valid(tuple as usize) {
            match self {
                Val::Boolean(val) => *val = Some(as_boolean_array(column).value(tuple as usize)),
                Val::Int64(val) => {
                    *val = Some(as_primitive_array::<Int64Type>(column).value(tuple as usize))
                }
                Val::Float64(val) => {
                    *val = Some(as_primitive_array::<Float64Type>(column).value(tuple as usize))
                }
                Val::Timestamp(val) => {
                    *val = Some(
                        as_primitive_array::<TimestampMicrosecondType>(column)
                            .value(tuple as usize),
                    )
                }
                Val::Date(val) => {
                    *val = Some(as_primitive_array::<Date32Type>(column).value(tuple as usize))
                }
                Val::Utf8(val) => todo!("use string pool"),
            }
        } else {
            match self {
                Val::Boolean(val) => *val = None,
                Val::Int64(val) => *val = None,
                Val::Float64(val) => *val = None,
                Val::Timestamp(val) => *val = None,
                Val::Date(val) => *val = None,
                Val::Utf8(val) => *val = None,
            }
        }
    }

    fn append(&self, builder: &mut Box<dyn ArrayBuilder>) {
        match self {
            Val::Boolean(val) => {
                let builder: &mut BooleanBuilder = builder
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .unwrap();
                if let Some(val) = val {
                    builder.append_value(*val);
                } else {
                    builder.append_null();
                }
            }
            Val::Int64(val) => {
                let builder: &mut Int64Builder =
                    builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
                if let Some(val) = val {
                    builder.append_value(*val);
                } else {
                    builder.append_null();
                }
            }
            Val::Float64(val) => {
                let builder: &mut Float64Builder = builder
                    .as_any_mut()
                    .downcast_mut::<Float64Builder>()
                    .unwrap();
                if let Some(val) = val {
                    builder.append_value(*val);
                } else {
                    builder.append_null();
                }
            }
            Val::Date(val) => {
                let builder: &mut Date32Builder = builder
                    .as_any_mut()
                    .downcast_mut::<Date32Builder>()
                    .unwrap();
                if let Some(val) = val {
                    builder.append_value(*val);
                } else {
                    builder.append_null();
                }
            }
            Val::Timestamp(val) => {
                let builder: &mut TimestampMicrosecondBuilder = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMicrosecondBuilder>()
                    .unwrap();
                if let Some(val) = val {
                    builder.append_value(*val);
                } else {
                    builder.append_null();
                }
            }
            Val::Utf8(val) => todo!("use string pool"),
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            Val::Boolean(_) => DataType::Boolean,
            Val::Int64(_) => DataType::Int64,
            Val::Float64(_) => DataType::Float64,
            Val::Timestamp(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
            Val::Date(_) => DataType::Date32(DateUnit::Day),
            Val::Utf8(_) => DataType::Utf8,
        }
    }
}

/// Is column1[row1] == column2[row2]?
fn equal(column1: &Arc<dyn Array>, row1: usize, column2: &Arc<dyn Array>, row2: usize) -> bool {
    match column1.data_type() {
        DataType::Boolean => equal_boolean(
            as_boolean_array(column1),
            row1,
            as_boolean_array(column2),
            row2,
        ),
        DataType::Int64 => equal_primitive(
            as_primitive_array::<Int64Type>(column1),
            row1,
            as_primitive_array::<Int64Type>(column2),
            row2,
        ),
        DataType::Float64 => equal_primitive(
            as_primitive_array::<Float64Type>(column1),
            row1,
            as_primitive_array::<Float64Type>(column2),
            row2,
        ),
        DataType::Date32(DateUnit::Day) => equal_primitive(
            as_primitive_array::<Date32Type>(column1),
            row1,
            as_primitive_array::<Date32Type>(column2),
            row2,
        ),
        DataType::Timestamp(TimeUnit::Microsecond, None) => equal_primitive(
            as_primitive_array::<TimestampMicrosecondType>(column1),
            row1,
            as_primitive_array::<TimestampMicrosecondType>(column2),
            row2,
        ),
        DataType::Utf8 => equal_string(
            as_string_array(column1),
            row1,
            as_string_array(column2),
            row2,
        ),
        other => panic!("{:?} not supported", other),
    }
}

fn equal_boolean(
    column1: &PrimitiveArray<BooleanType>,
    row1: usize,
    column2: &PrimitiveArray<BooleanType>,
    row2: usize,
) -> bool {
    column1.is_null(row1) && column2.is_null(row2)
        || column1.is_null(row1) == column2.is_null(row2)
            && column1.value(row1) == column2.value(row2)
}

fn equal_primitive<T: ArrowPrimitiveType>(
    column1: &PrimitiveArray<T>,
    row1: usize,
    column2: &PrimitiveArray<T>,
    row2: usize,
) -> bool {
    column1.is_null(row1) && column2.is_null(row2)
        || column1.is_null(row1) == column2.is_null(row2)
            && column1.value(row1) == column2.value(row2)
}

fn equal_string(
    column1: &GenericStringArray<i32>,
    row1: usize,
    column2: &GenericStringArray<i32>,
    row2: usize,
) -> bool {
    column1.is_null(row1) && column2.is_null(row2)
        || column1.is_null(row1) == column2.is_null(row2)
            && column1.value(row1) == column2.value(row2)
}

fn zeros(len: usize) -> UInt32Array {
    UInt32Array::from(vec![0].repeat(len))
}

fn array_builder(data_type: &DataType, capacity: usize) -> Box<dyn ArrayBuilder> {
    match data_type {
        DataType::Boolean => Box::new(BooleanArray::builder(capacity)),
        DataType::Int64 => Box::new(Int64Array::builder(capacity)),
        DataType::Float64 => Box::new(Float64Array::builder(capacity)),
        DataType::Date32(DateUnit::Day) => Box::new(Date32Array::builder(capacity)),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Box::new(TimestampMicrosecondArray::builder(capacity))
        }
        DataType::Utf8 => Box::new(StringBuilder::new(capacity)),
        other => panic!("{:?} not supported", other),
    }
}

fn append(builder: &mut Box<dyn ArrayBuilder>, column: &Arc<dyn Array>, tuple: u32) {
    match builder.data_type() {
        DataType::Boolean => {
            let builder: &mut BooleanBuilder = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap();
            let column: &BooleanArray = column.as_any().downcast_ref::<BooleanArray>().unwrap();
            if column.is_valid(tuple as usize) {
                builder.append_value(column.value(tuple as usize)).unwrap();
            } else {
                builder.append_null().unwrap();
            }
        }
        DataType::Int64 => {
            let builder: &mut Int64Builder =
                builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
            let column: &Int64Array = column.as_any().downcast_ref::<Int64Array>().unwrap();
            if column.is_valid(tuple as usize) {
                builder.append_value(column.value(tuple as usize)).unwrap();
            } else {
                builder.append_null().unwrap();
            }
        }
        DataType::Float64 => {
            let builder: &mut Float64Builder = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap();
            let column: &Float64Array = column.as_any().downcast_ref::<Float64Array>().unwrap();
            if column.is_valid(tuple as usize) {
                builder.append_value(column.value(tuple as usize)).unwrap();
            } else {
                builder.append_null().unwrap();
            }
        }
        DataType::Date32(DateUnit::Day) => {
            let builder: &mut Date32Builder = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .unwrap();
            let column: &Date32Array = column.as_any().downcast_ref::<Date32Array>().unwrap();
            if column.is_valid(tuple as usize) {
                builder.append_value(column.value(tuple as usize)).unwrap();
            } else {
                builder.append_null().unwrap();
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let builder: &mut TimestampMicrosecondBuilder = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .unwrap();
            let column: &TimestampMicrosecondArray = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            if column.is_valid(tuple as usize) {
                builder.append_value(column.value(tuple as usize)).unwrap();
            } else {
                builder.append_null().unwrap();
            }
        }
        DataType::Utf8 => {
            let builder: &mut StringBuilder = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            let column: &StringArray = column.as_any().downcast_ref::<StringArray>().unwrap();
            if column.is_valid(tuple as usize) {
                builder.append_value(column.value(tuple as usize)).unwrap();
            } else {
                builder.append_null().unwrap();
            }
        }
        other => panic!("{:?} not supported", other),
    }
}
