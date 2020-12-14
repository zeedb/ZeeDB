use arrow::array::*;
use arrow::datatypes::*;
use ast::AggregateFn;
use std::sync::Arc;

pub struct HashAggregate {
    slot_mask: BooleanArray,
    group_by_columns: Vec<Arc<dyn Array>>,
    aggregate_fns: Vec<AggregateFn>,
    aggregate_columns: Vec<Arc<dyn Array>>,
}

impl HashAggregate {
    pub fn new(
        group_by_columns: Vec<Arc<dyn Array>>,
        aggregate_fns: Vec<AggregateFn>,
        aggregate_columns: Vec<Arc<dyn Array>>,
    ) -> Self {
        let scatter = find_slots(&group_by_columns);
        let group_by_columns = group_by_columns
            .iter()
            .map(|c| kernel::scatter(c, &scatter))
            .collect();
        let aggregate_columns = (0..aggregate_fns.len())
            .map(|i| aggregate(&aggregate_fns[i], &aggregate_columns[i], &scatter))
            .collect();
        let slot_mask = slot_is_occupied(&scatter);
        HashAggregate {
            slot_mask,
            group_by_columns,
            aggregate_fns,
            aggregate_columns,
        }
    }

    pub fn merge(left: HashAggregate, right: HashAggregate) -> HashAggregate {
        let group_by_columns = (0..left.group_by_columns.len())
            .map(|i| {
                kernel::cat(&vec![
                    left.group_by_columns[i].clone(),
                    right.group_by_columns[i].clone(),
                ])
            })
            .collect();
        let aggregate_columns = (0..left.aggregate_columns.len())
            .map(|i| {
                kernel::cat(&vec![
                    left.aggregate_columns[i].clone(),
                    right.aggregate_columns[i].clone(),
                ])
            })
            .collect();
        HashAggregate::new(group_by_columns, left.aggregate_fns, aggregate_columns)
    }

    pub fn finish(&self) -> Vec<Arc<dyn Array>> {
        let mut columns = vec![];
        for column in &self.group_by_columns {
            columns.push(kernel::gather_logical(column, &self.slot_mask))
        }
        for column in &self.aggregate_columns {
            columns.push(kernel::gather_logical(column, &self.slot_mask))
        }
        columns
    }
}

fn find_slots(group_by_columns: &Vec<Arc<dyn Array>>) -> UInt32Array {
    let num_rows = if group_by_columns.is_empty() {
        1
    } else {
        table_size(group_by_columns[0].len())
    };
    todo!()
}

fn aggregate(
    aggregate_fn: &AggregateFn,
    column: &Arc<dyn Array>,
    scatter: &UInt32Array,
) -> Arc<dyn Array> {
    todo!()
}

fn aggregate_primitive<T: ArrowPrimitiveType>(
    column: &PrimitiveArray<T>,
    scatter: &UInt32Array,
    reduce: impl Fn(T::Native, T::Native) -> T::Native,
) -> PrimitiveArray<T> {
    let len = table_size(column.len());
    let mut is_valid: Vec<bool> = vec![false].repeat(len);
    let mut values: Vec<T::Native> = vec![T::Native::default()].repeat(len);
    for src in 0..column.len() {
        let dst = scatter.value(src) as usize;
        if column.is_valid(src) {
            if is_valid[dst] {
                values[dst] = reduce(values[dst], column.value(src));
            } else {
                is_valid[dst] = true;
                values[dst] = column.value(src)
            }
        }
    }
    let mut builder = PrimitiveBuilder::new(len);
    builder
        .append_values(values.as_slice(), is_valid.as_slice())
        .unwrap();
    builder.finish()
}

fn slot_is_occupied(scatter: &UInt32Array) -> BooleanArray {
    let mut result = vec![false].repeat(table_size(scatter.len()));
    for i in scatter {
        result[i.unwrap() as usize] = true;
    }
    BooleanArray::from(result)
}

fn table_size(n: usize) -> usize {
    let min = n * 4 / 3;
    min.next_power_of_two()
}
