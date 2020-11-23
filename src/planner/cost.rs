use crate::search_space::*;
use ast::*;
use std::collections::HashMap;

pub type Cost = f64;

const BLOCK_SIZE: Cost = 1_000_000.0;
const TUPLE_SIZE: Cost = 100.0;
const COST_READ_BLOCK: Cost = 1.0;
const COST_WRITE_BLOCK: Cost = COST_READ_BLOCK * 2.0;
const COST_CPU_PRED: Cost = 0.0001;
const COST_CPU_EVAL: Cost = COST_CPU_PRED;
const COST_CPU_APPLY: Cost = COST_CPU_PRED * 2.0;
const COST_CPU_COMP_MOVE: Cost = COST_CPU_PRED * 3.0;
const COST_ARRAY_PROBE: Cost = COST_CPU_PRED;
const COST_ARRAY_BUILD: Cost = COST_ARRAY_PROBE * 2.0;
const COST_HASH_PROBE: Cost = COST_ARRAY_PROBE + COST_CPU_PRED;
const COST_HASH_BUILD: Cost = COST_HASH_PROBE * 2.0;

// physicalCost computes the local cost of the physical operator at the head of a multi-expression tree.
// To compute the total physical cost of an expression, you need to choose a single physical expression
// at every node of the tree and add up the local costs.
pub fn physical_cost(ss: &SearchSpace, mid: MultiExprID) -> Cost {
    let parent = ss[mid].parent;
    match &ss[mid].expr {
        TableFreeScan { .. } => 0.0,
        SeqScan {
            predicates, table, ..
        } => {
            let table_cardinality = crate::optimize::table_cardinality(table) as f64;
            let read_blocks = f64::max(1.0, table_cardinality * TUPLE_SIZE / BLOCK_SIZE);
            let count_predicates = predicates.len() as f64;
            read_blocks * COST_READ_BLOCK + count_predicates * table_cardinality * COST_CPU_PRED
        }
        IndexScan { predicates, .. } => {
            let index_cardinality = 1 as f64; // TODO real index cardinality
            let count_predicates = predicates.len() as f64;
            index_cardinality * COST_READ_BLOCK
                + count_predicates * index_cardinality * COST_CPU_PRED
        }
        Filter { predicates, input } => {
            let input = ss[leaf(input)].props.cardinality as f64;
            let columns = predicates.len() as f64;
            input * columns * COST_CPU_PRED
        }
        Map { projects, .. } => {
            let output_cardinality = ss[parent].props.cardinality as f64;
            let count_exprs = projects.iter().filter(|(x, c)| !x.is_just(c)).count() as f64;
            count_exprs * output_cardinality * COST_CPU_EVAL
        }
        NestedLoop { join, left, right } => {
            let build = ss[leaf(left)].props.cardinality as f64;
            let probe = ss[leaf(right)].props.cardinality as f64;
            let iterations = build * probe;
            let count_predicates = join.predicates().len() as f64;
            build * COST_ARRAY_BUILD
                + iterations * COST_ARRAY_PROBE
                + iterations * count_predicates * COST_CPU_PRED
        }
        HashJoin {
            join, left, right, ..
        } => {
            let build = ss[leaf(left)].props.cardinality as f64;
            let probe = ss[leaf(right)].props.cardinality as f64;
            let count_predicates = join.predicates().len() as f64;
            build * COST_HASH_BUILD
                + probe * COST_HASH_PROBE
                + probe * count_predicates * COST_CPU_PRED
        }
        LookupJoin { join, input, .. } => {
            let index_cardinality = ss[leaf(input)].props.cardinality as f64;
            let count_predicates = join.predicates().len() as f64;
            index_cardinality * COST_READ_BLOCK
                + count_predicates * index_cardinality * COST_CPU_PRED
        }
        CreateTempTable { left, .. } => {
            let output = ss[leaf(left)].props.cardinality as f64;
            let blocks = f64::max(1.0, output * TUPLE_SIZE / BLOCK_SIZE);
            blocks * COST_WRITE_BLOCK
        }
        GetTempTable { .. } => {
            let output = ss[parent].props.cardinality as f64;
            let blocks = f64::max(1.0, output * TUPLE_SIZE / BLOCK_SIZE);
            blocks * COST_READ_BLOCK
        }
        Aggregate {
            group_by,
            aggregate,
            input,
        } => {
            let n = ss[leaf(input)].props.cardinality as f64;
            let n_group_by = n * group_by.len() as f64;
            let n_aggregate = n * aggregate.len() as f64;
            n_group_by * COST_HASH_BUILD + n_aggregate * COST_CPU_APPLY
        }
        Limit { .. } => 0.0,
        Sort { .. } => {
            let card = ss[parent].props.cardinality.max(1) as f64;
            let log = 2.0 * card * f64::log2(card);
            log * COST_CPU_COMP_MOVE
        }
        Union { .. } | Intersect { .. } | Except { .. } => 0.0,
        Values { .. } => 0.0,
        Insert { input, .. } | Update { input, .. } | Delete { input, .. } => {
            let length = ss[leaf(input)].props.cardinality as f64;
            let blocks = f64::max(1.0, length * TUPLE_SIZE / BLOCK_SIZE);
            blocks * COST_WRITE_BLOCK
        }
        AlterTable { .. } | Drop { .. } | Rename { .. } | Script { .. } | Assign { .. } => 0.0,
        _ => panic!(),
    }
}

// compute_lower_bound estimates a minimum possible physical cost for mexpr,
// based on a hypothetical idealized query plan that only has to pay
// the cost of joins and reading from disk.
pub fn compute_lower_bound(column_unique_cardinality: &HashMap<Column, usize>) -> Cost {
    // TODO estimate a lower-bound for joins
    fetching_cost(column_unique_cardinality)
}

fn fetching_cost(column_unique_cardinality: &HashMap<Column, usize>) -> Cost {
    let mut total = 0.0;
    for (_, cost) in table_max_cu_cards(column_unique_cardinality) {
        total += cost as Cost * COST_READ_BLOCK;
    }
    total
}

fn table_max_cu_cards(
    column_unique_cardinality: &HashMap<Column, usize>,
) -> HashMap<String, usize> {
    let mut max = HashMap::new();
    for (column, cost) in column_unique_cardinality {
        if let Some(table) = &column.table {
            if cost > max.get(table).unwrap_or(&0) {
                max.insert(table.clone(), *cost);
            }
        }
    }
    max
}
