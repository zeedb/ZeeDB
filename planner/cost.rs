use crate::search_space::*;
use ast::*;
use storage::Storage;

pub type Cost = f64;

const SEQ_SCAN: Cost = 1.0;
const INDEX_SCAN: Cost = 10.0;
const FILTER: Cost = 1.0;
const MAP: Cost = 1.0;
const NESTED_LOOP: Cost = 1.0;
const HASH_BUILD: Cost = 4.0;
const HASH_PROBE: Cost = 2.0;
const SORT: Cost = 1.0;
const EXCHANGE: Cost = 1.0;
const INSERT: Cost = 1.0;

const NODES: Cost = 10.0; // TODO use the actual # of nodes at runtime.

// physicalCost computes the local cost of the physical operator at the head of a multi-expression tree.
// To compute the total physical cost of an expression, you need to choose a single physical expression
// at every node of the tree and add up the local costs.
pub fn physical_cost(ss: &SearchSpace, storage: &Storage, mid: MultiExprID) -> Cost {
    let parent = ss[mid].parent;
    match &ss[mid].expr {
        TableFreeScan { .. }
        | Out { .. }
        | Limit { .. }
        | Union { .. }
        | Values { .. }
        | Script { .. }
        | Assign { .. }
        | Call { .. }
        | Explain { .. } => 0.0,
        SeqScan { table, .. } => {
            let n = storage.table_cardinality(table.id) as f64;
            n * SEQ_SCAN
        }
        IndexScan { .. } => {
            let n = ss[parent].props.cardinality as f64;
            n * INDEX_SCAN
        }
        Filter { input, .. } => {
            let n = ss[leaf(input)].props.cardinality as f64;
            n * FILTER
        }
        Map { .. } => {
            let n = ss[parent].props.cardinality as f64;
            n * MAP
        }
        NestedLoop { left, right, .. } => {
            let build = ss[leaf(left)].props.cardinality as f64;
            let probe = ss[leaf(right)].props.cardinality as f64;
            build * probe * NESTED_LOOP
        }
        HashJoin {
            broadcast: true,
            left,
            right,
            ..
        } => {
            let build = ss[leaf(left)].props.cardinality as f64;
            let probe = ss[leaf(right)].props.cardinality as f64;
            build * NODES * HASH_BUILD + probe * HASH_PROBE
        }
        HashJoin {
            broadcast: false,
            left,
            right,
            ..
        } => {
            let build = ss[leaf(left)].props.cardinality as f64;
            let probe = ss[leaf(right)].props.cardinality as f64;
            build * HASH_BUILD + probe * HASH_PROBE
        }
        CreateTempTable { input, .. } => ss[leaf(input)].props.cardinality as f64,
        GetTempTable { .. } => ss[parent].props.cardinality as f64,
        Aggregate { input, .. } => {
            let n = ss[leaf(input)].props.cardinality as f64;
            n * HASH_BUILD
        }
        Sort { .. } => {
            let n = ss[parent].props.cardinality.max(1) as f64;
            n * n.log2() * SORT
        }
        Broadcast { input } => {
            let n = ss[leaf(input)].props.cardinality as f64;
            n * EXCHANGE * NODES
        }
        Exchange { input } => {
            let n = ss[leaf(input)].props.cardinality as f64;
            n * EXCHANGE
        }
        Insert { input, .. } | Delete { input, .. } => {
            let n = ss[leaf(input)].props.cardinality as f64;
            n * INSERT
        }
        Leaf { .. }
        | LogicalSingleGet
        | LogicalJoin { .. }
        | LogicalDependentJoin { .. }
        | LogicalWith { .. }
        | LogicalCreateTempTable { .. }
        | LogicalUnion { .. }
        | LogicalFilter { .. }
        | LogicalOut { .. }
        | LogicalMap { .. }
        | LogicalAggregate { .. }
        | LogicalLimit { .. }
        | LogicalSort { .. }
        | LogicalInsert { .. }
        | LogicalValues { .. }
        | LogicalUpdate { .. }
        | LogicalDelete { .. }
        | LogicalGet { .. }
        | LogicalGetWith { .. }
        | LogicalCreateDatabase { .. }
        | LogicalCreateTable { .. }
        | LogicalCreateIndex { .. }
        | LogicalDrop { .. }
        | LogicalRewrite { .. }
        | LogicalScript { .. }
        | LogicalAssign { .. }
        | LogicalCall { .. }
        | LogicalExplain { .. } => panic!("logical operator {}", &ss[mid].expr),
    }
}

pub(crate) fn compute_lower_bound(
    ss: &SearchSpace,
    mexpr: &MultiExpr,
    props: &LogicalProps,
) -> Cost {
    match &mexpr.expr {
        LogicalGet { .. } => props.cardinality as Cost * SEQ_SCAN,
        expr => {
            let mut cost = 0 as Cost;
            for i in 0..expr.len() {
                if let Leaf { gid } = &expr[i] {
                    cost += ss[GroupID(*gid)].lower_bound;
                } else {
                    panic!("expected Leaf but found {:?}", &expr[i])
                }
            }
            cost
        }
    }
}
