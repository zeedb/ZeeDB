use ast::*;
use statistics::Statistics;

use crate::{cardinality_estimation::LogicalProps, search_space::*};

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
pub(crate) fn physical_cost(mid: MultiExprID, statistics: &Statistics, ss: &SearchSpace) -> Cost {
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
            let n = statistics[table.id].approx_cardinality();
            n as f64 * SEQ_SCAN
        }
        IndexScan { .. } => {
            let n = ss[parent].props.cardinality;
            n * INDEX_SCAN
        }
        Filter { input, .. } => {
            let n = ss[leaf(input)].props.cardinality;
            n * FILTER
        }
        Map { .. } => {
            let n = ss[parent].props.cardinality;
            n * MAP
        }
        NestedLoop { left, right, .. } => {
            let build = ss[leaf(left)].props.cardinality;
            let probe = ss[leaf(right)].props.cardinality;
            build * probe * NESTED_LOOP
        }
        HashJoin {
            broadcast: true,
            left,
            right,
            ..
        } => {
            let build = ss[leaf(left)].props.cardinality;
            let probe = ss[leaf(right)].props.cardinality;
            build * NODES * HASH_BUILD + probe * HASH_PROBE
        }
        HashJoin {
            broadcast: false,
            left,
            right,
            ..
        } => {
            let build = ss[leaf(left)].props.cardinality;
            let probe = ss[leaf(right)].props.cardinality;
            build * HASH_BUILD + probe * HASH_PROBE
        }
        CreateTempTable { input, .. } => ss[leaf(input)].props.cardinality,
        GetTempTable { .. } => ss[parent].props.cardinality,
        Aggregate { input, .. } => {
            let n = ss[leaf(input)].props.cardinality;
            n * HASH_BUILD
        }
        Sort { .. } => {
            let n = ss[parent].props.cardinality.max(1.0);
            n * n.log2() * SORT
        }
        Broadcast { input } => {
            let n = ss[leaf(input)].props.cardinality;
            n * EXCHANGE * NODES
        }
        Exchange { input } => {
            let n = ss[leaf(input)].props.cardinality;
            n * EXCHANGE
        }
        Insert { input, .. } | Delete { input, .. } => {
            let n = ss[leaf(input)].props.cardinality;
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
    mexpr: &MultiExpr,
    props: &LogicalProps,
    ss: &SearchSpace,
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
