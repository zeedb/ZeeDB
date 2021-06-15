use ast::*;
use context::WORKER_COUNT_KEY;
use remote_execution::REMOTE_EXECUTION_KEY;

use crate::{cardinality_estimation::LogicalProps, optimize::Optimizer, search_space::*};

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

// physicalCost computes the local cost of the physical operator at the head of a multi-expression tree.
// To compute the total physical cost of an expression, you need to choose a single physical expression
// at every node of the tree and add up the local costs.
pub(crate) fn physical_cost(mid: MultiExprID, opt: &Optimizer) -> Cost {
    let parent = opt.ss[mid].parent;
    match &opt.ss[mid].expr {
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
            let n = opt.context[REMOTE_EXECUTION_KEY].approx_cardinality(table.id);
            n * SEQ_SCAN
        }
        IndexScan { .. } => {
            let n = opt.ss[parent].props.cardinality;
            n * INDEX_SCAN
        }
        Filter { input, .. } => {
            let n = opt.ss[leaf(input)].props.cardinality;
            n * FILTER
        }
        Map { .. } => {
            let n = opt.ss[parent].props.cardinality;
            n * MAP
        }
        NestedLoop { left, right, .. } => {
            let build = opt.ss[leaf(left)].props.cardinality;
            let probe = opt.ss[leaf(right)].props.cardinality;
            build * probe * NESTED_LOOP
        }
        HashJoin {
            broadcast: true,
            left,
            right,
            ..
        } => {
            let build = opt.ss[leaf(left)].props.cardinality;
            let probe = opt.ss[leaf(right)].props.cardinality;
            let workers = opt.context[WORKER_COUNT_KEY] as f64;
            build * workers * HASH_BUILD + probe * HASH_PROBE
        }
        HashJoin {
            broadcast: false,
            left,
            right,
            ..
        } => {
            let build = opt.ss[leaf(left)].props.cardinality;
            let probe = opt.ss[leaf(right)].props.cardinality;
            build * HASH_BUILD + probe * HASH_PROBE
        }
        CreateTempTable { input, .. } => opt.ss[leaf(input)].props.cardinality,
        GetTempTable { .. } => opt.ss[parent].props.cardinality,
        Aggregate { input, .. } => {
            let n = opt.ss[leaf(input)].props.cardinality;
            n * HASH_BUILD
        }
        Sort { .. } => {
            let n = opt.ss[parent].props.cardinality.max(1.0);
            n * n.log2() * SORT
        }
        Broadcast { input, .. } => {
            let n = opt.ss[leaf(input)].props.cardinality;
            let workers = opt.context[WORKER_COUNT_KEY] as f64;
            n * EXCHANGE * workers
        }
        Exchange { input, .. } => {
            let n = opt.ss[leaf(input)].props.cardinality;
            n * EXCHANGE
        }
        Insert { input, .. } | Delete { input, .. } => {
            let n = opt.ss[leaf(input)].props.cardinality;
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
        | LogicalExplain { .. } => panic!("logical operator {}", &opt.ss[mid].expr),
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
