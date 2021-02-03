use crate::{optimize::Optimizer, search_space::*};
use ast::*;
use catalog::{Catalog, CATALOG_KEY};
use std::collections::HashSet;

#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub(crate) enum Rule {
    // Rewrite rules
    InnerJoinCommutivity,
    InnerJoinAssociativity,
    // Enforcers.
    InsertBroadcast,
    InsertExchange,
    // Implementation rules
    LogicalGetToTableFreeScan,
    LogicalGetToSeqScan,
    LogicalGetToIndexScan,
    LogicalFilterToFilter,
    LogicalOutToOut,
    LogicalMapToMap,
    LogicalJoinToNestedLoop,
    LogicalJoinToBroadcastHashJoin,
    LogicalJoinToExchangeHashJoin,
    LogicalJoinToIndexScan,
    LogicalAggregateToAggregate,
    LogicalLimitToLimit,
    LogicalSortToSort,
    LogicallUnionToUnion,
    LogicalCreateTempTableToCreateTempTable,
    LogicalGetWithToGetTempTable,
    LogicalInsertToInsert,
    LogicalValuesToValues,
    LogicalDeleteToDelete,
    LogicalAssignToAssign,
    LogicalCallToCall,
    LogicalExplainToExplain,
    LogicalScriptToScript,
}

impl Rule {
    pub fn output_is_physical(&self) -> bool {
        match self {
            Rule::InsertBroadcast
            | Rule::InsertExchange
            | Rule::LogicalGetToTableFreeScan
            | Rule::LogicalGetToSeqScan
            | Rule::LogicalGetToIndexScan
            | Rule::LogicalFilterToFilter
            | Rule::LogicalOutToOut
            | Rule::LogicalMapToMap
            | Rule::LogicalJoinToNestedLoop
            | Rule::LogicalJoinToBroadcastHashJoin
            | Rule::LogicalJoinToExchangeHashJoin
            | Rule::LogicalJoinToIndexScan
            | Rule::LogicalAggregateToAggregate
            | Rule::LogicalLimitToLimit
            | Rule::LogicalSortToSort
            | Rule::LogicallUnionToUnion
            | Rule::LogicalCreateTempTableToCreateTempTable
            | Rule::LogicalGetWithToGetTempTable
            | Rule::LogicalInsertToInsert
            | Rule::LogicalValuesToValues
            | Rule::LogicalDeleteToDelete
            | Rule::LogicalAssignToAssign
            | Rule::LogicalScriptToScript => true,
            _ => false,
        }
    }

    // Quickly check if rule matches expression *without* exploring the inputs to the expression.
    pub fn matches_fast(&self, mexpr: &MultiExpr, required: PhysicalProp) -> bool {
        self.matches_expr(mexpr) && self.matches_required(required)
    }

    fn matches_expr(&self, mexpr: &MultiExpr) -> bool {
        match (self, &mexpr.expr) {
            // Only inner joins are commutative.
            (Rule::InnerJoinCommutivity, LogicalJoin { join, .. })
            | (Rule::InnerJoinAssociativity, LogicalJoin { join, .. }) => match join {
                Join::Inner(_) => true,
                Join::Right(_)
                | Join::Outer(_)
                | Join::Semi(_)
                | Join::Anti(_)
                | Join::Single(_)
                | Join::Mark(_, _) => false,
            },
            // Index join is only available for inner joins.
            // TODO: in principle, index scan can work for anything but outer joins.
            (Rule::LogicalJoinToIndexScan, LogicalJoin { join, .. }) => match join {
                Join::Inner(_) => true,
                Join::Right(_)
                | Join::Outer(_)
                | Join::Semi(_)
                | Join::Anti(_)
                | Join::Single(_)
                | Join::Mark(_, _) => false,
            },
            (Rule::LogicalJoinToNestedLoop, LogicalJoin { .. })
            | (Rule::LogicalJoinToBroadcastHashJoin, LogicalJoin { .. })
            | (Rule::LogicalJoinToExchangeHashJoin, LogicalJoin { .. }) => true,
            (Rule::LogicalGetToTableFreeScan, LogicalSingleGet)
            | (Rule::LogicalGetToSeqScan, LogicalGet { .. })
            | (Rule::LogicalGetToIndexScan, LogicalGet { .. })
            | (Rule::LogicalFilterToFilter, LogicalFilter { .. })
            | (Rule::LogicalOutToOut, LogicalOut { .. })
            | (Rule::LogicalMapToMap, LogicalMap { .. })
            | (Rule::LogicalAggregateToAggregate, LogicalAggregate { .. })
            | (Rule::LogicalLimitToLimit, LogicalLimit { .. })
            | (Rule::LogicalSortToSort, LogicalSort { .. })
            | (Rule::LogicallUnionToUnion, LogicalUnion { .. })
            | (Rule::LogicalCreateTempTableToCreateTempTable, LogicalCreateTempTable { .. })
            | (Rule::LogicalGetWithToGetTempTable, LogicalGetWith { .. })
            | (Rule::LogicalInsertToInsert, LogicalInsert { .. })
            | (Rule::LogicalValuesToValues, LogicalValues { .. })
            | (Rule::LogicalDeleteToDelete, LogicalDelete { .. })
            | (Rule::LogicalAssignToAssign, LogicalAssign { .. })
            | (Rule::LogicalCallToCall, LogicalCall { .. })
            | (Rule::LogicalExplainToExplain, LogicalExplain { .. })
            | (Rule::LogicalScriptToScript, LogicalScript { .. })
            | (Rule::InsertBroadcast, _)
            | (Rule::InsertExchange, _) => true,
            _ => false,
        }
    }

    fn matches_required(&self, required: PhysicalProp) -> bool {
        match self {
            Rule::InnerJoinCommutivity
            | Rule::InnerJoinAssociativity
            | Rule::LogicalGetToTableFreeScan
            | Rule::LogicalGetToSeqScan
            | Rule::LogicalGetToIndexScan
            | Rule::LogicalFilterToFilter
            | Rule::LogicalOutToOut
            | Rule::LogicalMapToMap
            | Rule::LogicalJoinToNestedLoop
            | Rule::LogicalJoinToBroadcastHashJoin
            | Rule::LogicalJoinToExchangeHashJoin
            | Rule::LogicalJoinToIndexScan
            | Rule::LogicalAggregateToAggregate
            | Rule::LogicalLimitToLimit
            | Rule::LogicalSortToSort
            | Rule::LogicallUnionToUnion
            | Rule::LogicalCreateTempTableToCreateTempTable
            | Rule::LogicalGetWithToGetTempTable
            | Rule::LogicalInsertToInsert
            | Rule::LogicalValuesToValues
            | Rule::LogicalDeleteToDelete
            | Rule::LogicalAssignToAssign
            | Rule::LogicalCallToCall
            | Rule::LogicalExplainToExplain
            | Rule::LogicalScriptToScript => required == PhysicalProp::None,
            Rule::InsertBroadcast => required == PhysicalProp::BroadcastDist,
            Rule::InsertExchange => required == PhysicalProp::ExchangeDist,
        }
    }

    pub fn non_leaf(&self, child: usize) -> bool {
        match (self, child) {
            (Rule::InnerJoinAssociativity, 0) => true,
            (Rule::LogicalJoinToIndexScan, 0) => true,
            _ => false,
        }
    }

    pub fn bind(&self, ss: &SearchSpace, mid: MultiExprID) -> Vec<Expr> {
        let mut binds = vec![];
        match self {
            Rule::InnerJoinAssociativity => {
                if let LogicalJoin {
                    join: Join::Inner(parent_predicates),
                    left,
                    right,
                } = &ss[mid].expr
                {
                    if let Leaf { gid: left } = left.as_ref() {
                        for left in &ss[GroupID(*left)].logical {
                            if let LogicalJoin {
                                join: Join::Inner(left_predicates),
                                left: left_left,
                                right: left_middle,
                                ..
                            } = &ss[*left].expr
                            {
                                binds.push(LogicalJoin {
                                    join: Join::Inner(parent_predicates.clone()),
                                    left: Box::new(LogicalJoin {
                                        join: Join::Inner(left_predicates.clone()),
                                        left: left_left.clone(),
                                        right: left_middle.clone(),
                                    }),
                                    right: right.clone(),
                                })
                            }
                        }
                    }
                }
            }
            Rule::LogicalJoinToIndexScan => {
                if let LogicalJoin { join, left, right } = &ss[mid].expr {
                    if let Leaf { gid: left } = left.as_ref() {
                        for left in &ss[GroupID(*left)].logical {
                            if let LogicalGet {
                                predicates,
                                projects,
                                table,
                            } = &ss[*left].expr
                            {
                                binds.push(LogicalJoin {
                                    join: join.clone(),
                                    left: Box::new(LogicalGet {
                                        predicates: predicates.clone(),
                                        projects: projects.clone(),
                                        table: table.clone(),
                                    }),
                                    right: right.clone(),
                                })
                            }
                        }
                    }
                }
            }
            Rule::InsertBroadcast | Rule::InsertExchange => binds.push(Leaf {
                gid: ss[mid].parent.0,
            }),
            _ => binds.push(ss[mid].expr.clone().map(|group| group)),
        }
        binds
    }

    pub fn apply(&self, bind: Expr, parent: &Optimizer) -> Option<Expr> {
        match self {
            Rule::InnerJoinCommutivity => {
                if let LogicalJoin {
                    join: Join::Inner(join_predicates),
                    left,
                    right,
                    ..
                } = bind
                {
                    return Some(LogicalJoin {
                        join: Join::Inner(join_predicates.clone()),
                        left: right,
                        right: left,
                    });
                }
            }
            // Rearrange left-deep join into right-deep join.
            //               parent
            //                +  +
            //                +  +
            //        left_join  right
            //         +     +
            //         +     +
            // left_left   left_middle
            //
            //               parent
            //                +  +
            //                +  +
            //        left_left  right_join
            //                    +     +
            //                    +     +
            //            left_middle  right
            Rule::InnerJoinAssociativity => {
                if let LogicalJoin {
                    join: Join::Inner(parent_predicates),
                    left,
                    right,
                    ..
                } = bind
                {
                    if let Leaf { gid: right } = right.as_ref() {
                        if let LogicalJoin {
                            join: Join::Inner(left_predicates),
                            left: left_left,
                            right: left_middle,
                            ..
                        } = *left
                        {
                            if let (Leaf { gid: left_left }, Leaf { gid: left_middle }) =
                                (left_left.as_ref(), left_middle.as_ref())
                            {
                                let mut new_parent_predicates = vec![];
                                let mut new_right_predicates = vec![];
                                let middle_scope = &parent.ss[GroupID(*left_middle)].props.columns;
                                let right_scope = &parent.ss[GroupID(*right)].props.columns;
                                let mut redistribute_predicate = |p: Scalar| {
                                    if p.references().iter().all(|c| {
                                        middle_scope.contains_key(c) || right_scope.contains_key(c)
                                    }) {
                                        new_right_predicates.push(p);
                                    } else {
                                        new_parent_predicates.push(p);
                                    }
                                };
                                for p in parent_predicates {
                                    redistribute_predicate(p);
                                }
                                for p in left_predicates {
                                    redistribute_predicate(p);
                                }
                                return Some(LogicalJoin {
                                    join: Join::Inner(new_parent_predicates),
                                    left: Box::new(Leaf { gid: *left_left }),
                                    right: Box::new(LogicalJoin {
                                        join: Join::Inner(new_right_predicates),
                                        left: Box::new(Leaf { gid: *left_middle }),
                                        right: Box::new(Leaf { gid: *right }),
                                    }),
                                });
                            }
                        }
                    }
                }
            }
            Rule::InsertBroadcast => {
                return Some(Broadcast {
                    input: Box::new(bind),
                })
            }
            Rule::InsertExchange => {
                return Some(Exchange {
                    input: Box::new(bind),
                });
            }
            Rule::LogicalGetToTableFreeScan => {
                if let LogicalSingleGet = bind {
                    return Some(TableFreeScan);
                }
            }
            Rule::LogicalGetToSeqScan => {
                if let LogicalGet {
                    projects,
                    predicates,
                    table,
                } = bind
                {
                    return Some(SeqScan {
                        projects,
                        predicates,
                        table,
                    });
                }
            }
            Rule::LogicalGetToIndexScan => {
                if let LogicalGet {
                    projects,
                    predicates,
                    table,
                } = bind
                {
                    // TODO return multiple indexes!
                    let catalog: &Box<dyn Catalog> = parent.context.get(CATALOG_KEY);
                    for index in catalog.indexes(table.id, parent.txn, &parent.context) {
                        if let Some((lookup, predicates)) = index.matches(&predicates) {
                            return Some(IndexScan {
                                include_existing: true,
                                projects,
                                predicates,
                                lookup,
                                index: index.clone(),
                                table,
                                input: Box::new(LogicalSingleGet),
                            });
                        }
                    }
                }
            }
            Rule::LogicalFilterToFilter => {
                if let LogicalFilter { predicates, input } = bind {
                    return Some(Filter { predicates, input });
                }
            }
            Rule::LogicalOutToOut => {
                if let LogicalOut { projects, input } = bind {
                    return Some(Out { projects, input });
                }
            }
            Rule::LogicalMapToMap => {
                if let LogicalMap {
                    include_existing,
                    projects,
                    input,
                } = bind
                {
                    return Some(Map {
                        include_existing,
                        projects,
                        input,
                    });
                }
            }
            Rule::LogicalJoinToNestedLoop => {
                if let LogicalJoin {
                    join, left, right, ..
                } = bind
                {
                    return Some(NestedLoop { join, left, right });
                }
            }
            Rule::LogicalJoinToBroadcastHashJoin => {
                return to_hash_join(&parent.ss, bind, true);
            }
            Rule::LogicalJoinToExchangeHashJoin => {
                return to_hash_join(&parent.ss, bind, false);
            }
            Rule::LogicalJoinToIndexScan => {
                if let LogicalJoin { join, left, right } = bind {
                    if let LogicalGet {
                        predicates: table_predicates,
                        projects,
                        table,
                    } = *left
                    {
                        // TODO return multiple indexes!
                        let catalog: &Box<dyn Catalog> = parent.context.get(CATALOG_KEY);
                        for index in catalog.indexes(table.id, parent.txn, &parent.context) {
                            if let Some((lookup, mut predicates)) = index.matches(join.predicates())
                            {
                                predicates.extend(table_predicates);
                                return Some(IndexScan {
                                    include_existing: true,
                                    projects,
                                    predicates,
                                    lookup,
                                    index: index.clone(),
                                    table,
                                    input: right,
                                });
                            }
                        }
                    }
                }
            }
            Rule::LogicalAggregateToAggregate => {
                if let LogicalAggregate {
                    group_by,
                    aggregate,
                    input,
                } = bind
                {
                    return Some(Aggregate {
                        group_by,
                        aggregate,
                        input,
                    });
                }
            }
            Rule::LogicalLimitToLimit => {
                if let LogicalLimit {
                    limit,
                    offset,
                    input,
                } = bind
                {
                    return Some(Limit {
                        limit,
                        offset,
                        input,
                    });
                }
            }
            Rule::LogicalSortToSort => {
                if let LogicalSort { order_by, input } = bind {
                    return Some(Sort { order_by, input });
                }
            }
            Rule::LogicallUnionToUnion => {
                if let LogicalUnion { left, right, .. } = bind {
                    return Some(Union { left, right });
                }
            }
            Rule::LogicalCreateTempTableToCreateTempTable => {
                if let LogicalCreateTempTable {
                    name,
                    columns,
                    input,
                } = bind
                {
                    return Some(CreateTempTable {
                        name,
                        columns,
                        input,
                    });
                }
            }
            Rule::LogicalGetWithToGetTempTable => {
                if let LogicalGetWith { name, columns } = bind {
                    return Some(GetTempTable { name, columns });
                }
            }
            Rule::LogicalInsertToInsert => {
                if let LogicalInsert {
                    table,
                    input,
                    columns,
                } = bind
                {
                    let catalog: &Box<dyn Catalog> = parent.context.get(CATALOG_KEY);
                    let indexes = catalog.indexes(table.id, parent.txn, &parent.context);
                    return Some(Insert {
                        table,
                        indexes,
                        input,
                        columns,
                    });
                }
            }
            Rule::LogicalValuesToValues => {
                if let LogicalValues {
                    columns,
                    values,
                    input,
                } = bind
                {
                    return Some(Values {
                        columns,
                        values,
                        input,
                    });
                }
            }
            Rule::LogicalDeleteToDelete => {
                if let LogicalDelete { table, tid, input } = bind {
                    return Some(Delete { table, tid, input });
                }
            }
            Rule::LogicalAssignToAssign => {
                if let LogicalAssign {
                    variable,
                    value,
                    input,
                } = bind
                {
                    return Some(Assign {
                        variable,
                        value,
                        input,
                    });
                }
            }
            Rule::LogicalCallToCall => {
                if let LogicalCall { procedure, input } = bind {
                    return Some(Call { procedure, input });
                }
            }
            Rule::LogicalExplainToExplain => {
                if let LogicalExplain { input } = bind {
                    return Some(Explain { input });
                }
            }
            Rule::LogicalScriptToScript => {
                if let LogicalScript { statements } = bind {
                    return Some(Script { statements });
                }
            }
        }
        None
    }

    pub fn all() -> Vec<Rule> {
        vec![
            Rule::InnerJoinCommutivity,
            Rule::InnerJoinAssociativity,
            Rule::InsertBroadcast,
            Rule::InsertExchange,
            Rule::LogicalGetToTableFreeScan,
            Rule::LogicalGetToSeqScan,
            Rule::LogicalGetToIndexScan,
            Rule::LogicalFilterToFilter,
            Rule::LogicalOutToOut,
            Rule::LogicalMapToMap,
            Rule::LogicalJoinToNestedLoop,
            Rule::LogicalJoinToBroadcastHashJoin,
            Rule::LogicalJoinToExchangeHashJoin,
            Rule::LogicalJoinToIndexScan,
            Rule::LogicalAggregateToAggregate,
            Rule::LogicalLimitToLimit,
            Rule::LogicalSortToSort,
            Rule::LogicallUnionToUnion,
            Rule::LogicalCreateTempTableToCreateTempTable,
            Rule::LogicalGetWithToGetTempTable,
            Rule::LogicalInsertToInsert,
            Rule::LogicalValuesToValues,
            Rule::LogicalDeleteToDelete,
            Rule::LogicalAssignToAssign,
            Rule::LogicalCallToCall,
            Rule::LogicalExplainToExplain,
            Rule::LogicalScriptToScript,
        ]
    }
}

fn to_hash_join(ss: &SearchSpace, bind: Expr, broadcast: bool) -> Option<Expr> {
    if let LogicalJoin { join, left, right } = bind {
        if let (Leaf { gid: left }, Leaf { gid: right }) = (left.as_ref(), right.as_ref()) {
            if let Some((partition_left, partition_right)) = hash_join(
                ss,
                join.predicates().clone(),
                GroupID(*left),
                GroupID(*right),
            ) {
                return Some(HashJoin {
                    broadcast,
                    join,
                    partition_left,
                    partition_right,
                    left: Box::new(Leaf { gid: *left }),
                    right: Box::new(Leaf { gid: *right }),
                });
            }
        }
    }
    None
}

fn hash_join(
    ss: &SearchSpace,
    mut join_predicates: Vec<Scalar>,
    left: GroupID,
    right: GroupID,
) -> Option<(Vec<Scalar>, Vec<Scalar>)> {
    let mut partition_left = vec![];
    let mut partition_right = vec![];
    for predicate in join_predicates.drain(0..) {
        if let Scalar::Call(function) = predicate {
            if let F::Equal(left_side, right_side) | F::Is(left_side, right_side) = *function {
                if contains_all(&ss[left], left_side.references())
                    && contains_all(&ss[right], right_side.references())
                {
                    partition_left.push(left_side);
                    partition_right.push(right_side);
                } else if contains_all(&ss[right], left_side.references())
                    && contains_all(&ss[left], right_side.references())
                {
                    partition_left.push(right_side);
                    partition_right.push(left_side);
                }
            }
        }
    }
    if partition_left.is_empty() {
        return None;
    }
    Some((partition_left, partition_right))
}

fn contains_all(group: &Group, columns: HashSet<Column>) -> bool {
    columns.iter().all(|c| group.props.columns.contains_key(c))
}
