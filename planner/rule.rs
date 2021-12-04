use std::collections::HashSet;

use ast::*;
use kernel::DataType;

use crate::search_space::*;

#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub(crate) enum Rule {
    // Rewrite rules
    InnerJoinCommutivity,
    InnerJoinAssociativity,
    // Enforcers.
    InsertBroadcast,
    InsertExchange,
    InsertGather,
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
    LogicalCallToCall,
    LogicalExplainToExplain,
    LogicalScriptToScript,
}

impl Rule {
    pub fn output_is_physical(&self) -> bool {
        match self {
            Rule::InsertBroadcast
            | Rule::InsertExchange
            | Rule::InsertGather
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
            // Full outer joins can't be implemented by broadcasting the build side of the join.
            // TODO return a good error message when someone tries to do a non-equi full outer join.
            (
                Rule::LogicalJoinToNestedLoop,
                LogicalJoin {
                    join: Join::Outer(_),
                    ..
                },
            )
            | (
                Rule::LogicalJoinToBroadcastHashJoin,
                LogicalJoin {
                    join: Join::Outer(_),
                    ..
                },
            ) => false,
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
            | (Rule::LogicalCallToCall, LogicalCall { .. })
            | (Rule::LogicalExplainToExplain, LogicalExplain { .. })
            | (Rule::LogicalScriptToScript, LogicalScript { .. })
            | (Rule::InsertBroadcast, _)
            | (Rule::InsertExchange, _)
            | (Rule::InsertGather, _) => true,
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
            | Rule::LogicalCallToCall
            | Rule::LogicalExplainToExplain
            | Rule::LogicalScriptToScript => required == PhysicalProp::None,
            Rule::InsertBroadcast => required == PhysicalProp::BroadcastDist,
            Rule::InsertExchange => required == PhysicalProp::ExchangeDist,
            Rule::InsertGather => required == PhysicalProp::GatherDist,
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
            Rule::InsertBroadcast | Rule::InsertExchange | Rule::InsertGather => binds.push(Leaf {
                gid: ss[mid].parent.0,
            }),
            _ => binds.push(ss[mid].expr.clone()),
        }
        binds
    }

    pub fn apply(&self, bind: Expr, ss: &SearchSpace) -> Vec<Expr> {
        match self {
            Rule::InnerJoinCommutivity => {
                if let LogicalJoin {
                    join: Join::Inner(join_predicates),
                    left,
                    right,
                    ..
                } = bind
                {
                    return single(LogicalJoin {
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
                                let middle_scope = &ss[GroupID(*left_middle)].props.columns;
                                let right_scope = &ss[GroupID(*right)].props.columns;
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
                                return single(LogicalJoin {
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
                return single(Broadcast {
                    stage: None,
                    input: Box::new(bind),
                })
            }
            Rule::InsertExchange => {
                return single(Exchange {
                    stage: None,
                    hash_column: None,
                    input: Box::new(bind),
                });
            }
            Rule::InsertGather => {
                return single(Gather {
                    worker: None,
                    stage: None,
                    input: Box::new(bind),
                });
            }
            Rule::LogicalGetToTableFreeScan => {
                if let LogicalSingleGet = bind {
                    return single(TableFreeScan { worker: None });
                }
            }
            Rule::LogicalGetToSeqScan => {
                if let LogicalGet {
                    projects,
                    predicates,
                    table,
                } = bind
                {
                    return single(SeqScan {
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
                    let mut results = vec![];
                    for index in crate::catalog::indexes(table.id, ss.txn) {
                        if let Some((lookup, predicates)) = index.matches(&predicates) {
                            results.push(IndexScan {
                                include_existing: true,
                                projects: projects.clone(),
                                predicates,
                                lookup,
                                index: index.clone(),
                                table: table.clone(),
                                input: Box::new(LogicalSingleGet),
                            });
                        }
                    }
                    return results;
                }
            }
            Rule::LogicalFilterToFilter => {
                if let LogicalFilter { predicates, input } = bind {
                    return single(Filter { predicates, input });
                }
            }
            Rule::LogicalOutToOut => {
                if let LogicalOut { projects, input } = bind {
                    return single(Out { projects, input });
                }
            }
            Rule::LogicalMapToMap => {
                if let LogicalMap {
                    include_existing,
                    projects,
                    input,
                } = bind
                {
                    return single(Map {
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
                    return single(NestedLoop { join, left, right });
                }
            }
            Rule::LogicalJoinToBroadcastHashJoin => {
                return to_hash_join(&ss, bind, true);
            }
            Rule::LogicalJoinToExchangeHashJoin => {
                return to_hash_join(&ss, bind, false);
            }
            Rule::LogicalJoinToIndexScan => {
                if let LogicalJoin { join, left, right } = bind {
                    if let LogicalGet {
                        predicates: table_predicates,
                        projects,
                        table,
                    } = *left
                    {
                        let mut results = vec![];
                        for index in crate::catalog::indexes(table.id, ss.txn) {
                            if let Some((lookup, mut predicates)) = index.matches(join.predicates())
                            {
                                predicates.extend(table_predicates.clone());
                                results.push(IndexScan {
                                    include_existing: true,
                                    projects: projects.clone(),
                                    predicates,
                                    lookup,
                                    index: index.clone(),
                                    table: table.clone(),
                                    input: right.clone(),
                                });
                            }
                        }
                        return results;
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
                    return single(to_aggregate(group_by, aggregate, input));
                }
            }
            Rule::LogicalLimitToLimit => {
                if let LogicalLimit {
                    limit,
                    offset,
                    input,
                } = bind
                {
                    return single(Limit {
                        limit,
                        offset,
                        input,
                    });
                }
            }
            Rule::LogicalSortToSort => {
                if let LogicalSort { order_by, input } = bind {
                    return single(Sort { order_by, input });
                }
            }
            Rule::LogicallUnionToUnion => {
                if let LogicalUnion { left, right, .. } = bind {
                    return single(Union { left, right });
                }
            }
            Rule::LogicalCreateTempTableToCreateTempTable => {
                if let LogicalCreateTempTable {
                    name,
                    columns,
                    input,
                } = bind
                {
                    return single(CreateTempTable {
                        name,
                        columns,
                        input,
                    });
                }
            }
            Rule::LogicalGetWithToGetTempTable => {
                if let LogicalGetWith { name, columns } = bind {
                    return single(GetTempTable { name, columns });
                }
            }
            Rule::LogicalInsertToInsert => {
                if let LogicalInsert {
                    table,
                    input,
                    columns,
                } = bind
                {
                    let indexes = crate::catalog::indexes(table.id, ss.txn);
                    return single(Insert {
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
                    return single(Values {
                        columns,
                        values,
                        input,
                    });
                }
            }
            Rule::LogicalDeleteToDelete => {
                if let LogicalDelete { table, tid, input } = bind {
                    return single(Delete { table, tid, input });
                }
            }
            Rule::LogicalCallToCall => {
                if let LogicalCall { procedure, input } = bind {
                    return single(Call { procedure, input });
                }
            }
            Rule::LogicalExplainToExplain => {
                if let LogicalExplain { input } = bind {
                    return single(Explain { input });
                }
            }
            Rule::LogicalScriptToScript => {
                if let LogicalScript { statements } = bind {
                    return single(Script { statements });
                }
            }
        }
        vec![]
    }

    pub fn all() -> Vec<Rule> {
        vec![
            Rule::InnerJoinCommutivity,
            Rule::InnerJoinAssociativity,
            Rule::InsertBroadcast,
            Rule::InsertExchange,
            Rule::InsertGather,
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
            Rule::LogicalCallToCall,
            Rule::LogicalExplainToExplain,
            Rule::LogicalScriptToScript,
        ]
    }
}

fn to_aggregate(group_by: Vec<Column>, aggregate: Vec<AggregateExpr>, input: Box<Expr>) -> Expr {
    if group_by.is_empty() {
        SimpleAggregate {
            worker: None,
            aggregate,
            input,
        }
    } else {
        let partition_by = group_by.iter().map(|c| Scalar::Column(c.clone())).collect();
        let (partition_by, input) = create_hash_column(partition_by, *input);
        GroupByAggregate {
            partition_by,
            group_by,
            aggregate,
            input: Box::new(input),
        }
    }
}

fn single(expr: Expr) -> Vec<Expr> {
    vec![expr]
}

fn to_hash_join(ss: &SearchSpace, bind: Expr, broadcast: bool) -> Vec<Expr> {
    if let LogicalJoin { join, left, right } = bind {
        if let (Leaf { gid: left }, Leaf { gid: right }) = (left.as_ref(), right.as_ref()) {
            if let Some((partition_left, partition_right)) = hash_join(
                ss,
                join.predicates().clone(),
                GroupID(*left),
                GroupID(*right),
            ) {
                let (partition_left, left) =
                    create_hash_column(partition_left, Leaf { gid: *left });
                let (partition_right, right) =
                    create_hash_column(partition_right, Leaf { gid: *right });
                return single(HashJoin {
                    broadcast,
                    join,
                    partition_left,
                    partition_right,
                    left: Box::new(left),
                    right: Box::new(right),
                });
            }
        }
    }
    vec![]
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

fn create_hash_column(partition_by: Vec<Scalar>, input: Expr) -> (Column, Expr) {
    let column = Column::fresh("$hash", DataType::I64);
    let scalar = Scalar::Call(Box::new(F::Hash(partition_by)));
    let expr = LogicalMap {
        projects: vec![(scalar, column.clone())],
        include_existing: true,
        input: Box::new(input),
    };
    (column, expr)
}
