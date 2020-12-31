use crate::search_space::*;
use ast::*;
use catalog::Index;
use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
};

#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub enum Rule {
    // Rewrite rules
    InnerJoinCommutivity,
    InnerJoinAssociativity,
    // Implementation rules
    LogicalGetToTableFreeScan,
    LogicalGetToSeqScan,
    LogicalGetToIndexScan,
    LogicalFilterToFilter,
    LogicalOutToOut,
    LogicalMapToMap,
    LogicalJoinToNestedLoop,
    LogicalJoinToHashJoin,
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
    LogicalDropToDrop,
    LogicalAssignToAssign,
    LogicalCallToCall,
    LogicalExplainToExplain,
    LogicalScriptToScript,
}

impl Rule {
    pub fn output_is_physical(&self) -> bool {
        match self {
            Rule::LogicalGetToTableFreeScan
            | Rule::LogicalGetToSeqScan
            | Rule::LogicalGetToIndexScan
            | Rule::LogicalFilterToFilter
            | Rule::LogicalOutToOut
            | Rule::LogicalMapToMap
            | Rule::LogicalJoinToNestedLoop
            | Rule::LogicalJoinToHashJoin
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
            | Rule::LogicalDropToDrop
            | Rule::LogicalAssignToAssign
            | Rule::LogicalScriptToScript => true,
            _ => false,
        }
    }

    pub fn promise(&self) -> isize {
        todo!("promise")
    }

    // Quickly check if rule matches expression *without* exploring the inputs to the expression.
    pub fn matches_fast(&self, mexpr: &MultiExpr) -> bool {
        match (self, &mexpr.expr) {
            (
                Rule::InnerJoinCommutivity,
                LogicalJoin {
                    join: Join::Inner(_),
                    ..
                },
            )
            | (
                Rule::InnerJoinAssociativity,
                LogicalJoin {
                    join: Join::Inner(_),
                    ..
                },
            )
            | (Rule::LogicalGetToTableFreeScan, LogicalSingleGet)
            | (Rule::LogicalGetToSeqScan, LogicalGet { .. })
            | (Rule::LogicalGetToIndexScan, LogicalGet { .. })
            | (Rule::LogicalFilterToFilter, LogicalFilter { .. })
            | (Rule::LogicalOutToOut, LogicalOut { .. })
            | (Rule::LogicalMapToMap, LogicalMap { .. })
            | (Rule::LogicalJoinToNestedLoop, LogicalJoin { .. })
            | (Rule::LogicalJoinToHashJoin, LogicalJoin { .. })
            | (Rule::LogicalJoinToIndexScan, LogicalJoin { .. })
            | (Rule::LogicalAggregateToAggregate, LogicalAggregate { .. })
            | (Rule::LogicalLimitToLimit, LogicalLimit { .. })
            | (Rule::LogicalSortToSort, LogicalSort { .. })
            | (Rule::LogicallUnionToUnion, LogicalUnion { .. })
            | (Rule::LogicalCreateTempTableToCreateTempTable, LogicalCreateTempTable { .. })
            | (Rule::LogicalGetWithToGetTempTable, LogicalGetWith { .. })
            | (Rule::LogicalInsertToInsert, LogicalInsert { .. })
            | (Rule::LogicalValuesToValues, LogicalValues { .. })
            | (Rule::LogicalDeleteToDelete, LogicalDelete { .. })
            | (Rule::LogicalDropToDrop, LogicalDrop { .. })
            | (Rule::LogicalAssignToAssign, LogicalAssign { .. })
            | (Rule::LogicalCallToCall, LogicalCall { .. })
            | (Rule::LogicalExplainToExplain, LogicalExplain { .. })
            | (Rule::LogicalScriptToScript, LogicalScript { .. }) => true,
            _ => false,
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
            _ => binds.push(ss[mid].expr.clone().map(|group| group)),
        }
        binds
    }

    pub fn apply(
        &self,
        ss: &SearchSpace,
        indexes: &HashMap<i64, Vec<Index>>,
        bind: Expr,
    ) -> Option<Expr> {
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
                                let middle_scope =
                                    &ss[GroupID(*left_middle)].props.column_unique_cardinality;
                                let right_scope =
                                    &ss[GroupID(*right)].props.column_unique_cardinality;
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
                    let indexes = indexes.get(&table.id)?;
                    // TODO return multiple indexes!
                    let (index, lookup, predicates) =
                        find_index(indexes, &projects, &predicates).pop()?;
                    return Some(IndexScan {
                        include_existing: false,
                        projects,
                        predicates,
                        lookup,
                        index,
                        table,
                        input: Box::new(LogicalSingleGet),
                    });
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
            Rule::LogicalJoinToHashJoin => {
                if let LogicalJoin { join, left, right } = bind {
                    if let (Leaf { gid: left }, Leaf { gid: right }) =
                        (left.as_ref(), right.as_ref())
                    {
                        if let Some((partition_left, partition_right)) = hash_join(
                            ss,
                            join.predicates().clone(),
                            GroupID(*left),
                            GroupID(*right),
                        ) {
                            return Some(HashJoin {
                                join,
                                partition_left,
                                partition_right,
                                left: Box::new(Leaf { gid: *left }),
                                right: Box::new(Leaf { gid: *right }),
                            });
                        }
                    }
                }
            }
            Rule::LogicalJoinToIndexScan => {
                if let LogicalJoin { join, left, right } = bind {
                    if let LogicalGet {
                        predicates: table_predicates,
                        projects,
                        table,
                    } = *left
                    {
                        let mut predicates = join.predicates().clone();
                        predicates.extend(table_predicates);
                        let indexes = indexes.get(&table.id)?;
                        // TODO return multiple indexes!
                        let (index, lookup, predicates) =
                            find_index(indexes, &projects, &predicates).pop()?;
                        return Some(IndexScan {
                            include_existing: true,
                            projects,
                            predicates,
                            lookup,
                            index,
                            table,
                            input: right,
                        });
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
                    let indexes = indexes.get(&table.id).cloned().unwrap_or(vec![]);
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
            Rule::LogicalDropToDrop => {
                if let LogicalDrop { .. } = bind {
                    todo!()
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
            Rule::LogicalGetToTableFreeScan,
            Rule::LogicalGetToSeqScan,
            Rule::LogicalGetToIndexScan,
            Rule::LogicalFilterToFilter,
            Rule::LogicalOutToOut,
            Rule::LogicalMapToMap,
            Rule::LogicalJoinToNestedLoop,
            Rule::LogicalJoinToHashJoin,
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
            Rule::LogicalDropToDrop,
            Rule::LogicalAssignToAssign,
            Rule::LogicalCallToCall,
            Rule::LogicalExplainToExplain,
            Rule::LogicalScriptToScript,
        ]
    }
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
            if let Function::Equal(left_side, right_side) | Function::Is(left_side, right_side) =
                *function
            {
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
    columns
        .iter()
        .all(|c| group.props.column_unique_cardinality.contains_key(c))
}

fn find_index(
    indexes: &Vec<Index>,
    table_columns: &Vec<Column>,
    predicates: &Vec<Scalar>,
) -> Vec<(Index, Vec<Scalar>, Vec<Scalar>)> {
    indexes
        .iter()
        .flat_map(|index| check_index(index, table_columns, predicates))
        .collect()
}

fn check_index(
    index: &Index,
    table_columns: &Vec<Column>,
    predicates: &Vec<Scalar>,
) -> Option<(Index, Vec<Scalar>, Vec<Scalar>)> {
    let mut index_predicates = vec![];
    let mut remaining_predicates = vec![];
    for column_name in &index.columns {
        for predicate in predicates {
            match predicate {
                Scalar::Call(function) => match function.as_ref() {
                    Function::Equal(Scalar::Column(column), lookup)
                    | Function::Equal(lookup, Scalar::Column(column))
                        if column_name == &column.name && table_columns.contains(column) =>
                    {
                        index_predicates.push(lookup.clone());
                    }
                    _ => remaining_predicates.push(predicate.clone()),
                },
                _ => remaining_predicates.push(predicate.clone()),
            }
        }
    }
    if index.columns.len() == index_predicates.len() {
        Some((index.clone(), index_predicates, remaining_predicates))
    } else {
        None
    }
}
