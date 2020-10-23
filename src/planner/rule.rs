use crate::search_space::*;
use encoding::*;
use node::*;
use std::collections::HashSet;

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
    LogicalMapToMap,
    LogicalJoinToNestedLoop,
    LogicalJoinToHashJoin,
    LogicalAggregateToAggregate,
    LogicalProjectToProject,
    LogicalLimitToLimit,
    LogicalSortToSort,
    LogicallUnionToUnion,
    LogicalIntersectToIntersect,
    LogicalExceptToExcept,
    LogicalWithToCreateTempTable,
    LogicalGetWithToGetTempTable,
    LogicalInsertToInsert,
    LogicalValuesToValues,
    LogicalUpdateToUpdate,
    LogicalDeleteToDelete,
    LogicalCreateDatabaseToCreateDatabase,
    LogicalCreateTableToCreateTable,
    LogicalCreateIndexToCreateIndex,
    LogicalAlterTableToAlterTable,
    LogicalDropToDrop,
    LogicalRenameToRename,
}

impl Rule {
    pub fn output_is_physical(&self) -> bool {
        match self {
            Rule::LogicalGetToTableFreeScan
            | Rule::LogicalGetToSeqScan
            | Rule::LogicalGetToIndexScan
            | Rule::LogicalFilterToFilter
            | Rule::LogicalMapToMap
            | Rule::LogicalJoinToNestedLoop
            | Rule::LogicalJoinToHashJoin
            | Rule::LogicalAggregateToAggregate
            | Rule::LogicalProjectToProject
            | Rule::LogicalLimitToLimit
            | Rule::LogicalSortToSort
            | Rule::LogicallUnionToUnion
            | Rule::LogicalIntersectToIntersect
            | Rule::LogicalExceptToExcept
            | Rule::LogicalWithToCreateTempTable
            | Rule::LogicalGetWithToGetTempTable
            | Rule::LogicalInsertToInsert
            | Rule::LogicalValuesToValues
            | Rule::LogicalUpdateToUpdate
            | Rule::LogicalDeleteToDelete
            | Rule::LogicalCreateDatabaseToCreateDatabase
            | Rule::LogicalCreateTableToCreateTable
            | Rule::LogicalCreateIndexToCreateIndex
            | Rule::LogicalAlterTableToAlterTable
            | Rule::LogicalDropToDrop
            | Rule::LogicalRenameToRename => true,
            _ => false,
        }
    }

    pub fn promise(&self) -> isize {
        todo!("promise")
    }

    // Quickly check if rule matches expression *without* exploring the inputs to the expression.
    pub fn matches_fast(&self, mexpr: &MultiExpr) -> bool {
        match (self, &mexpr.op) {
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
            | (Rule::LogicalFilterToFilter, LogicalFilter(_, _))
            | (Rule::LogicalMapToMap, LogicalMap(_, _))
            | (Rule::LogicalJoinToNestedLoop, LogicalJoin { .. })
            | (Rule::LogicalJoinToHashJoin, LogicalJoin { .. })
            | (Rule::LogicalAggregateToAggregate, LogicalAggregate { .. })
            | (Rule::LogicalProjectToProject, LogicalProject { .. })
            | (Rule::LogicalLimitToLimit, LogicalLimit { .. })
            | (Rule::LogicalSortToSort, LogicalSort(_, _))
            | (Rule::LogicallUnionToUnion, LogicalUnion(_, _))
            | (Rule::LogicalIntersectToIntersect, LogicalIntersect(_, _))
            | (Rule::LogicalExceptToExcept, LogicalExcept(_, _))
            | (Rule::LogicalWithToCreateTempTable, LogicalWith(_, _, _, _))
            | (Rule::LogicalGetWithToGetTempTable, LogicalGetWith(_, _))
            | (Rule::LogicalInsertToInsert, LogicalInsert(_, _, _))
            | (Rule::LogicalValuesToValues, LogicalValues(_, _, _))
            | (Rule::LogicalUpdateToUpdate, LogicalUpdate(_, _))
            | (Rule::LogicalDeleteToDelete, LogicalDelete(_, _))
            | (Rule::LogicalCreateDatabaseToCreateDatabase, LogicalCreateDatabase(_))
            | (Rule::LogicalCreateTableToCreateTable, LogicalCreateTable { .. })
            | (Rule::LogicalCreateIndexToCreateIndex, LogicalCreateIndex { .. })
            | (Rule::LogicalAlterTableToAlterTable, LogicalAlterTable { .. })
            | (Rule::LogicalDropToDrop, LogicalDrop { .. })
            | (Rule::LogicalRenameToRename, LogicalRename { .. }) => true,
            _ => false,
        }
    }

    pub fn non_leaf(&self, child: usize) -> bool {
        match (self, child) {
            (Rule::InnerJoinAssociativity, 0) => true,
            _ => false,
        }
    }

    pub fn bind(&self, ss: &SearchSpace, mid: MultiExprID) -> Vec<Operator<Bind>> {
        let mut binds = vec![];
        match self {
            Rule::InnerJoinAssociativity => {
                if let LogicalJoin {
                    join: Join::Inner(parent_predicates),
                    left,
                    right,
                    ..
                } = &ss[mid].op
                {
                    for left in &ss[*left].logical {
                        if let LogicalJoin {
                            join: Join::Inner(left_predicates),
                            left: left_left,
                            right: left_middle,
                            ..
                        } = &ss[*left].op
                        {
                            binds.push(LogicalJoin {
                                parameters: vec![],
                                join: Join::Inner(parent_predicates.clone()),
                                left: Bind::Operator(Box::new(LogicalJoin {
                                    parameters: vec![],
                                    join: Join::Inner(left_predicates.clone()),
                                    left: Bind::Group(*left_left),
                                    right: Bind::Group(*left_middle),
                                })),
                                right: Bind::Group(*right),
                            })
                        }
                    }
                }
            }
            Rule::LogicalGetToIndexScan => {
                if let LogicalGet {
                    projects,
                    predicates,
                    table,
                } = &ss[mid].op
                {
                    if find_index_scan(predicates, table).is_some() {
                        binds.push(LogicalGet {
                            projects: projects.clone(),
                            predicates: predicates.clone(),
                            table: table.clone(),
                        })
                    }
                }
            }
            _ => binds.push(ss[mid].op.clone().map(|group| Bind::Group(group))),
        }
        binds
    }

    pub fn apply(&self, ss: &SearchSpace, bind: Operator<Bind>) -> Option<Operator<Bind>> {
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
                        parameters: vec![],
                        join: Join::Inner(join_predicates.clone()),
                        left: right,
                        right: left,
                    });
                }
            }
            // Rearrange left-deep join into right-deep join.
            //
            //             +---+ parent +---+
            //             |                |
            //             +                +
            //      +--+leftJoin+---+     right
            //      |               |
            //      +               +
            //   leftLeft      leftMiddle
            Rule::InnerJoinAssociativity => {
                if let LogicalJoin {
                    join: Join::Inner(parent_predicates),
                    left: Bind::Operator(left),
                    right,
                    ..
                } = bind
                {
                    if let LogicalJoin {
                        join: Join::Inner(left_predicates),
                        left: left_left,
                        right: left_middle,
                        ..
                    } = *left
                    {
                        let mut new_parent_predicates = vec![];
                        let mut new_right_predicates = vec![];
                        todo!("redistribute predicates");
                        return Some(LogicalJoin {
                            parameters: vec![],
                            join: Join::Inner(new_parent_predicates),
                            left: left_left,
                            right: Bind::Operator(Box::new(LogicalJoin {
                                parameters: vec![],
                                join: Join::Inner(new_right_predicates),
                                left: left_middle,
                                right,
                            })),
                        });
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
                    let mut predicates = predicates;
                    let i = find_index_scan(&predicates, &table).unwrap();
                    let (c, x) = unpack_index_lookup(predicates.remove(i), &table).unwrap();
                    return Some(IndexScan {
                        projects,
                        predicates,
                        table,
                        equals: vec![(c, x)],
                    });
                }
            }
            Rule::LogicalFilterToFilter => {
                if let LogicalFilter(predicates, input) = bind {
                    return Some(Filter(predicates, input));
                }
            }
            Rule::LogicalMapToMap => {
                if let LogicalMap(projects, input) = bind {
                    return Some(Map(projects, input));
                }
            }
            Rule::LogicalJoinToNestedLoop => {
                if let LogicalJoin {
                    join, left, right, ..
                } = bind
                {
                    return Some(NestedLoop(join, left, right));
                }
            }
            Rule::LogicalJoinToHashJoin => {
                if let LogicalJoin {
                    join,
                    left: Bind::Group(left),
                    right: Bind::Group(right),
                    ..
                } = bind
                {
                    let (hash_predicates, join) = match join {
                        Join::Inner(join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(ss, join_predicates, left, right);
                            let join = Join::Inner(remaining_predicates);
                            (hash_predicates, join)
                        }
                        Join::Right(join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(ss, join_predicates, left, right);
                            let join = Join::Right(remaining_predicates);
                            (hash_predicates, join)
                        }
                        Join::Outer(join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(ss, join_predicates, left, right);
                            let join = Join::Outer(remaining_predicates);
                            (hash_predicates, join)
                        }
                        Join::Semi(join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(ss, join_predicates, left, right);
                            let join = Join::Semi(remaining_predicates);
                            (hash_predicates, join)
                        }
                        Join::Anti(join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(ss, join_predicates, left, right);
                            let join = Join::Anti(remaining_predicates);
                            (hash_predicates, join)
                        }
                        Join::Single(join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(ss, join_predicates, left, right);
                            let join = Join::Single(remaining_predicates);
                            (hash_predicates, join)
                        }
                        Join::Mark(column, join_predicates) => {
                            let (hash_predicates, remaining_predicates) =
                                hash_join(ss, join_predicates, left, right);
                            let join = Join::Mark(column, remaining_predicates);
                            (hash_predicates, join)
                        }
                    };
                    if !hash_predicates.is_empty() {
                        return Some(HashJoin(
                            join,
                            hash_predicates,
                            Bind::Group(left),
                            Bind::Group(right),
                        ));
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
            Rule::LogicalProjectToProject => {
                if let LogicalProject(projects, input) = bind {
                    return Some(Project(projects, input));
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
                if let LogicalSort(order_by, input) = bind {
                    return Some(Sort(order_by, input));
                }
            }
            Rule::LogicallUnionToUnion => {
                if let LogicalUnion(outputs, inputs) = bind {
                    return Some(Union(outputs, inputs));
                }
            }
            Rule::LogicalIntersectToIntersect => {
                if let LogicalIntersect(outputs, inputs) = bind {
                    return Some(Intersect(outputs, inputs));
                }
            }
            Rule::LogicalExceptToExcept => {
                if let LogicalExcept(outputs, inputs) = bind {
                    return Some(Except(outputs, inputs));
                }
            }
            Rule::LogicalWithToCreateTempTable => {
                if let LogicalWith(name, columns, left, right) = bind {
                    return Some(CreateTempTable(name, columns, left, right));
                }
            }
            Rule::LogicalGetWithToGetTempTable => {
                if let LogicalGetWith(name, columns) = bind {
                    return Some(GetTempTable(name, columns));
                }
            }
            Rule::LogicalInsertToInsert => {
                if let LogicalInsert(table, columns, input) = bind {
                    return Some(Insert(table, columns, input));
                }
            }
            Rule::LogicalValuesToValues => {
                if let LogicalValues(columns, rows, input) = bind {
                    return Some(Values(columns, rows, input));
                }
            }
            Rule::LogicalUpdateToUpdate => {
                if let LogicalUpdate(updates, input) = bind {
                    return Some(Update(updates, input));
                }
            }
            Rule::LogicalDeleteToDelete => {
                if let LogicalDelete(table, input) = bind {
                    return Some(Delete(table, input));
                }
            }
            Rule::LogicalCreateDatabaseToCreateDatabase => {
                if let LogicalCreateDatabase { .. } = bind {
                    todo!()
                }
            }
            Rule::LogicalCreateTableToCreateTable => {
                if let LogicalCreateTable {
                    name,
                    columns,
                    partition_by,
                    cluster_by,
                    primary_key,
                    input,
                } = bind
                {
                    return Some(CreateTable {
                        name,
                        columns,
                        partition_by,
                        cluster_by,
                        primary_key,
                        input,
                    });
                }
            }
            Rule::LogicalCreateIndexToCreateIndex => {
                if let LogicalCreateIndex { .. } = bind {
                    todo!()
                }
            }
            Rule::LogicalAlterTableToAlterTable => {
                if let LogicalAlterTable { .. } = bind {
                    todo!()
                }
            }
            Rule::LogicalDropToDrop => {
                if let LogicalDrop { .. } = bind {
                    todo!()
                }
            }
            Rule::LogicalRenameToRename => {
                if let LogicalRename { .. } = bind {
                    todo!()
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
            Rule::LogicalMapToMap,
            Rule::LogicalJoinToNestedLoop,
            Rule::LogicalJoinToHashJoin,
            Rule::LogicalAggregateToAggregate,
            Rule::LogicalProjectToProject,
            Rule::LogicalLimitToLimit,
            Rule::LogicalSortToSort,
            Rule::LogicallUnionToUnion,
            Rule::LogicalIntersectToIntersect,
            Rule::LogicalExceptToExcept,
            Rule::LogicalWithToCreateTempTable,
            Rule::LogicalGetWithToGetTempTable,
            Rule::LogicalInsertToInsert,
            Rule::LogicalValuesToValues,
            Rule::LogicalUpdateToUpdate,
            Rule::LogicalDeleteToDelete,
            Rule::LogicalCreateDatabaseToCreateDatabase,
            Rule::LogicalCreateTableToCreateTable,
            Rule::LogicalCreateIndexToCreateIndex,
            Rule::LogicalAlterTableToAlterTable,
            Rule::LogicalDropToDrop,
            Rule::LogicalRenameToRename,
        ]
    }
}

pub enum Bind {
    Group(GroupID),
    Operator(Box<Operator<Bind>>),
}

fn find_index_scan(predicates: &Vec<Scalar>, table: &Table) -> Option<usize> {
    for i in 0..predicates.len() {
        if unpack_index_lookup(predicates[i].clone(), table).is_some() {
            return Some(i);
        }
    }
    None
}

fn unpack_index_lookup(predicate: Scalar, table: &Table) -> Option<(Column, Scalar)> {
    if let Scalar::Call(Function::Equal, mut arguments, _) = predicate {
        match (arguments.pop().unwrap(), arguments.pop().unwrap()) {
            (Scalar::Column(column), equals) | (equals, Scalar::Column(column))
                if column.name.ends_with("_id") && column.table.as_ref() == Some(&table.name) =>
            {
                return Some((column, equals))
            }
            _ => {}
        }
    }
    None
}

fn hash_join(
    ss: &SearchSpace,
    mut join_predicates: Vec<Scalar>,
    left: GroupID,
    right: GroupID,
) -> (Vec<(Scalar, Scalar)>, Vec<Scalar>) {
    let mut hash_predicates = vec![];
    let mut remaining_predicates = vec![];
    for predicate in join_predicates.drain(0..) {
        if let Scalar::Call(Function::Equal, mut arguments, _) = predicate {
            let right_side = arguments.pop().unwrap();
            let left_side = arguments.pop().unwrap();
            if contains_all(&ss[left], left_side.free())
                && contains_all(&ss[right], right_side.free())
            {
                hash_predicates.push((left_side, right_side))
            } else if contains_all(&ss[right], left_side.free())
                && contains_all(&ss[left], right_side.free())
            {
                hash_predicates.push((right_side, left_side))
            } else {
                remaining_predicates.push(Scalar::Call(
                    Function::Equal,
                    vec![left_side, right_side],
                    Type::Bool,
                ));
            }
        } else {
            remaining_predicates.push(predicate);
        }
    }
    (hash_predicates, remaining_predicates)
}

fn contains_all(group: &Group, columns: HashSet<Column>) -> bool {
    columns
        .iter()
        .all(|c| group.props.column_unique_cardinality.contains_key(c))
}
