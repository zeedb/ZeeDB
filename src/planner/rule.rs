use crate::search_space::*;
use encoding::*;
use node::*;

#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub enum Rule {
    // Rewrite rules
    LogicalInnerJoinCommutivity,
    LogicalInnerJoinAssociativity,
    // Implementation rules
    LogicalGetToTableFreeScan,
    LogicalGetToSeqScan,
    LogicalGetToIndexScan,
    LogicalFilterToFilter,
    LogicalProjectToProject,
    LogicalJoinToNestedLoop,
    LogicalJoinToHashJoin,
    LogicalAggregateToAggregate,
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
            | Rule::LogicalProjectToProject
            | Rule::LogicalJoinToNestedLoop
            | Rule::LogicalJoinToHashJoin
            | Rule::LogicalAggregateToAggregate
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
            (Rule::LogicalInnerJoinCommutivity, LogicalJoin(Join::Inner(_), _, _))
            | (Rule::LogicalInnerJoinAssociativity, LogicalJoin(Join::Inner(_), _, _))
            | (Rule::LogicalGetToTableFreeScan, LogicalSingleGet)
            | (Rule::LogicalGetToSeqScan, LogicalGet(_))
            | (Rule::LogicalGetToIndexScan, LogicalFilter(_, _))
            | (Rule::LogicalFilterToFilter, LogicalFilter(_, _))
            | (Rule::LogicalProjectToProject, LogicalProject(_, _))
            | (Rule::LogicalJoinToNestedLoop, LogicalJoin(_, _, _))
            | (Rule::LogicalJoinToHashJoin, LogicalJoin(_, _, _))
            | (Rule::LogicalAggregateToAggregate, LogicalAggregate { .. })
            | (Rule::LogicalLimitToLimit, LogicalLimit { .. })
            | (Rule::LogicalSortToSort, LogicalSort(_, _))
            | (Rule::LogicallUnionToUnion, LogicalUnion(_, _))
            | (Rule::LogicalIntersectToIntersect, LogicalIntersect(_, _))
            | (Rule::LogicalExceptToExcept, LogicalExcept(_, _))
            | (Rule::LogicalWithToCreateTempTable, LogicalWith(_, _, _))
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
            (Rule::LogicalInnerJoinAssociativity, 0) => true,
            // TODO once we add predicate pushdown into LogicalGet, we won't need this anymore
            (Rule::LogicalGetToIndexScan, 0) => true,
            _ => false,
        }
    }

    pub fn bind(&self, ss: &SearchSpace, mid: MultiExprID) -> Vec<Operator<Bind>> {
        let mut binds = vec![];
        match self {
            Rule::LogicalInnerJoinAssociativity => {
                if let LogicalJoin(Join::Inner(parent_predicates), left, right) = &ss[mid].op {
                    for left in &ss[*left].logical {
                        if let LogicalJoin(Join::Inner(left_predicates), left_left, left_middle) =
                            &ss[*left].op
                        {
                            binds.push(LogicalJoin(
                                Join::Inner(parent_predicates.clone()),
                                Bind::Operator(Box::new(LogicalJoin(
                                    Join::Inner(left_predicates.clone()),
                                    Bind::Group(*left_left),
                                    Bind::Group(*left_middle),
                                ))),
                                Bind::Group(*right),
                            ))
                        }
                    }
                }
            }
            Rule::LogicalGetToIndexScan => {
                if let LogicalFilter(predicates, input) = &ss[mid].op {
                    for input in &ss[*input].logical {
                        if let LogicalGet(table) = &ss[*input].op {
                            if can_index_scan(predicates, table) {
                                binds.push(LogicalFilter(
                                    predicates.clone(),
                                    Bind::Operator(Box::new(LogicalGet(table.clone()))),
                                ))
                            }
                        }
                    }
                }
            }
            _ => binds.push(ss[mid].op.clone().map(|group| Bind::Group(group))),
        }
        binds
    }

    pub fn apply(&self, ss: &SearchSpace, bind: Operator<Bind>) -> Option<Operator<Bind>> {
        match self {
            Rule::LogicalInnerJoinCommutivity => {
                if let LogicalJoin(Join::Inner(join_predicates), left, right) = bind {
                    return Some(LogicalJoin(
                        Join::Inner(join_predicates.clone()),
                        right,
                        left,
                    ));
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
            Rule::LogicalInnerJoinAssociativity => {
                if let LogicalJoin(Join::Inner(parent_predicates), Bind::Operator(left), right) =
                    bind
                {
                    if let LogicalJoin(Join::Inner(left_predicates), left_left, left_middle) = *left
                    {
                        let mut new_parent_predicates = vec![];
                        let mut new_right_predicates = vec![];
                        todo!("redistribute predicates");
                        return Some(LogicalJoin(
                            Join::Inner(new_parent_predicates),
                            left_left,
                            Bind::Operator(Box::new(LogicalJoin(
                                Join::Inner(new_right_predicates),
                                left_middle,
                                right,
                            ))),
                        ));
                    }
                }
            }
            Rule::LogicalGetToTableFreeScan => {
                if let LogicalSingleGet = bind {
                    return Some(TableFreeScan);
                }
            }
            Rule::LogicalGetToSeqScan => {
                if let LogicalGet(table) = bind {
                    return Some(SeqScan(table));
                }
            }
            Rule::LogicalGetToIndexScan => {
                if let LogicalFilter(predicates, Bind::Operator(input)) = bind {
                    if let LogicalGet(table) = *input {
                        return index_scan(predicates, table);
                    }
                }
            }
            Rule::LogicalFilterToFilter => {
                if let LogicalFilter(predicates, input) = bind {
                    return Some(Filter(predicates, input));
                }
            }
            Rule::LogicalProjectToProject => {
                if let LogicalProject(projects, input) = bind {
                    return Some(Project(projects, input));
                }
            }
            Rule::LogicalJoinToNestedLoop => {
                if let LogicalJoin(join, left, right) = bind {
                    return Some(NestedLoop(join, left, right));
                }
            }
            Rule::LogicalJoinToHashJoin => {
                if let LogicalJoin(join, Bind::Group(left), Bind::Group(right)) = bind {
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
                if let LogicalWith(name, left, right) = bind {
                    return Some(CreateTempTable(name, left, right));
                }
            }
            Rule::LogicalGetWithToGetTempTable => {
                if let LogicalGetWith(name, _) = bind {
                    return Some(GetTempTable(name));
                }
            }
            Rule::LogicalInsertToInsert => {
                if let LogicalInsert(_, _, _) = bind {
                    todo!()
                }
            }
            Rule::LogicalValuesToValues => {
                if let LogicalValues(_, _, _) = bind {
                    todo!()
                }
            }
            Rule::LogicalUpdateToUpdate => {
                if let LogicalUpdate(_, _) = bind {
                    todo!()
                }
            }
            Rule::LogicalDeleteToDelete => {
                if let LogicalDelete(_, _) = bind {
                    todo!()
                }
            }
            Rule::LogicalCreateDatabaseToCreateDatabase => {
                if let LogicalCreateDatabase { .. } = bind {
                    todo!()
                }
            }
            Rule::LogicalCreateTableToCreateTable => {
                if let LogicalCreateTable { .. } = bind {
                    todo!()
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
            Rule::LogicalInnerJoinCommutivity,
            Rule::LogicalInnerJoinAssociativity,
            Rule::LogicalGetToTableFreeScan,
            Rule::LogicalGetToSeqScan,
            Rule::LogicalGetToIndexScan,
            Rule::LogicalFilterToFilter,
            Rule::LogicalProjectToProject,
            Rule::LogicalJoinToNestedLoop,
            Rule::LogicalJoinToHashJoin,
            Rule::LogicalAggregateToAggregate,
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

fn can_index_scan(predicates: &Vec<Scalar>, table: &Table) -> bool {
    index_scan(predicates.clone(), table.clone()).is_some()
}

fn index_scan(predicates: Vec<Scalar>, table: Table) -> Option<Operator<Bind>> {
    // TODO real implementation
    if let Some((column, scalar)) = match_indexed_lookup(predicates) {
        if column.table.clone() == Some(table.name.clone()) {
            return Some(IndexScan {
                table,
                equals: vec![(column, scalar)],
            });
        }
    }
    None
}

fn match_indexed_lookup(mut predicates: Vec<Scalar>) -> Option<(Column, Scalar)> {
    if predicates.len() == 1 {
        if let Scalar::Call(Function::Equal, mut arguments, _) = predicates.pop().unwrap() {
            match (arguments.pop().unwrap(), arguments.pop().unwrap()) {
                (Scalar::Column(column), equals) | (equals, Scalar::Column(column))
                    if column.name.ends_with("_id") =>
                {
                    return Some((column, equals))
                }
                _ => {}
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
) -> (Vec<(Scalar, Scalar)>, Vec<Scalar>) {
    let mut hash_predicates = vec![];
    let mut remaining_predicates = vec![];
    for predicate in join_predicates.drain(0..) {
        if let Scalar::Call(Function::Equal, mut arguments, _) = predicate {
            let right_side = arguments.pop().unwrap();
            let left_side = arguments.pop().unwrap();
            if left_side.columns().all(|c| introduces(ss, left, c))
                && right_side.columns().all(|c| introduces(ss, right, c))
            {
                hash_predicates.push((left_side, right_side))
            } else if right_side.columns().all(|c| introduces(ss, left, c))
                && left_side.columns().all(|c| introduces(ss, right, c))
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

fn introduces(ss: &SearchSpace, gid: GroupID, column: &Column) -> bool {
    // use the first logical expression in the group to check for column
    let mid = ss[gid].logical[0];
    // check if the operator at the head of the logical expression introduces column
    let op = &ss[mid].op;
    if op.introduces(column) {
        return true;
    }
    // check if any of the inputs to the logical expression introduces column
    for i in 0..op.len() {
        if introduces(ss, op[i], column) {
            return true;
        }
    }
    return false;
}
