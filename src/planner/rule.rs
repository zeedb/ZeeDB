use crate::search_space::*;
use arrow::datatypes::*;
use ast::*;
use std::collections::{HashMap, HashSet};
use std::fmt;

#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub enum Rule {
    // Rewrite rules
    InnerJoinCommutivity,
    InnerJoinAssociativity,
    RemoveDependentJoin,
    RewriteDependentJoin,
    // Implementation rules
    LogicalGetToTableFreeScan,
    LogicalGetToSeqScan,
    LogicalGetToIndexScan,
    LogicalFilterToFilter,
    LogicalMapToMap,
    LogicalJoinToNestedLoop,
    LogicalJoinToHashJoin,
    LogicalJoinToLookupJoin,
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
            | Rule::LogicalMapToMap
            | Rule::LogicalJoinToNestedLoop
            | Rule::LogicalJoinToHashJoin
            | Rule::LogicalJoinToLookupJoin
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
            | (Rule::RemoveDependentJoin, LogicalDependentJoin { .. })
            | (Rule::RewriteDependentJoin, LogicalDependentJoin { .. })
            | (Rule::LogicalGetToTableFreeScan, LogicalSingleGet)
            | (Rule::LogicalGetToSeqScan, LogicalGet { .. })
            | (Rule::LogicalGetToIndexScan, LogicalGet { .. })
            | (Rule::LogicalFilterToFilter, LogicalFilter(_, _))
            | (Rule::LogicalMapToMap, LogicalMap { .. })
            | (Rule::LogicalJoinToNestedLoop, LogicalJoin { .. })
            | (Rule::LogicalJoinToHashJoin, LogicalJoin { .. })
            | (Rule::LogicalJoinToLookupJoin, LogicalJoin { .. })
            | (Rule::LogicalAggregateToAggregate, LogicalAggregate { .. })
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
            (Rule::LogicalJoinToLookupJoin, 0) => true,
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
                    if let Leaf(left) = left.as_ref() {
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
            Rule::LogicalJoinToLookupJoin => {
                if let LogicalJoin { join, left, right } = &ss[mid].expr {
                    if let Leaf(left) = left.as_ref() {
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

    pub fn apply(&self, ss: &SearchSpace, bind: Expr) -> Option<Expr> {
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
                    if let Leaf(right) = right.as_ref() {
                        if let LogicalJoin {
                            join: Join::Inner(left_predicates),
                            left: left_left,
                            right: left_middle,
                            ..
                        } = *left
                        {
                            if let (Leaf(left_left), Leaf(left_middle)) =
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
                                    left: Box::new(Leaf(*left_left)),
                                    right: Box::new(LogicalJoin {
                                        join: Join::Inner(new_right_predicates),
                                        left: Box::new(Leaf(*left_middle)),
                                        right: Box::new(Leaf(*right)),
                                    }),
                                });
                            }
                        }
                    }
                }
            }
            Rule::RemoveDependentJoin => {
                if let LogicalDependentJoin {
                    parameters,
                    predicates,
                    subquery,
                    ..
                } = bind
                {
                    debug_assert!(!parameters.is_empty());
                    if let Leaf(subquery) = subquery.as_ref() {
                        // Check if predicates contains subquery.a = domain.b for every b in domain.
                        let subquery_scope =
                            &ss[GroupID(*subquery)].props.column_unique_cardinality;
                        let match_equals = |x: &Scalar| {
                            if let Scalar::Call(Function::Equal, args, _) = x {
                                if let Scalar::Column(left) = &args[0] {
                                    if let Scalar::Column(right) = &args[1] {
                                        if subquery_scope.contains_key(&left)
                                            && parameters.contains(&right)
                                        {
                                            return Some((left.clone(), right.clone()));
                                        } else if subquery_scope.contains_key(&right)
                                            && parameters.contains(&left)
                                        {
                                            return Some((right.clone(), left.clone()));
                                        }
                                    }
                                }
                            }
                            None
                        };
                        let mut equiv_predicates = HashMap::new();
                        let mut filter_predicates = vec![];
                        for p in predicates {
                            if let Some((subquery_column, domain_column)) = match_equals(&p) {
                                equiv_predicates.insert(domain_column, subquery_column);
                            } else {
                                filter_predicates.push(p.clone())
                            }
                        }
                        if !parameters.iter().all(|c| equiv_predicates.contains_key(c)) {
                            return None;
                        }
                        // Infer domain from subquery using equivalences from the join condition.
                        let project_domain: Vec<(Scalar, Column)> = parameters
                            .iter()
                            .map(|c| (Scalar::Column(equiv_predicates[c].clone()), c.clone()))
                            .collect();
                        if filter_predicates.is_empty() {
                            return Some(LogicalMap {
                                include_existing: true,
                                projects: project_domain,
                                input: Box::new(Leaf(*subquery)),
                            });
                        } else {
                            return Some(LogicalFilter(
                                filter_predicates,
                                Box::new(LogicalMap {
                                    include_existing: true,
                                    projects: project_domain,
                                    input: Box::new(Leaf(*subquery)),
                                }),
                            ));
                        }
                    }
                }
            }
            Rule::RewriteDependentJoin => {
                if let LogicalDependentJoin {
                    parameters,
                    predicates,
                    subquery,
                    domain,
                } = bind
                {
                    return Some(LogicalJoin {
                        join: Join::Inner(predicates),
                        left: subquery,
                        right: Box::new(LogicalAggregate {
                            group_by: parameters,
                            aggregate: vec![],
                            input: domain,
                        }),
                    });
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
                    if let Some(i) = find_index_scan(&predicates, &table) {
                        let mut predicates = predicates;
                        let (c, x) = unpack_index_lookup(predicates.remove(i), &table).unwrap();
                        return Some(IndexScan {
                            projects,
                            predicates,
                            table,
                            index_predicates: vec![(c, x)],
                        });
                    }
                }
            }
            Rule::LogicalFilterToFilter => {
                if let LogicalFilter(predicates, input) = bind {
                    return Some(Filter(predicates, input));
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
                    return Some(NestedLoop(join, left, right));
                }
            }
            Rule::LogicalJoinToHashJoin => {
                if let LogicalJoin { join, left, right } = bind {
                    if let (Leaf(left), Leaf(right)) = (left.as_ref(), right.as_ref()) {
                        let (equi_predicates, remaining_predicates) = hash_join(
                            ss,
                            join.predicates().clone(),
                            GroupID(*left),
                            GroupID(*right),
                        );
                        let join = join.replace(remaining_predicates);
                        if !equi_predicates.is_empty() {
                            return Some(HashJoin {
                                join,
                                equi_predicates,
                                left: Box::new(Leaf(*left)),
                                right: Box::new(Leaf(*right)),
                            });
                        }
                    }
                }
            }
            Rule::LogicalJoinToLookupJoin => {
                if let LogicalJoin { join, left, right } = bind {
                    if let LogicalGet {
                        predicates: table_predicates,
                        projects,
                        table,
                    } = *left
                    {
                        let mut predicates = join.predicates().clone();
                        predicates.extend(table_predicates);
                        if let Some(i) = find_index_scan(&predicates, &table) {
                            let (c, x) = unpack_index_lookup(predicates.remove(i), &table).unwrap();
                            return Some(LookupJoin {
                                join: join.replace(predicates),
                                projects,
                                table,
                                index_predicates: vec![(c, x)],
                                input: right,
                            });
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
            Rule::RemoveDependentJoin,
            Rule::RewriteDependentJoin,
            Rule::LogicalGetToTableFreeScan,
            Rule::LogicalGetToSeqScan,
            Rule::LogicalGetToIndexScan,
            Rule::LogicalFilterToFilter,
            Rule::LogicalMapToMap,
            Rule::LogicalJoinToNestedLoop,
            Rule::LogicalJoinToHashJoin,
            Rule::LogicalJoinToLookupJoin,
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
            (Scalar::Column(column), index_predicates)
            | (index_predicates, Scalar::Column(column))
                if column.name.ends_with("_id") && column.table.as_ref() == Some(&table.name) =>
            {
                return Some((column, index_predicates))
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
    let mut equi_predicates = vec![];
    let mut remaining_predicates = vec![];
    for predicate in join_predicates.drain(0..) {
        if let Scalar::Call(Function::Equal, mut arguments, _) = predicate {
            let right_side = arguments.pop().unwrap();
            let left_side = arguments.pop().unwrap();
            if contains_all(&ss[left], left_side.references())
                && contains_all(&ss[right], right_side.references())
            {
                equi_predicates.push((left_side, right_side))
            } else if contains_all(&ss[right], left_side.references())
                && contains_all(&ss[left], right_side.references())
            {
                equi_predicates.push((right_side, left_side))
            } else {
                remaining_predicates.push(Scalar::Call(
                    Function::Equal,
                    vec![left_side, right_side],
                    DataType::Boolean,
                ));
            }
        } else {
            remaining_predicates.push(predicate);
        }
    }
    (equi_predicates, remaining_predicates)
}

fn contains_all(group: &Group, columns: HashSet<Column>) -> bool {
    columns
        .iter()
        .all(|c| group.props.column_unique_cardinality.contains_key(c))
}
