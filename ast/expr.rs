use kernel::*;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    fmt,
    hash::Hash,
};
use zetasql::TableRefProto;

use crate::{AggregateExpr, Column, Index, Procedure, Scalar};

// Expr plan nodes combine inputs in a Plan tree.
#[derive(Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Expr {
    Leaf {
        gid: usize,
    },
    // LogicalSingleGet acts as the input to a SELECT with no FROM clause.
    LogicalSingleGet,
    // LogicalGet { table } implements the FROM clause.
    LogicalGet {
        predicates: Vec<Scalar>,
        projects: Vec<Column>,
        table: Table,
    },
    // LogicalFilter { predicates } implements the WHERE/HAVING clauses.
    LogicalFilter {
        predicates: Vec<Scalar>,
        input: Box<Expr>,
    },
    LogicalOut {
        projects: Vec<Column>,
        input: Box<Expr>,
    },
    // LogicalMap { columns } implements the SELECT clause.
    LogicalMap {
        include_existing: bool,
        projects: Vec<(Scalar, Column)>,
        input: Box<Expr>,
    },
    // LogicalJoin implements the JOIN operator.
    LogicalJoin {
        join: Join,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    // LogicalJoin implements a special inner join where the right side (domain) is deduplicated,
    // and the left side (subquery) is allowed to depend on the right side.
    LogicalDependentJoin {
        parameters: Vec<Column>,
        predicates: Vec<Scalar>,
        subquery: Box<Expr>,
        domain: Box<Expr>,
    },
    // LogicalWith { table } implements with subquery as  _.
    // The with-subquery is always on the left.
    LogicalWith {
        name: String,
        // Matches 1:1 position-wise with LogicalGetWith.columns.
        columns: Vec<Column>,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    LogicalCreateTempTable {
        name: String,
        columns: Vec<Column>,
        input: Box<Expr>,
    },
    // LogicalGetWith { table } reads the subquery that was created by With.
    LogicalGetWith {
        name: String,
        // Matches 1:1 position-wise with LogicalWith.columns.
        columns: Vec<Column>,
    },
    // LogicalAggregate { group_by, aggregate } implements the GROUP BY clause.
    LogicalAggregate {
        group_by: Vec<Column>,
        aggregate: Vec<AggregateExpr>,
        input: Box<Expr>,
    },
    // LogicalLimit { n } implements the LIMIT / OFFSET / TOP clause.
    LogicalLimit {
        limit: usize,
        offset: usize,
        input: Box<Expr>,
    },
    // LogicalSort { columns } implements the ORDER BY clause.
    LogicalSort {
        order_by: Vec<OrderBy>,
        input: Box<Expr>,
    },
    // LogicalUnion implements SELECT _ UNION ALL SELECT _.
    LogicalUnion {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    // LogicalInsert { table, columns } implements the INSERT operation.
    LogicalInsert {
        table: Table,
        input: Box<Expr>,
        /// [(query_output_column, table_column), ..]
        columns: Vec<(Column, String)>,
    },
    // LogicalValues { columns, values } implements VALUES expressions.
    LogicalValues {
        columns: Vec<Column>,
        values: Vec<Vec<Scalar>>,
        input: Box<Expr>,
    },
    // LogicalUpdate { tid } implements the UPDATE operation.
    LogicalUpdate {
        table: Table,
        tid: Column,
        input: Box<Expr>,
        /// [(query_output_column, table_column), ..]
        columns: Vec<(Column, String)>,
    },
    // LogicalDelete { table } implements the DELETE operation.
    LogicalDelete {
        table: Table,
        tid: Column,
        input: Box<Expr>,
    },
    // LogicalCreateDatabase { database } implements the CREATE DATABASE operation.
    LogicalCreateDatabase {
        name: Name,
        reserved_id: i64,
    },
    // LogicalCreateTable implements the CREATE TABLE operation.
    LogicalCreateTable {
        name: Name,
        columns: Vec<(String, DataType)>,
        reserved_id: i64,
    },
    // LogicalCreateIndex implements the CREATE INDEX operation.
    LogicalCreateIndex {
        name: Name,
        table: Table,
        columns: Vec<String>,
        reserved_id: i64,
    },
    // LogicalDrop implements the DROP DATABASE/TABLE/INDEX operation.
    LogicalDrop {
        object: ObjectType,
        name: Name,
    },
    LogicalScript {
        statements: Vec<Expr>,
    },
    LogicalCall {
        procedure: Procedure,
        input: Box<Expr>,
    },
    LogicalExplain {
        input: Box<Expr>,
    },
    LogicalRewrite {
        sql: String,
    },
    TableFreeScan {
        worker: Option<i32>,
    },
    SeqScan {
        projects: Vec<Column>,
        predicates: Vec<Scalar>,
        table: Table,
    },
    IndexScan {
        include_existing: bool,
        projects: Vec<Column>,
        predicates: Vec<Scalar>,
        lookup: Vec<Scalar>,
        index: Index,
        table: Table,
        input: Box<Expr>,
    },
    Filter {
        predicates: Vec<Scalar>,
        input: Box<Expr>,
    },
    Out {
        projects: Vec<Column>,
        input: Box<Expr>,
    },
    Map {
        projects: Vec<(Scalar, Column)>,
        include_existing: bool,
        input: Box<Expr>,
    },
    NestedLoop {
        join: Join,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    HashJoin {
        broadcast: bool,
        join: Join,
        partition_left: Column,
        partition_right: Column,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    CreateTempTable {
        name: String,
        // Matches 1:1 position-wise with GetTempTable.columns.
        columns: Vec<Column>,
        input: Box<Expr>,
    },
    GetTempTable {
        name: String,
        // Matches 1:1 position-wise with CreateTempTable.columns.
        columns: Vec<Column>,
    },
    SimpleAggregate {
        worker: Option<i32>,
        aggregate: Vec<AggregateExpr>,
        input: Box<Expr>,
    },
    GroupByAggregate {
        partition_by: Column,
        group_by: Vec<Column>,
        aggregate: Vec<AggregateExpr>,
        input: Box<Expr>,
    },
    Limit {
        limit: usize,
        offset: usize,
        input: Box<Expr>,
    },
    Sort {
        order_by: Vec<OrderBy>,
        input: Box<Expr>,
    },
    Union {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// Broadcast the build side of a join to every node.
    Broadcast {
        stage: Option<i32>,
        input: Box<Expr>,
    },
    /// Exchange shuffles data during joins and aggregations.
    Exchange {
        stage: Option<i32>,
        /// hash_column is initially unset during the optimization phase, to reduce the size of the search space, then added before compilation.
        hash_column: Option<Column>,
        input: Box<Expr>,
    },
    /// Gather input on a single node for sorting.
    Gather {
        worker: Option<i32>,
        stage: Option<i32>,
        input: Box<Expr>,
    },
    Insert {
        table: Table,
        indexes: Vec<Index>,
        input: Box<Expr>,
        /// [(query_output_column, table_column), ..]
        columns: Vec<(Column, String)>,
    },
    Values {
        columns: Vec<Column>,
        values: Vec<Vec<Scalar>>,
        input: Box<Expr>,
    },
    Delete {
        table: Table,
        tid: Column,
        input: Box<Expr>,
    },
    Script {
        statements: Vec<Expr>,
    },
    Call {
        procedure: Procedure,
        input: Box<Expr>,
    },
    Explain {
        input: Box<Expr>,
    },
}

impl Expr {
    pub fn is_logical(&self) -> bool {
        match self {
            Expr::LogicalJoin { .. }
            | Expr::LogicalDependentJoin { .. }
            | Expr::LogicalWith { .. }
            | Expr::LogicalCreateTempTable { .. }
            | Expr::LogicalUnion { .. }
            | Expr::LogicalFilter { .. }
            | Expr::LogicalOut { .. }
            | Expr::LogicalMap { .. }
            | Expr::LogicalAggregate { .. }
            | Expr::LogicalLimit { .. }
            | Expr::LogicalSort { .. }
            | Expr::LogicalInsert { .. }
            | Expr::LogicalValues { .. }
            | Expr::LogicalUpdate { .. }
            | Expr::LogicalDelete { .. }
            | Expr::LogicalSingleGet
            | Expr::LogicalGet { .. }
            | Expr::LogicalGetWith { .. }
            | Expr::LogicalCreateDatabase { .. }
            | Expr::LogicalCreateTable { .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalDrop { .. }
            | Expr::LogicalRewrite { .. }
            | Expr::LogicalScript { .. }
            | Expr::LogicalCall { .. }
            | Expr::LogicalExplain { .. } => true,
            Expr::Leaf { .. }
            | Expr::TableFreeScan { .. }
            | Expr::SeqScan { .. }
            | Expr::IndexScan { .. }
            | Expr::Filter { .. }
            | Expr::Out { .. }
            | Expr::Map { .. }
            | Expr::NestedLoop { .. }
            | Expr::HashJoin { .. }
            | Expr::CreateTempTable { .. }
            | Expr::GetTempTable { .. }
            | Expr::SimpleAggregate { .. }
            | Expr::GroupByAggregate { .. }
            | Expr::Limit { .. }
            | Expr::Sort { .. }
            | Expr::Union { .. }
            | Expr::Broadcast { .. }
            | Expr::Exchange { .. }
            | Expr::Gather { .. }
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Delete { .. }
            | Expr::Script { .. }
            | Expr::Call { .. }
            | Expr::Explain { .. } => false,
        }
    }

    pub fn is_leaf(&self) -> bool {
        match self {
            Expr::Leaf { .. } => true,
            _ => false,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Expr::LogicalJoin { .. }
            | Expr::LogicalDependentJoin { .. }
            | Expr::LogicalWith { .. }
            | Expr::LogicalUnion { .. }
            | Expr::NestedLoop { .. }
            | Expr::HashJoin { .. }
            | Expr::Union { .. } => 2,
            Expr::LogicalFilter { .. }
            | Expr::LogicalOut { .. }
            | Expr::LogicalMap { .. }
            | Expr::LogicalAggregate { .. }
            | Expr::LogicalLimit { .. }
            | Expr::LogicalSort { .. }
            | Expr::LogicalInsert { .. }
            | Expr::LogicalValues { .. }
            | Expr::LogicalUpdate { .. }
            | Expr::LogicalDelete { .. }
            | Expr::Filter { .. }
            | Expr::Out { .. }
            | Expr::Map { .. }
            | Expr::IndexScan { .. }
            | Expr::SimpleAggregate { .. }
            | Expr::GroupByAggregate { .. }
            | Expr::Limit { .. }
            | Expr::Sort { .. }
            | Expr::Broadcast { .. }
            | Expr::Exchange { .. }
            | Expr::Gather { .. }
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Delete { .. }
            | Expr::LogicalCreateTempTable { .. }
            | Expr::CreateTempTable { .. }
            | Expr::LogicalCall { .. }
            | Expr::Call { .. }
            | Expr::LogicalExplain { .. }
            | Expr::Explain { .. } => 1,
            Expr::Leaf { .. }
            | Expr::LogicalSingleGet
            | Expr::LogicalGet { .. }
            | Expr::LogicalGetWith { .. }
            | Expr::LogicalCreateDatabase { .. }
            | Expr::LogicalCreateTable { .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalDrop { .. }
            | Expr::TableFreeScan { .. }
            | Expr::SeqScan { .. }
            | Expr::GetTempTable { .. }
            | Expr::LogicalRewrite { .. } => 0,
            Expr::LogicalScript { statements } | Expr::Script { statements } => statements.len(),
        }
    }

    pub fn pre_order(&self) -> PreOrderTraversal {
        PreOrderTraversal {
            deferred: vec![self],
        }
    }

    pub fn attributes(&self) -> HashSet<Column> {
        match self {
            Expr::LogicalGet { projects, .. } | Expr::LogicalOut { projects, .. } => {
                projects.iter().map(|c| c.clone()).collect()
            }
            Expr::LogicalMap {
                include_existing,
                projects,
                input,
            } => {
                let mut set = if *include_existing {
                    input.attributes()
                } else {
                    HashSet::new()
                };
                for (_, c) in projects {
                    set.insert(c.clone());
                }
                set
            }
            Expr::LogicalFilter { input, .. }
            | Expr::LogicalWith { right: input, .. }
            | Expr::LogicalLimit { input, .. }
            | Expr::LogicalSort { input, .. }
            | Expr::LogicalUnion { right: input, .. }
            | Expr::LogicalJoin {
                join: Join::Semi { .. },
                right: input,
                ..
            }
            | Expr::LogicalJoin {
                join: Join::Anti { .. },
                right: input,
                ..
            }
            | Expr::LogicalJoin {
                join: Join::Single { .. },
                right: input,
                ..
            }
            | Expr::LogicalCall { input, .. }
            | Expr::LogicalExplain { input, .. } => input.attributes(),
            Expr::LogicalJoin {
                join: Join::Mark(mark, _),
                right,
                ..
            } => {
                let mut set = right.attributes();
                set.insert(mark.clone());
                set
            }
            Expr::LogicalJoin {
                join: Join::Inner { .. },
                left,
                right,
                ..
            }
            | Expr::LogicalJoin {
                join: Join::Right { .. },
                left,
                right,
                ..
            }
            | Expr::LogicalJoin {
                join: Join::Outer { .. },
                left,
                right,
                ..
            }
            | Expr::LogicalDependentJoin {
                subquery: left,
                domain: right,
                ..
            } => {
                let mut set = left.attributes();
                set.extend(right.attributes());
                set
            }
            Expr::LogicalGetWith { columns, .. } | Expr::LogicalValues { columns, .. } => {
                columns.iter().map(|c| c.clone()).collect()
            }
            Expr::LogicalAggregate {
                group_by,
                aggregate,
                ..
            } => {
                let mut set = HashSet::new();
                set.extend(group_by.clone());
                for AggregateExpr { output, .. } in aggregate {
                    set.insert(output.clone());
                }
                set
            }
            Expr::Leaf { .. }
            | Expr::LogicalSingleGet { .. }
            | Expr::LogicalInsert { .. }
            | Expr::LogicalUpdate { .. }
            | Expr::LogicalDelete { .. }
            | Expr::LogicalCreateDatabase { .. }
            | Expr::LogicalCreateTempTable { .. }
            | Expr::LogicalCreateTable { .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalDrop { .. }
            | Expr::LogicalScript { .. }
            | Expr::LogicalRewrite { .. } => HashSet::new(),
            Expr::TableFreeScan { .. }
            | Expr::SeqScan { .. }
            | Expr::IndexScan { .. }
            | Expr::Filter { .. }
            | Expr::Out { .. }
            | Expr::Map { .. }
            | Expr::NestedLoop { .. }
            | Expr::HashJoin { .. }
            | Expr::CreateTempTable { .. }
            | Expr::GetTempTable { .. }
            | Expr::SimpleAggregate { .. }
            | Expr::GroupByAggregate { .. }
            | Expr::Limit { .. }
            | Expr::Sort { .. }
            | Expr::Union { .. }
            | Expr::Broadcast { .. }
            | Expr::Exchange { .. }
            | Expr::Gather { .. }
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Delete { .. }
            | Expr::Script { .. }
            | Expr::Call { .. }
            | Expr::Explain { .. } => panic!(
                "attributes is not implemented for physical operator {}",
                &self.name()
            ),
        }
    }

    pub fn references(&self) -> HashSet<Column> {
        let mut set = HashSet::new();
        self.collect_references(&mut set);
        set
    }

    fn collect_references(&self, set: &mut HashSet<Column>) {
        match self {
            Expr::LogicalGet { predicates, .. } | Expr::LogicalFilter { predicates, .. } => {
                for p in predicates {
                    p.collect_references(set);
                }
            }
            Expr::LogicalMap { projects, .. } => {
                for (x, _) in projects {
                    x.collect_references(set);
                }
            }
            Expr::LogicalJoin { join, .. } => {
                for p in join.predicates() {
                    p.collect_references(set);
                }
            }
            Expr::LogicalDependentJoin { predicates, .. } => {
                for p in predicates {
                    p.collect_references(set);
                }
            }
            Expr::LogicalWith { columns, .. } | Expr::LogicalGetWith { columns, .. } => {
                set.extend(columns.clone());
            }
            Expr::LogicalAggregate {
                group_by,
                aggregate,
                ..
            } => {
                set.extend(group_by.clone());
                for AggregateExpr { input, .. } in aggregate {
                    set.insert(input.clone());
                }
            }
            Expr::LogicalSort { order_by, .. } => {
                for o in order_by {
                    set.insert(o.column.clone());
                }
            }
            Expr::LogicalValues {
                columns, values, ..
            } => {
                set.extend(columns.clone());
                for array in values {
                    for x in array {
                        x.collect_references(set);
                    }
                }
            }
            Expr::LogicalCall { procedure, .. } => procedure.collect_references(set),
            Expr::LogicalSingleGet { .. }
            | Expr::Leaf { .. }
            | Expr::LogicalOut { .. }
            | Expr::LogicalLimit { .. }
            | Expr::LogicalUnion { .. }
            | Expr::LogicalInsert { .. }
            | Expr::LogicalUpdate { .. }
            | Expr::LogicalDelete { .. }
            | Expr::LogicalCreateDatabase { .. }
            | Expr::LogicalCreateTempTable { .. }
            | Expr::LogicalCreateTable { .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalDrop { .. }
            | Expr::LogicalScript { .. }
            | Expr::LogicalRewrite { .. }
            | Expr::LogicalExplain { .. } => {}
            Expr::TableFreeScan { .. }
            | Expr::SeqScan { .. }
            | Expr::IndexScan { .. }
            | Expr::Filter { .. }
            | Expr::Out { .. }
            | Expr::Map { .. }
            | Expr::NestedLoop { .. }
            | Expr::HashJoin { .. }
            | Expr::CreateTempTable { .. }
            | Expr::GetTempTable { .. }
            | Expr::SimpleAggregate { .. }
            | Expr::GroupByAggregate { .. }
            | Expr::Limit { .. }
            | Expr::Sort { .. }
            | Expr::Union { .. }
            | Expr::Broadcast { .. }
            | Expr::Exchange { .. }
            | Expr::Gather { .. }
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Delete { .. }
            | Expr::Script { .. }
            | Expr::Call { .. }
            | Expr::Explain { .. } => unimplemented!(
                "references is not implemented for physical operator {}",
                self.name()
            ),
        }
        for i in 0..self.len() {
            self[i].collect_references(set)
        }
    }

    pub fn subst(&mut self, map: &HashMap<Column, Column>) {
        let subst_c = |c: &Column| map.get(c).unwrap_or(c).clone();
        let subst_x = |x: &Scalar| x.clone().subst(map);
        let subst_o = |o: &OrderBy| OrderBy {
            column: map.get(&o.column).unwrap_or(&o.column).clone(),
            descending: o.descending,
        };
        match self {
            Expr::LogicalGet {
                projects,
                predicates,
                ..
            } => {
                *projects = projects.iter().map(subst_c).collect();
                *predicates = predicates.iter().map(subst_x).collect();
            }
            Expr::LogicalFilter { predicates, .. } => {
                *predicates = predicates.iter().map(subst_x).collect();
            }
            Expr::LogicalMap { projects, .. } => {
                *projects = projects
                    .iter()
                    .map(|(x, c)| (subst_x(x), subst_c(c)))
                    .collect();
            }
            Expr::LogicalJoin { join, .. } => {
                *join = join.replace(join.predicates().iter().map(subst_x).collect());
            }
            Expr::LogicalDependentJoin {
                parameters,
                predicates,
                ..
            } => {
                *parameters = parameters.iter().map(subst_c).collect();
                *predicates = predicates.iter().map(subst_x).collect();
            }
            Expr::LogicalWith { columns, .. }
            | Expr::LogicalCreateTempTable { columns, .. }
            | Expr::LogicalGetWith { columns, .. } => {
                *columns = columns.iter().map(subst_c).collect();
            }
            Expr::LogicalAggregate {
                group_by,
                aggregate,
                ..
            } => {
                *group_by = group_by.iter().map(subst_c).collect();
                *aggregate = aggregate
                    .iter()
                    .map(
                        |AggregateExpr {
                             function,
                             distinct,
                             input,
                             output,
                         }| AggregateExpr {
                            function: *function,
                            distinct: *distinct,
                            input: subst_c(input),
                            output: subst_c(output),
                        },
                    )
                    .collect();
            }
            Expr::LogicalSort { order_by, .. } => {
                *order_by = order_by.iter().map(subst_o).collect()
            }
            Expr::LogicalValues {
                columns, values, ..
            } => {
                *columns = columns.iter().map(subst_c).collect();
                *values = values
                    .iter()
                    .map(|row| row.iter().map(subst_x).collect())
                    .collect();
            }
            Expr::LogicalOut { projects, .. } => {
                *projects = projects.iter().map(subst_c).collect();
            }
            Expr::LogicalUpdate { tid, .. } | Expr::LogicalDelete { tid, .. } => {
                *tid = subst_c(&tid);
            }
            Expr::Leaf { .. }
            | Expr::LogicalSingleGet
            | Expr::LogicalLimit { .. }
            | Expr::LogicalUnion { .. }
            | Expr::LogicalInsert { .. }
            | Expr::LogicalCreateDatabase { .. }
            | Expr::LogicalCreateTable { .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalDrop { .. }
            | Expr::LogicalScript { .. }
            | Expr::LogicalCall { .. }
            | Expr::LogicalExplain { .. }
            | Expr::LogicalRewrite { .. } => {}
            Expr::TableFreeScan { .. }
            | Expr::SeqScan { .. }
            | Expr::IndexScan { .. }
            | Expr::Filter { .. }
            | Expr::Out { .. }
            | Expr::Map { .. }
            | Expr::NestedLoop { .. }
            | Expr::HashJoin { .. }
            | Expr::CreateTempTable { .. }
            | Expr::GetTempTable { .. }
            | Expr::SimpleAggregate { .. }
            | Expr::GroupByAggregate { .. }
            | Expr::Limit { .. }
            | Expr::Sort { .. }
            | Expr::Union { .. }
            | Expr::Broadcast { .. }
            | Expr::Exchange { .. }
            | Expr::Gather { .. }
            | Expr::Insert { .. }
            | Expr::Values { .. }
            | Expr::Delete { .. }
            | Expr::Script { .. }
            | Expr::Call { .. }
            | Expr::Explain { .. } => panic!(
                "subst is not implemented for physical operator {}",
                self.name()
            ),
        }
        for i in 0..self.len() {
            self[i].subst(map)
        }
    }

    pub fn schema(&self) -> Vec<(String, DataType)> {
        use Expr::*;

        fn dummy_schema() -> Vec<(String, DataType)> {
            vec![("$dummy".to_string(), DataType::Bool)]
        }

        match self {
            TableFreeScan { .. } => dummy_schema(),
            Filter { input, .. }
            | Limit { input, .. }
            | Sort { input, .. }
            | Union { left: input, .. }
            | Broadcast { input, .. }
            | Exchange { input, .. }
            | Gather { input, .. }
            | Delete { input, .. } => input.schema(),
            SeqScan { projects, .. } => projects
                .iter()
                .map(|c| (c.canonical_name(), c.data_type))
                .collect(),
            Out { projects, .. } => projects
                .iter()
                .map(|c| (c.name.clone(), c.data_type))
                .collect(),
            IndexScan {
                include_existing,
                projects,
                input,
                ..
            } => {
                let mut fields: Vec<_> = projects
                    .iter()
                    .map(|c| (c.canonical_name(), c.data_type))
                    .collect();
                if *include_existing {
                    fields.extend_from_slice(&input.schema());
                }
                fields
            }
            Map {
                include_existing,
                projects,
                input,
            } => {
                let mut fields: Vec<_> = projects
                    .iter()
                    .map(|(_, c)| (c.canonical_name(), c.data_type))
                    .collect();
                if *include_existing {
                    fields.extend_from_slice(&input.schema());
                }
                fields
            }
            NestedLoop {
                join, left, right, ..
            } => {
                let mut fields = vec![];
                fields.extend_from_slice(&left.schema());
                fields.extend_from_slice(&right.schema());
                if let Join::Mark(column, _) = join {
                    fields.push((column.canonical_name(), column.data_type))
                }
                fields
            }
            HashJoin {
                join, left, right, ..
            } => {
                let mut fields = vec![];
                fields.extend_from_slice(&left.schema());
                fields.extend_from_slice(&right.schema());
                if let Join::Mark(column, _) = join {
                    fields.push((column.canonical_name(), column.data_type))
                }
                fields
            }
            GetTempTable { columns, .. } => columns
                .iter()
                .map(|column| (column.canonical_name(), column.data_type))
                .collect(),
            SimpleAggregate { aggregate, .. } => {
                let mut fields = vec![];
                for a in aggregate {
                    fields.push((a.output.canonical_name(), a.output.data_type));
                }
                fields
            }
            GroupByAggregate {
                group_by,
                aggregate,
                ..
            } => {
                let mut fields = vec![];
                for column in group_by {
                    fields.push((column.canonical_name(), column.data_type));
                }
                for a in aggregate {
                    fields.push((a.output.canonical_name(), a.output.data_type));
                }
                fields
            }
            Values { columns, .. } => columns
                .iter()
                .map(|column| (column.canonical_name(), column.data_type))
                .collect(),
            Script { statements, .. } => statements.last().unwrap().schema(),
            Explain { .. } => vec![("plan".to_string(), DataType::String)],
            CreateTempTable { .. } | Insert { .. } | Call { .. } => dummy_schema(),
            Leaf { .. }
            | LogicalSingleGet { .. }
            | LogicalGet { .. }
            | LogicalFilter { .. }
            | LogicalOut { .. }
            | LogicalMap { .. }
            | LogicalJoin { .. }
            | LogicalDependentJoin { .. }
            | LogicalWith { .. }
            | LogicalCreateTempTable { .. }
            | LogicalGetWith { .. }
            | LogicalAggregate { .. }
            | LogicalLimit { .. }
            | LogicalSort { .. }
            | LogicalUnion { .. }
            | LogicalInsert { .. }
            | LogicalValues { .. }
            | LogicalUpdate { .. }
            | LogicalDelete { .. }
            | LogicalCreateDatabase { .. }
            | LogicalCreateTable { .. }
            | LogicalCreateIndex { .. }
            | LogicalDrop { .. }
            | LogicalScript { .. }
            | LogicalCall { .. }
            | LogicalExplain { .. }
            | LogicalRewrite { .. } => panic!(
                "schema() is not implemented for logical operator {}",
                self.name()
            ),
        }
    }
}

impl std::ops::Index<usize> for Expr {
    type Output = Expr;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            Expr::LogicalJoin { left, right, .. }
            | Expr::LogicalDependentJoin {
                subquery: left,
                domain: right,
                ..
            }
            | Expr::LogicalWith { left, right, .. }
            | Expr::LogicalUnion { left, right, .. }
            | Expr::NestedLoop { left, right, .. }
            | Expr::HashJoin { left, right, .. }
            | Expr::Union { left, right } => match index {
                0 => left,
                1 => right,
                _ => panic!("{} is out of bounds [0,2)", index),
            },
            Expr::LogicalFilter { input, .. }
            | Expr::LogicalOut { input, .. }
            | Expr::LogicalMap { input, .. }
            | Expr::LogicalAggregate { input, .. }
            | Expr::LogicalLimit { input, .. }
            | Expr::LogicalSort { input, .. }
            | Expr::LogicalInsert { input, .. }
            | Expr::LogicalValues { input, .. }
            | Expr::LogicalUpdate { input, .. }
            | Expr::LogicalDelete { input, .. }
            | Expr::Filter { input, .. }
            | Expr::Out { input, .. }
            | Expr::Map { input, .. }
            | Expr::IndexScan { input, .. }
            | Expr::SimpleAggregate { input, .. }
            | Expr::GroupByAggregate { input, .. }
            | Expr::Limit { input, .. }
            | Expr::Sort { input, .. }
            | Expr::Broadcast { input, .. }
            | Expr::Exchange { input, .. }
            | Expr::Gather { input, .. }
            | Expr::Insert { input, .. }
            | Expr::Values { input, .. }
            | Expr::Delete { input, .. }
            | Expr::LogicalCreateTempTable { input, .. }
            | Expr::CreateTempTable { input, .. }
            | Expr::LogicalCall { input, .. }
            | Expr::Call { input, .. }
            | Expr::LogicalExplain { input, .. }
            | Expr::Explain { input, .. } => match index {
                0 => input,
                _ => panic!("{} is out of bounds [0,1)", index),
            },
            Expr::Leaf { .. }
            | Expr::LogicalSingleGet { .. }
            | Expr::LogicalGet { .. }
            | Expr::LogicalGetWith { .. }
            | Expr::LogicalCreateDatabase { .. }
            | Expr::LogicalCreateTable { .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalDrop { .. }
            | Expr::TableFreeScan { .. }
            | Expr::SeqScan { .. }
            | Expr::GetTempTable { .. }
            | Expr::LogicalRewrite { .. } => panic!("{} has no inputs", self.name()),
            Expr::LogicalScript { statements } | Expr::Script { statements } => &statements[index],
        }
    }
}

impl std::ops::IndexMut<usize> for Expr {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        match self {
            Expr::LogicalJoin { left, right, .. }
            | Expr::LogicalDependentJoin {
                subquery: left,
                domain: right,
                ..
            }
            | Expr::LogicalWith { left, right, .. }
            | Expr::LogicalUnion { left, right, .. }
            | Expr::NestedLoop { left, right, .. }
            | Expr::HashJoin { left, right, .. }
            | Expr::Union { left, right } => match index {
                0 => left.as_mut(),
                1 => right.as_mut(),
                _ => panic!("{} is out of bounds [0,2)", index),
            },
            Expr::LogicalFilter { input, .. }
            | Expr::LogicalOut { input, .. }
            | Expr::LogicalMap { input, .. }
            | Expr::LogicalAggregate { input, .. }
            | Expr::LogicalLimit { input, .. }
            | Expr::LogicalSort { input, .. }
            | Expr::LogicalInsert { input, .. }
            | Expr::LogicalValues { input, .. }
            | Expr::LogicalUpdate { input, .. }
            | Expr::LogicalDelete { input, .. }
            | Expr::Filter { input, .. }
            | Expr::Out { input, .. }
            | Expr::Map { input, .. }
            | Expr::IndexScan { input, .. }
            | Expr::SimpleAggregate { input, .. }
            | Expr::GroupByAggregate { input, .. }
            | Expr::Limit { input, .. }
            | Expr::Sort { input, .. }
            | Expr::Broadcast { input, .. }
            | Expr::Exchange { input, .. }
            | Expr::Gather { input, .. }
            | Expr::Insert { input, .. }
            | Expr::Values { input, .. }
            | Expr::Delete { input, .. }
            | Expr::LogicalCreateTempTable { input, .. }
            | Expr::CreateTempTable { input, .. }
            | Expr::LogicalCall { input, .. }
            | Expr::Call { input, .. }
            | Expr::LogicalExplain { input, .. }
            | Expr::Explain { input, .. } => match index {
                0 => input.as_mut(),
                _ => panic!("{} is out of bounds [0,1)", index),
            },
            Expr::Leaf { .. }
            | Expr::LogicalSingleGet { .. }
            | Expr::LogicalGet { .. }
            | Expr::LogicalGetWith { .. }
            | Expr::LogicalCreateDatabase { .. }
            | Expr::LogicalCreateTable { .. }
            | Expr::LogicalCreateIndex { .. }
            | Expr::LogicalDrop { .. }
            | Expr::TableFreeScan { .. }
            | Expr::SeqScan { .. }
            | Expr::GetTempTable { .. }
            | Expr::LogicalRewrite { .. } => panic!("{} has no inputs", self.name()),
            Expr::LogicalScript { statements } | Expr::Script { statements } => {
                &mut statements[index]
            }
        }
    }
}

impl Default for Expr {
    fn default() -> Self {
        Expr::Leaf { gid: usize::MAX }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Join {
    // Inner implements the default SQL join.
    Inner(Vec<Scalar>),
    // Right is used for both left and right joins.
    // When parsing a left join, we convert it to a right join by reversing the order of left and right.
    // The left side is the build side of the join.
    Right(Vec<Scalar>),
    // Outer includes non-matching rows from both sides.
    Outer(Vec<Scalar>),
    // Semi indicates a semi-join like `select 1 from customer where store_id in (select store_id from store)`.
    // Note that the left side is the build side of the join, which is the reverse of the usual convention.
    Semi(Vec<Scalar>),
    // Anti indicates an anti-join like `select 1 from customer where store_id not in (select store_id from store)`.
    // Note that like Semi, the left side is the build side of the join.
    Anti(Vec<Scalar>),
    // Single implements "Single Join" from http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
    // The correlated subquery is always on the left.
    Single(Vec<Scalar>),
    // Mark implements "Mark Join" from http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
    // The correlated subquery is always on the left.
    Mark(Column, Vec<Scalar>),
}

impl Join {
    pub fn predicates(&self) -> &Vec<Scalar> {
        match self {
            Join::Inner(predicates)
            | Join::Right(predicates)
            | Join::Outer(predicates)
            | Join::Semi(predicates)
            | Join::Anti(predicates)
            | Join::Single(predicates)
            | Join::Mark(_, predicates) => predicates,
        }
    }

    pub fn replace(&self, predicates: Vec<Scalar>) -> Self {
        match self {
            Join::Inner(_) => Join::Inner(predicates),
            Join::Right(_) => Join::Right(predicates),
            Join::Outer(_) => Join::Outer(predicates),
            Join::Semi(_) => Join::Semi(predicates),
            Join::Anti(_) => Join::Anti(predicates),
            Join::Single(_) => Join::Single(predicates),
            Join::Mark(mark, _) => Join::Mark(mark.clone(), predicates),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Table {
    pub id: i64,
    pub name: String,
}

impl Table {
    pub fn from(table: &TableRefProto) -> Self {
        Table {
            id: table.serialization_id.unwrap(),
            name: table.name.as_ref().unwrap().clone(),
        }
    }
}

impl PartialOrd for Table {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Table {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Name {
    pub catalog_id: i64,
    pub path: Vec<String>,
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path.join("."))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum ObjectType {
    Database,
    Table,
    Index,
    Column,
}

impl ObjectType {
    pub fn from(name: &String) -> Self {
        match name.to_lowercase().as_str() {
            "database" => ObjectType::Database,
            "table" => ObjectType::Table,
            "index" => ObjectType::Index,
            "column" => ObjectType::Column,
            _ => panic!("{}", name),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct OrderBy {
    pub column: Column,
    pub descending: bool,
}

impl fmt::Display for OrderBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.descending {
            write!(f, "-{}", self.column)
        } else {
            write!(f, "{}", self.column)
        }
    }
}

pub struct PreOrderTraversal<'it> {
    deferred: Vec<&'it Expr>,
}

impl<'it> Iterator for PreOrderTraversal<'it> {
    type Item = &'it Expr;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.deferred.pop()?;
        for i in 0..next.len() {
            self.deferred.push(&next[next.len() - 1 - i]);
        }
        Some(next)
    }
}
