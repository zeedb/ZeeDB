use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use ast::{Expr, Index, *};
use globals::Global;
use kernel::*;
use remote_execution::RecordStream;
use storage::{Heap, Page, Storage, PAGE_SIZE};

use crate::{hash_table::HashTable, index::PackedBytes};

#[derive(Debug)]
pub enum Node {
    TableFreeScan {
        empty: bool,
    },
    SeqScan {
        projects: Vec<Column>,
        predicates: Vec<Scalar>,
        table: Table,
        scan: Option<Vec<Arc<Page>>>,
    },
    IndexScan {
        include_existing: bool,
        projects: Vec<Column>,
        predicates: Vec<Scalar>,
        lookup: Vec<Scalar>,
        index: Index,
        table: Table,
        input: Box<Node>,
    },
    Filter {
        predicates: Vec<Scalar>,
        input: Box<Node>,
    },
    Out {
        projects: Vec<Column>,
        input: Box<Node>,
    },
    Map {
        include_existing: bool,
        projects: Vec<(Scalar, Column)>,
        input: Box<Node>,
    },
    NestedLoop {
        join: Join,
        left: Box<Node>,
        left_schema: Vec<(String, DataType)>,
        build_left: Option<RecordBatch>,
        unmatched_left: Option<BoolArray>,
        right: Box<Node>,
        right_schema: Vec<(String, DataType)>,
    },
    HashJoin {
        join: Join,
        partition_left: Column,
        partition_right: Column,
        left: Box<Node>,
        left_schema: Vec<(String, DataType)>,
        build_left: Option<HashTable>,
        unmatched_left: Option<BoolArray>,
        right: Box<Node>,
        right_schema: Vec<(String, DataType)>,
    },
    CreateTempTable {
        name: String,
        columns: Vec<Column>,
        input: Box<Node>,
    },
    GetTempTable {
        name: String,
        columns: Vec<Column>,
        scan: Option<Vec<Arc<Page>>>,
    },
    SimpleAggregate {
        finished: bool,
        aggregate: Vec<AggregateExpr>,
        input: Box<Node>,
    },
    GroupByAggregate {
        finished: bool,
        group_by: Vec<Column>,
        aggregate: Vec<AggregateExpr>,
        input: Box<Node>,
    },
    Limit {
        cursor: usize,
        limit: usize,
        offset: usize,
        input: Box<Node>,
    },
    Sort {
        order_by: Vec<OrderBy>,
        input: Box<Node>,
    },
    Union {
        left: Box<Node>,
        right: Box<Node>,
    },
    Broadcast {
        stage: i32,
        input: Option<Expr>,
        stream: Option<RemoteQuery>,
    },
    Exchange {
        stage: i32,
        input: Option<(Column, Expr)>,
        stream: Option<RemoteQuery>,
    },
    Gather {
        stage: i32,
        input: Option<Expr>,
        stream: Option<RemoteQuery>,
    },
    Insert {
        finished: bool,
        table: Table,
        indexes: Vec<Index>,
        input: Box<Node>,
        /// [(query_output_column, table_column), ..]
        columns: Vec<(Column, String)>,
    },
    Values {
        columns: Vec<Column>,
        values: Vec<Vec<Scalar>>,
        input: Box<Node>,
    },
    Delete {
        table: Table,
        tid: Column,
        input: Box<Node>,
    },
    Script {
        offset: usize,
        stmts: Vec<Node>,
    },
    Call {
        procedure: Procedure,
        input: Box<Node>,
    },
    Explain {
        finished: bool,
        input: Expr,
    },
}

pub struct RemoteQuery {
    inner: RecordStream,
}

impl Node {
    pub fn compile(expr: Expr) -> Self {
        match expr {
            TableFreeScan => Node::TableFreeScan { empty: false },
            SeqScan {
                projects,
                predicates,
                table,
            } => Node::SeqScan {
                projects,
                predicates,
                table,
                scan: None,
            },
            IndexScan {
                include_existing,
                projects,
                predicates,
                lookup,
                index,
                table,
                input,
            } => Node::IndexScan {
                include_existing,
                projects,
                predicates,
                lookup,
                index,
                table,
                input: Box::new(Node::compile(*input)),
            },
            Filter { predicates, input } => Node::Filter {
                predicates,
                input: Box::new(Node::compile(*input)),
            },
            Out { projects, input } => Node::Out {
                projects,
                input: Box::new(Node::compile(*input)),
            },
            Map {
                include_existing,
                projects,
                input,
            } => Node::Map {
                include_existing,
                projects,
                input: Box::new(Node::compile(*input)),
            },
            NestedLoop { join, left, right } => {
                let left_schema = left.schema();
                let right_schema = right.schema();
                Node::NestedLoop {
                    join,
                    left: Box::new(Node::compile(*left)),
                    left_schema,
                    build_left: None,
                    unmatched_left: None,
                    right: Box::new(Node::compile(*right)),
                    right_schema,
                }
            }
            HashJoin {
                join,
                partition_left,
                partition_right,
                left,
                right,
                ..
            } => {
                let left_schema = left.schema();
                let right_schema = right.schema();
                let left = Box::new(Node::compile(*left));
                let right = Box::new(Node::compile(*right));
                Node::HashJoin {
                    join,
                    partition_left,
                    partition_right,
                    left,
                    left_schema,
                    build_left: None,
                    unmatched_left: None,
                    right,
                    right_schema,
                }
            }
            CreateTempTable {
                name,
                columns,
                input,
            } => Node::CreateTempTable {
                name,
                columns,
                input: Box::new(Node::compile(*input)),
            },
            GetTempTable { name, columns } => Node::GetTempTable {
                name,
                columns,
                scan: None,
            },
            SimpleAggregate {
                aggregate, input, ..
            } => Node::SimpleAggregate {
                finished: false,
                aggregate,
                input: Box::new(Node::compile(*input)),
            },
            GroupByAggregate {
                group_by,
                aggregate,
                input,
                ..
            } => Node::GroupByAggregate {
                finished: false,
                group_by,
                aggregate,
                input: Box::new(Node::compile(*input)),
            },
            Limit {
                limit,
                offset,
                input,
            } => Node::Limit {
                cursor: 0,
                limit,
                offset,
                input: Box::new(Node::compile(*input)),
            },
            Sort { order_by, input } => Node::Sort {
                order_by,
                input: Box::new(Node::compile(*input)),
            },
            Union { left, right } => Node::Union {
                left: Box::new(Node::compile(*left)),
                right: Box::new(Node::compile(*right)),
            },
            Broadcast { input, stage } => Node::Broadcast {
                input: Some(*input),
                stream: None,
                stage: stage.unwrap(),
            },
            Exchange {
                hash_column,
                input,
                stage,
            } => Node::Exchange {
                input: Some((hash_column.unwrap(), *input)),
                stream: None,
                stage: stage.unwrap(),
            },
            Gather { input, stage } => Node::Gather {
                stage: stage.unwrap(),
                input: Some(*input),
                stream: None,
            },
            Insert {
                table,
                indexes,
                input,
                columns,
            } => Node::Insert {
                finished: false,
                table,
                indexes,
                input: Box::new(Node::compile(*input)),
                columns,
            },
            Values {
                columns,
                values,
                input,
            } => Node::Values {
                columns,
                values,
                input: Box::new(Node::compile(*input)),
            },
            Delete { table, tid, input } => Node::Delete {
                table,
                tid,
                input: Box::new(Node::compile(*input)),
            },
            Script { stmts } => {
                let mut compiled = vec![];
                for expr in stmts {
                    compiled.push(Node::compile(expr))
                }
                Node::Script {
                    offset: 0,
                    stmts: compiled,
                }
            }
            Call { procedure, input } => Node::Call {
                procedure,
                input: Box::new(Node::compile(*input)),
            },
            Explain { input } => Node::Explain {
                finished: false,
                input: *input,
            },
            Leaf { .. }
            | LogicalSingleGet
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
            | LogicalRewrite { .. }
            | LogicalCall { .. }
            | LogicalExplain { .. } => panic!("logical operation"),
        }
    }
}

impl Node {
    pub fn next(&mut self, storage: &Mutex<Storage>, txn: i64) -> Next {
        let _span = log::enter(self.name());
        match self {
            Node::TableFreeScan { empty } => {
                // Produce a single row on worker 0.
                if *empty || globals::WORKER.get() != leader(txn) {
                    return Next::End;
                }
                *empty = true;
                Next::Page(dummy_row())
            }
            Node::SeqScan {
                projects,
                predicates,
                table,
                scan,
            } => {
                if scan.is_none() {
                    *scan = Some(storage.lock().unwrap().table(table.id).scan());
                }
                let page = match scan.as_mut().unwrap().pop() {
                    Some(page) => page,
                    None => return Next::End,
                };
                let select_names = projects.iter().map(|c| c.name.clone()).collect();
                let query_names = projects
                    .iter()
                    .map(|c| (c.name.clone(), c.canonical_name()))
                    .collect();
                let input = page.select(&select_names).rename(&query_names);
                let boolean = crate::eval::all(predicates, &input, storage, txn)?;
                Next::Page(input.compress(&boolean))
            }
            Node::IndexScan {
                include_existing,
                projects,
                predicates,
                lookup,
                index,
                table,
                input,
            } => {
                let input = input.next(storage, txn)?;
                // Perform a bitmap scan on the left side of the join.
                let keys = evaluate_index_keys(lookup, &input, storage, txn)?;
                let sorted_tids = lookup_index_tids(keys, index, storage);
                let matching_pages = storage
                    .lock()
                    .unwrap()
                    .table(table.id)
                    .bitmap_scan(&sorted_tids);
                let filtered_pages =
                    filter_pages_using_tids(projects, &sorted_tids, matching_pages);
                // Combine the filtered pages.
                let query_names = projects
                    .iter()
                    .map(|c| (c.name.clone(), c.canonical_name()))
                    .collect();
                let mut output = match RecordBatch::cat(filtered_pages) {
                    Some(batch) => batch,
                    None => return Next::End,
                };
                output = output.rename(&query_names);
                // If requested, retain the right side of the join.
                if *include_existing {
                    // TODO this assumes the join is 1-to-1, which is not always the case.
                    output = RecordBatch::zip(output, input);
                }
                // Apply remaining predicates.
                if !predicates.is_empty() {
                    let boolean = crate::eval::all(predicates, &output, storage, txn)?;
                    output = output.compress(&boolean);
                }
                Next::Page(output)
            }
            Node::Filter { predicates, input } => {
                let input = input.next(storage, txn)?;
                let boolean = crate::eval::all(predicates, &input, storage, txn)?;
                Next::Page(input.compress(&boolean))
            }
            Node::Out { projects, input } => {
                let input = input.next(storage, txn)?;
                let mut columns = vec![];
                for column in projects {
                    columns.push((
                        column.name.clone(),
                        input.find_always(&column.canonical_name()).clone(),
                    ));
                }
                Next::Page(RecordBatch::new(columns))
            }
            Node::Map {
                include_existing,
                projects,
                input,
            } => {
                let input = input.next(storage, txn)?;
                let mut columns = vec![];
                if *include_existing {
                    columns.extend(input.columns.clone());
                }
                for (scalar, column) in projects {
                    columns.push((
                        column.canonical_name(),
                        crate::eval::eval(scalar, &input, storage, txn)?,
                    ));
                }
                Next::Page(RecordBatch::new(columns))
            }
            Node::NestedLoop {
                join: Join::Outer(predicates),
                left,
                left_schema,
                build_left,
                unmatched_left,
                right,
                right_schema,
            } => {
                // If this is the first iteration, build the left side of the join into a single batch.
                if build_left.is_none() {
                    let input = build_or_empty(left, storage, txn, left_schema)?;
                    let bits = BoolArray::trues(input.len());
                    *build_left = Some(input);
                    // Allocate a bit array to keep track of which rows on the left side never found join partners.
                    *unmatched_left = Some(bits);
                }
                match right.next(storage, txn) {
                    // If the right side has more rows, perform a right outer join on those rows, keeping track of unmatched left rows in the bit array.
                    Next::Page(right) => {
                        let filter =
                            |input: &RecordBatch| crate::eval::all(predicates, input, storage, txn);
                        let next = crate::join::nested_loop(
                            build_left.as_ref().unwrap(),
                            &right,
                            filter,
                            unmatched_left.as_mut(),
                            true,
                        )?;
                        Next::Page(next)
                    }
                    Next::End => match unmatched_left.take() {
                        // The first time we receive 'Empty' from the right side, consume unmatched_left and release the unmatched left side rows.
                        Some(unmatched_left) => {
                            let next = crate::join::unmatched_tuples(
                                build_left.as_ref().unwrap(),
                                &unmatched_left,
                                &right_schema,
                            )?;
                            Next::Page(next)
                        }
                        // The second time we receive 'Empty' from the right side, we are truly finished.
                        None => Next::End,
                    },
                    Next::Error(message) => Next::Error(message),
                }
            }
            Node::NestedLoop {
                join,
                left,
                left_schema,
                build_left,
                right,
                ..
            } => {
                // If this is the first iteration, build the left side of the join into a single batch.
                if build_left.is_none() {
                    let input = build_or_empty(left, storage, txn, left_schema)?;
                    *build_left = Some(input);
                }
                // Get the next batch of rows from the right (probe) side.
                let right = right.next(storage, txn)?;
                let filter =
                    |input: &RecordBatch| crate::eval::all(join.predicates(), input, storage, txn);
                // Join a batch of rows to the left (build) side.
                let next = match &join {
                    Join::Inner(_) => crate::join::nested_loop(
                        build_left.as_ref().unwrap(),
                        &right,
                        filter,
                        None,
                        false,
                    ),
                    Join::Right(_) => crate::join::nested_loop(
                        build_left.as_ref().unwrap(),
                        &right,
                        filter,
                        None,
                        true,
                    ),
                    Join::Outer(_) => panic!("outer joins are handled separately"),
                    Join::Semi(_) => {
                        crate::join::nested_loop_semi(build_left.as_ref().unwrap(), &right, filter)
                    }
                    Join::Anti(_) => {
                        crate::join::nested_loop_anti(build_left.as_ref().unwrap(), &right, filter)
                    }
                    Join::Single(_) => crate::join::nested_loop_single(
                        build_left.as_ref().unwrap(),
                        &right,
                        filter,
                    ),
                    Join::Mark(mark, _) => crate::join::nested_loop_mark(
                        mark,
                        build_left.as_ref().unwrap(),
                        &right,
                        filter,
                    ),
                };
                Next::Page(next?)
            }
            Node::HashJoin {
                join: Join::Outer(predicates),
                partition_left,
                partition_right,
                left,
                left_schema,
                build_left,
                unmatched_left,
                right,
                right_schema,
            } => {
                // If this is the first iteration, build the left side of the join into a hash table.
                if build_left.is_none() {
                    let left = build_or_empty(left, storage, txn, left_schema)?;
                    let partition_left = match left.find_always(&partition_left.canonical_name()) {
                        AnyArray::I64(a) => a,
                        _ => panic!(),
                    };
                    let table = HashTable::new(&left, partition_left);
                    *build_left = Some(table);
                    // Allocate a bit array to keep track of which rows on the left side never found join partners.
                    *unmatched_left = Some(BoolArray::trues(left.len()));
                }
                match right.next(storage, txn) {
                    // If the right side has more rows, perform a right outer join on those rows, keeping track of unmatched left rows in the bit array.
                    Next::Page(right) => {
                        let partition_right =
                            match right.find_always(&partition_right.canonical_name()) {
                                AnyArray::I64(a) => a,
                                _ => panic!(),
                            };
                        let filter =
                            |input: &RecordBatch| crate::eval::all(predicates, input, storage, txn);
                        let next = crate::join::hash_join(
                            build_left.as_ref().unwrap(),
                            &right,
                            &partition_right,
                            filter,
                            Some(unmatched_left.as_mut().unwrap()),
                            true,
                        )?;
                        Next::Page(next)
                    }
                    Next::End => match unmatched_left.take() {
                        // The first time we receive 'Empty' from the right side, consume unmatched_left and release the unmatched left side rows.
                        Some(unmatched_left) => {
                            let next = crate::join::unmatched_tuples(
                                build_left.as_ref().unwrap().build(),
                                &unmatched_left,
                                &right_schema,
                            )?;
                            Next::Page(next)
                        }
                        // The second time we receive 'Empty' from the right side, we are truly finished.
                        None => Next::End,
                    },
                    Next::Error(message) => Next::Error(message),
                }
            }
            Node::HashJoin {
                join,
                partition_left,
                partition_right,
                left,
                left_schema,
                build_left,
                right,
                ..
            } => {
                // If this is the first iteration, build the left side of the join into a hash table.
                if build_left.is_none() {
                    let left = build_or_empty(left, storage, txn, left_schema)?;
                    let partition_left = match left.find_always(&partition_left.canonical_name()) {
                        AnyArray::I64(a) => a,
                        _ => panic!(),
                    };
                    let table = HashTable::new(&left, &partition_left);
                    *build_left = Some(table);
                }
                // Get the next batch of rows from the right (probe) side.
                let right = right.next(storage, txn)?;
                let partition_right = match right.find_always(&partition_right.canonical_name()) {
                    AnyArray::I64(a) => a,
                    _ => panic!(),
                };
                let filter =
                    |input: &RecordBatch| crate::eval::all(join.predicates(), input, storage, txn);
                // Join a batch of rows to the left (build) side.
                let next = match &join {
                    Join::Inner(_) => crate::join::hash_join(
                        build_left.as_ref().unwrap(),
                        &right,
                        &partition_right,
                        filter,
                        None,
                        false,
                    ),
                    Join::Right(_) => crate::join::hash_join(
                        build_left.as_ref().unwrap(),
                        &right,
                        &partition_right,
                        filter,
                        None,
                        true,
                    ),
                    Join::Outer(_) => panic!("outer joins are handled separately"),
                    Join::Semi(_) => crate::join::hash_join_semi(
                        build_left.as_ref().unwrap(),
                        &right,
                        &partition_right,
                        filter,
                    ),
                    Join::Anti(_) => crate::join::hash_join_anti(
                        build_left.as_ref().unwrap(),
                        &right,
                        &partition_right,
                        filter,
                    ),
                    Join::Single(_) => crate::join::hash_join_single(
                        build_left.as_ref().unwrap(),
                        &right,
                        &partition_right,
                        filter,
                    ),
                    Join::Mark(mark, _) => crate::join::hash_join_mark(
                        mark,
                        build_left.as_ref().unwrap(),
                        &right,
                        &partition_right,
                        filter,
                    ),
                };
                Next::Page(next?)
            }
            Node::CreateTempTable {
                name,
                columns,
                input,
            } => {
                // Create a new temp table.
                let mut heap = Heap::default();
                // Temp table uses different column names.
                let column_names = columns.iter().map(|c| c.canonical_name()).collect();
                loop {
                    match input.next(storage, txn) {
                        Next::Page(batch) => {
                            heap.insert(&batch.with_names(&column_names), txn);
                        }
                        Next::Error(message) => return Next::Error(message),
                        Next::End => break,
                    }
                }
                // Store the temp table.
                // TODO delete this table when the transaction completes.
                storage
                    .lock()
                    .unwrap()
                    .create_temp_table(txn, name.clone(), heap);
                Next::End
            }
            Node::GetTempTable {
                name,
                columns,
                scan,
            } => {
                if scan.is_none() {
                    *scan = Some(storage.lock().unwrap().temp_table(txn, name.clone()).scan());
                }
                let page = match scan.as_mut().unwrap().pop() {
                    Some(page) => page,
                    None => return Next::End,
                };
                let column_names = columns.iter().map(|c| c.canonical_name()).collect();
                let next = page.with_names(&column_names);
                Next::Page(next)
            }
            Node::SimpleAggregate {
                finished,
                aggregate,
                input,
            } => {
                if globals::WORKER.get() != leader(txn) {
                    return Next::End;
                }
                if *finished {
                    return Next::End;
                } else {
                    *finished = true;
                }
                let mut operator = crate::aggregate::SimpleAggregate::new(aggregate);
                loop {
                    match input.next(storage, txn) {
                        Next::Page(batch) => {
                            let aggregate_columns: Vec<AnyArray> = aggregate
                                .iter()
                                .map(|a| batch.find_always(&a.input.canonical_name()).clone())
                                .collect();
                            operator.insert(aggregate_columns);
                        }
                        Next::End => {
                            let mut names = vec![];
                            for e in aggregate {
                                names.push(e.output.canonical_name());
                            }
                            let columns = operator
                                .finish()
                                .drain(..)
                                .enumerate()
                                .map(|(i, array)| (std::mem::take(&mut names[i]), array))
                                .collect();
                            return Next::Page(RecordBatch::new(columns));
                        }
                        Next::Error(message) => return Next::Error(message),
                    }
                }
            }
            Node::GroupByAggregate {
                finished,
                group_by,
                aggregate,
                input,
            } => {
                if *finished {
                    return Next::End;
                } else {
                    *finished = true;
                }
                let mut operator = crate::aggregate::GroupByAggregate::new(aggregate);
                loop {
                    match input.next(storage, txn) {
                        Next::Page(batch) => {
                            let group_by_columns: Vec<AnyArray> = group_by
                                .iter()
                                .map(|c| batch.find_always(&c.canonical_name()).clone())
                                .collect();
                            let aggregate_columns: Vec<AnyArray> = aggregate
                                .iter()
                                .map(|a| batch.find_always(&a.input.canonical_name()).clone())
                                .collect();
                            operator.insert(group_by_columns, aggregate_columns);
                        }
                        Next::Error(message) => return Next::Error(message),
                        Next::End => {
                            let mut names = vec![];
                            for c in group_by {
                                names.push(c.canonical_name());
                            }
                            for e in aggregate {
                                names.push(e.output.canonical_name());
                            }
                            let columns = operator
                                .finish()
                                .drain(..)
                                .enumerate()
                                .map(|(i, array)| (std::mem::take(&mut names[i]), array))
                                .collect();
                            return Next::Page(RecordBatch::new(columns));
                        }
                    }
                }
            }
            Node::Limit {
                cursor,
                limit,
                offset,
                input,
            } => {
                if *cursor == *limit {
                    return Next::End;
                }
                let input = input.next(storage, txn)?;
                let mut start_inclusive = 0;
                while start_inclusive < input.len() && cursor < offset {
                    start_inclusive += 1;
                    *cursor += 1;
                }
                let mut end_exclusive = start_inclusive;
                while end_exclusive <= input.len() && *cursor < *offset + *limit {
                    end_exclusive += 1;
                    *cursor += 1;
                }
                Next::Page(input.slice(start_inclusive..end_exclusive))
            }
            Node::Sort { order_by, input } => {
                let input = build(input, storage, txn)?;
                let desc = order_by.iter().map(|o| o.descending).collect();
                let columns = order_by
                    .iter()
                    .map(|o| {
                        (
                            o.column.canonical_name(),
                            input.find_always(&o.column.canonical_name()).clone(),
                        )
                    })
                    .collect();
                let indexes = RecordBatch::new(columns).sort(desc);
                let output = input.gather(&indexes);
                Next::Page(output)
            }
            Node::Union { left, right } => match left.next(storage, txn) {
                Next::Page(batch) => Next::Page(batch),
                Next::Error(message) => Next::Error(message),
                Next::End => right.next(storage, txn),
            },
            Node::Broadcast {
                input,
                stream,
                stage,
            } => {
                if let Some(expr) = input.take() {
                    *stream = Some(RemoteQuery::new(remote_execution::broadcast(
                        &expr, txn, *stage,
                    )));
                }
                stream.as_mut().unwrap().inner.next()
            }
            Node::Exchange {
                input,
                stream,
                stage,
            } => {
                if let Some((hash_column, expr)) = input.take() {
                    *stream = Some(RemoteQuery::new(remote_execution::exchange(
                        &expr,
                        txn,
                        *stage,
                        hash_column.canonical_name(),
                        globals::WORKER.get(),
                    )));
                }
                stream.as_mut().unwrap().inner.next()
            }
            Node::Gather {
                stage,
                input,
                stream,
            } => {
                if globals::WORKER.get() != leader(txn) {
                    return Next::End;
                }
                if let Some(expr) = input.take() {
                    *stream = Some(RemoteQuery::new(remote_execution::gather(
                        &expr, txn, *stage,
                    )));
                }
                stream.as_mut().unwrap().inner.next()
            }
            Node::Insert {
                finished,
                table,
                indexes,
                input,
                columns,
            } => {
                if *finished {
                    return Next::End;
                } else {
                    *finished = true;
                }
                loop {
                    let input = match input.next(storage, txn) {
                        Next::Page(batch) => batch,
                        Next::Error(message) => return Next::Error(message),
                        Next::End => break,
                    };
                    // Rename columns from query to match table.
                    let renames = columns
                        .iter()
                        .map(|(from, to)| (from.canonical_name(), to.clone()))
                        .collect();
                    let input = input.rename(&renames);
                    // Append rows to the table heap.
                    let txn = txn;
                    let tids = storage
                        .lock()
                        .unwrap()
                        .table_mut(table.id)
                        .insert(&input, txn);
                    // Update indexes.
                    for index in indexes.iter_mut() {
                        crate::index::insert(
                            storage.lock().unwrap().index_mut(index.index_id),
                            &index.columns,
                            &input,
                            &tids,
                        );
                    }
                }
                // Insert returns no values.
                Next::End
            }
            Node::Values {
                columns,
                values,
                input,
            } => {
                let input = input.next(storage, txn)?;
                let mut output = vec![];
                for i in 0..columns.len() {
                    let mut builder = vec![];
                    for value in &values[i] {
                        let value = crate::eval::eval(value, &input, storage, txn)?;
                        if value.len() != 1 {
                            panic!("input to values produced {} rows", value.len());
                        }
                        builder.push(value)
                    }
                    output.push((columns[i].canonical_name(), AnyArray::cat(builder)));
                }
                Next::Page(RecordBatch::new(output))
            }
            Node::Delete { table, tid, input } => {
                let input = input.next(storage, txn)?;
                // If no input, try next page.
                if input.len() == 0 {
                    return self.next(storage, txn);
                }
                // Identify rows to be updated by tid and sort them.
                let tids = match input.find_always(&tid.canonical_name()) {
                    AnyArray::I64(tids) => tids,
                    _ => panic!(),
                };
                let tids = tids.gather(&tids.sort());
                // Invalidate the old row versions.
                let storage = storage.lock().unwrap();
                let heap = storage.table(table.id);
                let mut i = 0;
                while i < tids.len() {
                    let pid = tids.get(0).unwrap() as usize / storage::PAGE_SIZE;
                    let page = heap.page(pid);
                    while i < tids.len()
                        && pid == tids.get(i).unwrap() as usize / storage::PAGE_SIZE
                    {
                        let rid = tids.get(i).unwrap() as usize % storage::PAGE_SIZE;
                        page.delete(rid, txn);
                        i += 1;
                    }
                }
                Next::Page(input)
            }
            Node::Script { offset, stmts } => {
                while *offset < stmts.len() {
                    match stmts[*offset].next(storage, txn) {
                        Next::Page(batch) => {
                            if *offset == stmts.len() - 1 {
                                return Next::Page(batch);
                            }
                        }
                        Next::End => {
                            *offset += 1;
                        }
                        Next::Error(message) => return Next::Error(message),
                    }
                }
                Next::End
            }
            Node::Call { procedure, input } => {
                let input = input.next(storage, txn)?;
                match procedure {
                    Procedure::CreateCatalog => {
                        storage.lock().unwrap().create_catalog();
                    }
                    Procedure::CreateTable => {
                        storage.lock().unwrap().create_table();
                    }
                    Procedure::CreateIndex => {
                        storage.lock().unwrap().create_index();
                    }
                    Procedure::Assert(test, description) => {
                        let test = crate::eval::eval(test, &input, storage, txn)?
                            .as_bool()
                            .get(0)
                            .unwrap_or(false);
                        if !test {
                            return Next::Error(description.clone());
                        }
                    }
                };
                Next::End
            }
            Node::Explain { finished, input } => {
                if *finished || globals::WORKER.get() != 0 {
                    return Next::End;
                }
                *finished = true;
                let batch = RecordBatch::new(vec![(
                    "plan".to_string(),
                    AnyArray::String(StringArray::from_values(vec![input.to_string()])),
                )]);
                Next::Page(batch)
            }
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Node::TableFreeScan { .. } => "TableFreeScan",
            Node::SeqScan { .. } => "SeqScan",
            Node::IndexScan { .. } => "IndexScan",
            Node::Filter { .. } => "Filter",
            Node::Out { .. } => "Out",
            Node::Map { .. } => "Map",
            Node::NestedLoop { .. } => "NestedLoop",
            Node::HashJoin { .. } => "HashJoin",
            Node::CreateTempTable { .. } => "CreateTempTable",
            Node::GetTempTable { .. } => "GetTempTable",
            Node::SimpleAggregate { .. } => "SimpleAggregate",
            Node::GroupByAggregate { .. } => "GroupByAggregate",
            Node::Limit { .. } => "Limit",
            Node::Sort { .. } => "Sort",
            Node::Union { .. } => "Union",
            Node::Broadcast { .. } => "Broadcast",
            Node::Exchange { .. } => "Exchange",
            Node::Gather { .. } => "Gather",
            Node::Insert { .. } => "Insert",
            Node::Values { .. } => "Values",
            Node::Delete { .. } => "Delete",
            Node::Script { .. } => "Script",
            Node::Call { .. } => "Call",
            Node::Explain { .. } => "Explain",
        }
    }
}

impl RemoteQuery {
    fn new(inner: RecordStream) -> Self {
        Self { inner }
    }
}

impl Debug for RemoteQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<stream>")
    }
}

fn dummy_row() -> RecordBatch {
    RecordBatch::new(vec![(
        "$dummy".to_string(),
        AnyArray::Bool(BoolArray::from_values(vec![false])),
    )])
}

fn build_or_empty(
    left: &mut Node,
    storage: &Mutex<Storage>,
    txn: i64,
    left_schema: &Vec<(String, DataType)>,
) -> Result<RecordBatch, String> {
    match build(left, storage, txn) {
        Next::Page(batch) => Ok(batch),
        Next::Error(message) => Err(message),
        Next::End => Ok(RecordBatch::empty(left_schema.clone())),
    }
}

fn build(input: &mut Node, storage: &Mutex<Storage>, txn: i64) -> Next {
    let mut batches = vec![];
    loop {
        match input.next(storage, txn) {
            Next::Page(batch) => batches.push(batch),
            Next::Error(message) => return Next::Error(message),
            Next::End => {
                if let Some(batch) = RecordBatch::cat(batches) {
                    return Next::Page(batch);
                } else {
                    return Next::End;
                }
            }
        }
    }
}

fn evaluate_index_keys(
    lookup: &Vec<Scalar>,
    input: &RecordBatch,
    storage: &Mutex<Storage>,
    txn: i64,
) -> Result<PackedBytes, String> {
    let columns: Result<Vec<_>, _> = lookup
        .iter()
        .map(|scalar| crate::eval::eval(scalar, &input, storage, txn))
        .collect();
    let keys = crate::index::byte_key_prefix(columns?.iter().map(|c| c).collect());
    Ok(keys)
}

fn lookup_index_tids(keys: PackedBytes, index: &Index, storage: &Mutex<Storage>) -> Vec<i64> {
    let storage = storage.lock().unwrap();
    let mut tids = vec![];
    let art = storage.index(index.index_id);
    for i in 0..keys.len() {
        let start = keys.get(i);
        let end = crate::index::upper_bound(start);
        let next = art.range(start..end.as_slice());
        tids.extend(next);
    }
    tids.sort();
    tids
}

fn filter_pages_using_tids(
    projects: &Vec<Column>,
    sorted_tids: &Vec<i64>,
    matching_pages: Vec<Arc<Page>>,
) -> Vec<RecordBatch> {
    let select_names = projects.iter().map(|c| c.name.clone()).collect();
    let mut i = 0;
    let mut j = 0;
    let mut filtered_pages = vec![];
    while i < sorted_tids.len() && j < matching_pages.len() {
        let pid = sorted_tids[i] as usize / PAGE_SIZE;
        if pid < j {
            // Go to the next tid.
            i += 1
        } else if j < pid {
            // Go to the next page.
            j += 1
        } else {
            // Filter the current page.
            let rids = rids(&sorted_tids[i..], pid);
            let page = matching_pages[j].select(&select_names).gather(&rids);
            filtered_pages.push(page);
            // Go to the next page.
            j += 1
        }
    }
    filtered_pages
}

/// Returns a slice of the first n tids that have page-id pid.
fn rids(tids: &[i64], pid: usize) -> I32Array {
    let mut rids = I32Array::default();
    for tid in tids {
        if *tid as usize / PAGE_SIZE > pid {
            break;
        }
        let rid = *tid as usize % PAGE_SIZE;
        rids.push(Some(rid as i32));
    }
    rids
}

/// We select a single worker as the "leader" for each transaction in a round-robin manner.
/// The leader is responsible for executing operations that take place on a single node.
fn leader(txn: i64) -> i32 {
    let n: i64 = std::env::var("WORKER_COUNT").unwrap().parse().unwrap();
    (txn % n) as i32
}
