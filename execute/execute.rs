use std::{collections::HashMap, fmt::Debug, sync::Arc};

use ast::{Expr, Index, *};
use context::{Context, WORKER_ID_KEY};
use kernel::{RecordBatch, *};
use remote_execution::{RecordStream, REMOTE_EXECUTION_KEY};
use storage::*;

use crate::hash_table::HashTable;

pub fn execute<'a>(
    expr: Expr,
    txn: i64,
    variables: &HashMap<String, AnyArray>,
    context: &'a Context,
) -> RunningQuery<'a> {
    RunningQuery {
        input: Node::compile(expr, txn, context),
        state: QueryState {
            txn,
            variables: variables.clone(),
            context,
            temp_tables: Storage::default(),
            temp_table_ids: HashMap::new(),
        },
    }
}

pub struct RunningQuery<'a> {
    state: QueryState<'a>,
    input: Node,
}

pub(crate) struct QueryState<'a> {
    pub txn: i64,
    pub variables: HashMap<String, AnyArray>,
    pub context: &'a Context,
    pub temp_tables: Storage,
    pub temp_table_ids: HashMap<String, i64>,
}

#[derive(Debug)]
enum Node {
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
    Aggregate {
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
        input: Option<Expr>,
        stream: Option<RemoteQuery>,
    },
    Exchange {
        input: Option<(Column, Expr)>,
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
        statements: Vec<Node>,
    },
    Assign {
        variable: String,
        value: Scalar,
        input: Box<Node>,
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

struct RemoteQuery {
    inner: RecordStream,
}

impl<'a> Iterator for RunningQuery<'a> {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        self.input.next(&mut self.state)
    }
}

impl Node {
    fn compile(expr: Expr, txn: i64, context: &Context) -> Self {
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
                input: Box::new(Node::compile(*input, txn, context)),
            },
            Filter { predicates, input } => Node::Filter {
                predicates,
                input: Box::new(Node::compile(*input, txn, context)),
            },
            Out { projects, input } => Node::Out {
                projects,
                input: Box::new(Node::compile(*input, txn, context)),
            },
            Map {
                include_existing,
                projects,
                input,
            } => Node::Map {
                include_existing,
                projects,
                input: Box::new(Node::compile(*input, txn, context)),
            },
            NestedLoop { join, left, right } => {
                let right_schema = schema(right.as_ref());
                Node::NestedLoop {
                    join,
                    left: Box::new(Node::compile(*left, txn, context)),
                    build_left: None,
                    unmatched_left: None,
                    right: Box::new(Node::compile(*right, txn, context)),
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
                let right_schema = schema(right.as_ref());
                let left = Box::new(Node::compile(*left, txn, context));
                let right = Box::new(Node::compile(*right, txn, context));
                Node::HashJoin {
                    join,
                    partition_left,
                    partition_right,
                    left,
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
                input: Box::new(Node::compile(*input, txn, context)),
            },
            GetTempTable { name, columns } => Node::GetTempTable {
                name,
                columns,
                scan: None,
            },
            Aggregate {
                group_by,
                aggregate,
                input,
                ..
            } => Node::Aggregate {
                finished: false,
                group_by,
                aggregate,
                input: Box::new(Node::compile(*input, txn, context)),
            },
            Limit {
                limit,
                offset,
                input,
            } => Node::Limit {
                cursor: 0,
                limit,
                offset,
                input: Box::new(Node::compile(*input, txn, context)),
            },
            Sort { order_by, input } => Node::Sort {
                order_by,
                input: Box::new(Node::compile(*input, txn, context)),
            },
            Union { left, right } => Node::Union {
                left: Box::new(Node::compile(*left, txn, context)),
                right: Box::new(Node::compile(*right, txn, context)),
            },
            Broadcast { input } => Node::Broadcast {
                input: Some(*input),
                stream: None,
            },
            Exchange { hash_column, input } => Node::Exchange {
                input: Some((hash_column.unwrap(), *input)),
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
                input: Box::new(Node::compile(*input, txn, context)),
                columns,
            },
            Values {
                columns,
                values,
                input,
            } => Node::Values {
                columns,
                values,
                input: Box::new(Node::compile(*input, txn, context)),
            },
            Delete { table, tid, input } => Node::Delete {
                table,
                tid,
                input: Box::new(Node::compile(*input, txn, context)),
            },
            Script { statements } => {
                let mut compiled = vec![];
                for expr in statements {
                    compiled.push(Node::compile(expr, txn, context))
                }
                Node::Script {
                    offset: 0,
                    statements: compiled,
                }
            }
            Assign {
                variable,
                value,
                input,
            } => Node::Assign {
                variable,
                value,
                input: Box::new(Node::compile(*input, txn, context)),
            },
            Call { procedure, input } => Node::Call {
                procedure,
                input: Box::new(Node::compile(*input, txn, context)),
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
            | LogicalAssign { .. }
            | LogicalCall { .. }
            | LogicalExplain { .. } => panic!("logical operation"),
        }
    }
}

impl Node {
    fn next(&mut self, state: &mut QueryState) -> Option<RecordBatch> {
        match self {
            Node::TableFreeScan { empty } => {
                if *empty {
                    return None;
                }
                *empty = true;
                Some(dummy_row())
            }
            Node::SeqScan {
                projects,
                predicates,
                table,
                scan,
            } => {
                if scan.is_none() {
                    *scan = Some(
                        state.context[STORAGE_KEY]
                            .lock()
                            .unwrap()
                            .table(table.id)
                            .scan(),
                    );
                }
                let page = scan.as_mut().unwrap().pop()?;
                let select_names = projects.iter().map(|c| c.name.clone()).collect();
                let query_names = projects
                    .iter()
                    .map(|c| (c.name.clone(), c.canonical_name()))
                    .collect();
                let input = page.select(&select_names).rename(&query_names);
                let boolean = crate::eval::all(predicates, &input, state);
                Some(input.compress(&boolean))
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
                // Evaluate lookup scalars.
                let input = input.next(state)?;
                let columns: Vec<AnyArray> = lookup
                    .iter()
                    .map(|scalar| crate::eval::eval(scalar, &input, state))
                    .collect();
                let keys = crate::index::byte_key_prefix(columns.iter().map(|c| c).collect());
                // Look up scalars in the index.
                let sorted_tids = {
                    let storage = state.context[STORAGE_KEY].lock().unwrap();
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
                };
                // Select pages that contain tids.
                let matching_pages = state.context[STORAGE_KEY]
                    .lock()
                    .unwrap()
                    .table(table.id)
                    .bitmap_scan(&sorted_tids);
                /// Returns a slice of the first n tids that have page-id pid.
                fn rids(tids: &[i64], pid: usize) -> I32Array {
                    let mut rids = I32Array::new();
                    for tid in tids {
                        if *tid as usize / PAGE_SIZE > pid {
                            break;
                        }
                        let rid = *tid as usize % PAGE_SIZE;
                        rids.push(Some(rid as i32));
                    }
                    rids
                }
                // Perform a bitmap scan on each page.
                let select_names = projects.iter().map(|c| c.name.clone()).collect();
                let query_names = projects
                    .iter()
                    .map(|c| (c.name.clone(), c.canonical_name()))
                    .collect();
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
                // Combine the filtered pages.
                let mut output = RecordBatch::cat(filtered_pages)?.rename(&query_names);
                if *include_existing {
                    output = RecordBatch::zip(output, input);
                }
                let boolean = crate::eval::all(predicates, &output, state);
                Some(output.compress(&boolean))
            }
            Node::Filter { predicates, input } => {
                let input = input.next(state)?;
                let boolean = crate::eval::all(predicates, &input, state);
                Some(input.compress(&boolean))
            }
            Node::Out { projects, input } => {
                let input = input.next(state)?;
                let mut columns = vec![];
                for column in projects {
                    columns.push((
                        column.name.clone(),
                        input.find(&column.canonical_name()).unwrap().clone(),
                    ));
                }
                Some(RecordBatch::new(columns))
            }
            Node::Map {
                include_existing,
                projects,
                input,
            } => {
                let input = input.next(state)?;
                let mut columns = vec![];
                if *include_existing {
                    columns.extend(input.columns.clone());
                }
                for (scalar, column) in projects {
                    columns.push((
                        column.canonical_name(),
                        crate::eval::eval(scalar, &input, state),
                    ));
                }
                Some(RecordBatch::new(columns))
            }
            Node::NestedLoop {
                join: Join::Outer(predicates),
                left,
                build_left,
                unmatched_left,
                right,
                right_schema,
            } => {
                // If this is the first iteration, build the left side of the join into a single batch.
                if build_left.is_none() {
                    let input = build(left, state)?;
                    let bits = BoolArray::trues(input.len());
                    *build_left = Some(input);
                    // Allocate a bit array to keep track of which rows on the left side never found join partners.
                    *unmatched_left = Some(bits);
                }
                match right.next(state) {
                    // If the right side has more rows, perform a right outer join on those rows, keeping track of unmatched left rows in the bit array.
                    Some(right) => {
                        let filter =
                            |input: &RecordBatch| crate::eval::all(predicates, input, state);
                        Some(crate::join::nested_loop(
                            build_left.as_ref().unwrap(),
                            &right,
                            filter,
                            unmatched_left.as_mut(),
                            true,
                        ))
                    }
                    None => match unmatched_left.take() {
                        // The first time we receive 'Empty' from the right side, consume unmatched_left and release the unmatched left side rows.
                        Some(unmatched_left) => Some(crate::join::unmatched_tuples(
                            build_left.as_ref().unwrap(),
                            &unmatched_left,
                            &right_schema,
                        )),
                        // The second time we receive 'Empty' from the right side, we are truly finished.
                        None => None,
                    },
                }
            }
            Node::NestedLoop {
                join,
                left,
                build_left,
                right,
                ..
            } => {
                // If this is the first iteration, build the left side of the join into a single batch.
                if build_left.is_none() {
                    let input = build(left, state)?;
                    *build_left = Some(input);
                }
                // Get the next batch of rows from the right (probe) side.
                let right = right.next(state)?;
                let filter =
                    |input: &RecordBatch| crate::eval::all(join.predicates(), input, state);
                // Join a batch of rows to the left (build) side.
                match &join {
                    Join::Inner(_) => Some(crate::join::nested_loop(
                        build_left.as_ref().unwrap(),
                        &right,
                        filter,
                        None,
                        false,
                    )),
                    Join::Right(_) => Some(crate::join::nested_loop(
                        build_left.as_ref().unwrap(),
                        &right,
                        filter,
                        None,
                        true,
                    )),
                    Join::Outer(_) => panic!("outer joins are handled separately"),
                    Join::Semi(_) => Some(crate::join::nested_loop_semi(
                        build_left.as_ref().unwrap(),
                        &right,
                        filter,
                    )),
                    Join::Anti(_) => Some(crate::join::nested_loop_anti(
                        build_left.as_ref().unwrap(),
                        &right,
                        filter,
                    )),
                    Join::Single(_) => Some(crate::join::nested_loop_single(
                        build_left.as_ref().unwrap(),
                        &right,
                        filter,
                    )),
                    Join::Mark(mark, _) => Some(crate::join::nested_loop_mark(
                        mark,
                        build_left.as_ref().unwrap(),
                        &right,
                        filter,
                    )),
                }
            }
            Node::HashJoin {
                join: Join::Outer(predicates),
                partition_left,
                partition_right,
                left,
                build_left,
                unmatched_left,
                right,
                right_schema,
            } => {
                // If this is the first iteration, build the left side of the join into a hash table.
                if build_left.is_none() {
                    let left = build(left, state)?;
                    let partition_left = match left.find(&partition_left.canonical_name()).unwrap()
                    {
                        AnyArray::I64(a) => a,
                        _ => panic!(),
                    };
                    let table = HashTable::new(&left, partition_left);
                    *build_left = Some(table);
                    // Allocate a bit array to keep track of which rows on the left side never found join partners.
                    *unmatched_left = Some(BoolArray::trues(left.len()));
                }
                match right.next(state) {
                    // If the right side has more rows, perform a right outer join on those rows, keeping track of unmatched left rows in the bit array.
                    Some(right) => {
                        let partition_right =
                            match right.find(&partition_right.canonical_name()).unwrap() {
                                AnyArray::I64(a) => a,
                                _ => panic!(),
                            };
                        let filter =
                            |input: &RecordBatch| crate::eval::all(predicates, input, state);
                        Some(crate::join::hash_join(
                            build_left.as_ref().unwrap(),
                            &right,
                            &partition_right,
                            filter,
                            Some(unmatched_left.as_mut().unwrap()),
                            true,
                        ))
                    }
                    None => match unmatched_left.take() {
                        // The first time we receive 'Empty' from the right side, consume unmatched_left and release the unmatched left side rows.
                        Some(unmatched_left) => Some(crate::join::unmatched_tuples(
                            build_left.as_ref().unwrap().build(),
                            &unmatched_left,
                            &right_schema,
                        )),
                        // The second time we receive 'Empty' from the right side, we are truly finished.
                        None => None,
                    },
                }
            }
            Node::HashJoin {
                join,
                partition_left,
                partition_right,
                left,
                build_left,
                right,
                ..
            } => {
                // If this is the first iteration, build the left side of the join into a hash table.
                if build_left.is_none() {
                    let left = build(left, state)?;
                    let partition_left = match left.find(&partition_left.canonical_name()).unwrap()
                    {
                        AnyArray::I64(a) => a,
                        _ => panic!(),
                    };
                    let table = HashTable::new(&left, &partition_left);
                    *build_left = Some(table);
                }
                // Get the next batch of rows from the right (probe) side.
                let right = right.next(state)?;
                let partition_right = match right.find(&partition_right.canonical_name()).unwrap() {
                    AnyArray::I64(a) => a,
                    _ => panic!(),
                };
                let filter =
                    |input: &RecordBatch| crate::eval::all(join.predicates(), input, state);
                // Join a batch of rows to the left (build) side.
                match &join {
                    Join::Inner(_) => Some(crate::join::hash_join(
                        build_left.as_ref().unwrap(),
                        &right,
                        &partition_right,
                        filter,
                        None,
                        false,
                    )),
                    Join::Right(_) => Some(crate::join::hash_join(
                        build_left.as_ref().unwrap(),
                        &right,
                        &partition_right,
                        filter,
                        None,
                        true,
                    )),
                    Join::Outer(_) => panic!("outer joins are handled separately"),
                    Join::Semi(_) => Some(crate::join::hash_join_semi(
                        build_left.as_ref().unwrap(),
                        &right,
                        &partition_right,
                        filter,
                    )),
                    Join::Anti(_) => Some(crate::join::hash_join_anti(
                        build_left.as_ref().unwrap(),
                        &right,
                        &partition_right,
                        filter,
                    )),
                    Join::Single(_) => Some(crate::join::hash_join_single(
                        build_left.as_ref().unwrap(),
                        &right,
                        &partition_right,
                        filter,
                    )),
                    Join::Mark(mark, _) => Some(crate::join::hash_join_mark(
                        mark,
                        build_left.as_ref().unwrap(),
                        &right,
                        &partition_right,
                        filter,
                    )),
                }
            }
            Node::CreateTempTable {
                name,
                columns,
                input,
            } => {
                // Register a new temp table.
                let table_id = 100 + state.temp_table_ids.len() as i64;
                state.temp_table_ids.insert(name.clone(), table_id);
                state.temp_tables.create_table(table_id);
                // Temp table uses different column names.
                let renames = columns
                    .iter()
                    .map(|c| (c.canonical_name(), c.name.clone()))
                    .collect();
                // Insert entire input into the temp table.
                loop {
                    let batch = input.next(state)?.rename(&renames);
                    let heap = state.temp_tables.table_mut(table_id);
                    heap.insert(&batch, state.txn);
                }
            }
            Node::GetTempTable {
                name,
                columns,
                scan,
            } => {
                if scan.is_none() {
                    *scan = Some(state.temp_tables.table(state.temp_table_ids[name]).scan())
                }
                match scan.as_mut().unwrap().pop() {
                    Some(page) => {
                        let select_names = columns.iter().map(|c| c.name.clone()).collect();
                        let query_names = columns
                            .iter()
                            .map(|c| (c.name.clone(), c.canonical_name()))
                            .collect();
                        Some(page.select(&select_names).rename(&query_names))
                    }
                    None => None,
                }
            }
            Node::Aggregate {
                finished,
                group_by,
                aggregate,
                input,
            } => {
                if *finished {
                    return None;
                } else {
                    *finished = true;
                }
                let mut operator = crate::aggregate::GroupByAggregate::new(aggregate);
                loop {
                    match input.next(state) {
                        None => {
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
                            return Some(RecordBatch::new(columns));
                        }
                        Some(batch) => {
                            let group_by_columns: Vec<AnyArray> = group_by
                                .iter()
                                .map(|c| batch.find(&c.canonical_name()).unwrap().clone())
                                .collect();
                            let aggregate_columns: Vec<AnyArray> = aggregate
                                .iter()
                                .map(|a| batch.find(&a.input.canonical_name()).unwrap().clone())
                                .collect();
                            operator.insert(group_by_columns, aggregate_columns);
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
                let input = input.next(state)?;
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
                Some(input.slice(start_inclusive..end_exclusive))
            }
            Node::Sort { order_by, input } => {
                let input = input.next(state)?;
                let desc = order_by.iter().map(|o| o.descending).collect();
                let columns = order_by
                    .iter()
                    .map(|o| {
                        (
                            o.column.canonical_name(),
                            input.find(&o.column.canonical_name()).unwrap().clone(),
                        )
                    })
                    .collect();
                let indexes = RecordBatch::new(columns).sort(desc);
                let output = input.gather(&indexes);
                Some(output)
            }
            Node::Union { left, right } => match left.next(state) {
                Some(batch) => Some(batch),
                None => right.next(state),
            },
            Node::Broadcast { input, stream } => {
                if let Some(expr) = input.take() {
                    *stream = Some(RemoteQuery::new(
                        state.context[REMOTE_EXECUTION_KEY].broadcast(
                            expr,
                            state.variables.clone(),
                            state.txn,
                        ),
                    ));
                }
                stream.as_mut().unwrap().inner.next()
            }
            Node::Exchange { input, stream } => {
                if let Some((hash_column, expr)) = input.take() {
                    *stream = Some(RemoteQuery::new(
                        state.context[REMOTE_EXECUTION_KEY].exchange(
                            expr,
                            state.variables.clone(),
                            state.txn,
                            hash_column.canonical_name(),
                            state.context[WORKER_ID_KEY],
                        ),
                    ));
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
                    return None;
                } else {
                    *finished = true;
                }
                loop {
                    let input = match input.next(state) {
                        Some(next) => next,
                        None => break,
                    };
                    // Rename columns from query to match table.
                    let renames = columns
                        .iter()
                        .map(|(from, to)| (from.canonical_name(), to.clone()))
                        .collect();
                    let input = input.rename(&renames);
                    // Append rows to the table heap.
                    let txn = state.txn;
                    let mut storage = state.context[STORAGE_KEY].lock().unwrap();
                    let tids = storage.table_mut(table.id).insert(&input, txn);
                    // Update statistics.
                    storage
                        .statistics_mut(table.id)
                        .expect(&table.name)
                        .insert(&input);
                    // Update indexes.
                    for index in indexes.iter_mut() {
                        crate::index::insert(
                            storage.index_mut(index.index_id),
                            &index.columns,
                            &input,
                            &tids,
                        );
                    }
                }
                // Insert returns no values.
                None
            }
            Node::Values {
                columns,
                values,
                input,
            } => {
                let input = input.next(state)?;
                let mut output = vec![];
                for i in 0..columns.len() {
                    let mut builder = vec![];
                    for value in &values[i] {
                        let value = crate::eval::eval(value, &input, state);
                        if value.len() != 1 {
                            panic!("input to values produced {} rows", value.len());
                        }
                        builder.push(value)
                    }
                    output.push((columns[i].canonical_name(), AnyArray::cat(builder)));
                }
                Some(RecordBatch::new(output))
            }
            Node::Delete { table, tid, input } => {
                let input = input.next(state)?;
                // If no input, try next page.
                if input.len() == 0 {
                    return self.next(state);
                }
                // Identify rows to be updated by tid and sort them.
                let tids = match input.find(&tid.canonical_name()) {
                    Some(AnyArray::I64(tids)) => tids,
                    _ => panic!(),
                };
                let tids = tids.gather(&tids.sort());
                // Invalidate the old row versions.
                let storage = state.context[STORAGE_KEY].lock().unwrap();
                let heap = storage.table(table.id);
                let mut i = 0;
                while i < tids.len() {
                    let pid = tids.get(0).unwrap() as usize / storage::PAGE_SIZE;
                    let page = heap.page(pid);
                    while i < tids.len()
                        && pid == tids.get(i).unwrap() as usize / storage::PAGE_SIZE
                    {
                        let rid = tids.get(i).unwrap() as usize % storage::PAGE_SIZE;
                        page.delete(rid, state.txn);
                        i += 1;
                    }
                }
                Some(input)
            }
            Node::Script { offset, statements } => {
                while *offset < statements.len() {
                    match statements[*offset].next(state) {
                        None => {
                            *offset += 1;
                        }
                        Some(batch) => {
                            if *offset == statements.len() - 1 {
                                return Some(batch);
                            }
                        }
                    }
                }
                None
            }
            Node::Assign {
                variable,
                value,
                input,
            } => {
                let input = input.next(state)?;
                let value = crate::eval::eval(value, &input, state);
                state.variables.insert(variable.clone(), value);
                None
            }
            Node::Call { procedure, input } => {
                let input = input.next(state)?;
                match procedure {
                    Procedure::CreateTable(id) => {
                        let ids = crate::eval::eval(id, &input, state).as_i64();
                        for i in 0..ids.len() {
                            if let Some(id) = ids.get(i) {
                                state.context[STORAGE_KEY].lock().unwrap().create_table(id);
                            }
                        }
                    }
                    Procedure::DropTable(id) => {
                        let ids = crate::eval::eval(id, &input, state).as_i64();
                        for i in 0..ids.len() {
                            if let Some(id) = ids.get(i) {
                                state.context[STORAGE_KEY].lock().unwrap().drop_table(id);
                            }
                        }
                    }
                    Procedure::CreateIndex(id) => {
                        let ids = crate::eval::eval(id, &input, state).as_i64();
                        for i in 0..ids.len() {
                            if let Some(id) = ids.get(i) {
                                state.context[STORAGE_KEY].lock().unwrap().create_index(id);
                            }
                        }
                    }
                    Procedure::DropIndex(id) => {
                        let ids = crate::eval::eval(id, &input, state).as_i64();
                        for i in 0..ids.len() {
                            if let Some(id) = ids.get(i) {
                                state.context[STORAGE_KEY].lock().unwrap().drop_index(id);
                            }
                        }
                    }
                    Procedure::SetVar(name, value) => {
                        let name = crate::eval::eval(name, &input, state)
                            .as_string()
                            .get(0)
                            .unwrap()
                            .to_string();
                        let value = crate::eval::eval(value, &input, state);
                        state.variables.insert(name, value);
                    }
                };
                None
            }
            Node::Explain { finished, input } => {
                if *finished {
                    None
                } else {
                    *finished = true;
                    Some(RecordBatch::new(vec![(
                        "plan".to_string(),
                        AnyArray::String(StringArray::from_values(vec![input
                            .to_string()
                            .as_str()])),
                    )]))
                }
            }
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

fn dummy_schema() -> Vec<(String, DataType)> {
    vec![("$dummy".to_string(), DataType::Bool)]
}

fn schema(expr: &Expr) -> Vec<(String, DataType)> {
    match expr {
        TableFreeScan { .. } => dummy_schema(),
        Filter { input, .. }
        | Limit { input, .. }
        | Sort { input, .. }
        | Union { left: input, .. }
        | Broadcast { input, .. }
        | Exchange { input, .. }
        | Delete { input, .. } => schema(&*input),
        SeqScan { projects, .. } | Out { projects, .. } => projects
            .iter()
            .map(|c| (c.canonical_name(), c.data_type))
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
                fields.extend_from_slice(&schema(&*input));
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
                fields.extend_from_slice(&schema(&*input));
            }
            fields
        }
        NestedLoop {
            join, left, right, ..
        } => {
            let mut fields = vec![];
            fields.extend_from_slice(&schema(&*left));
            fields.extend_from_slice(&schema(&*right));
            if let Join::Mark(column, _) = join {
                fields.push((column.canonical_name(), column.data_type))
            }
            fields
        }
        HashJoin {
            join, left, right, ..
        } => {
            let mut fields = vec![];
            fields.extend_from_slice(&schema(&*left));
            fields.extend_from_slice(&schema(&*right));
            if let Join::Mark(column, _) = join {
                fields.push((column.canonical_name(), column.data_type))
            }
            fields
        }
        GetTempTable { columns, .. } => columns
            .iter()
            .map(|column| (column.canonical_name(), column.data_type))
            .collect(),
        Aggregate {
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
        Script { statements, .. } => schema(statements.last().unwrap()),
        Explain { .. } => vec![("plan".to_string(), DataType::String)],
        CreateTempTable { .. } | Insert { .. } | Assign { .. } | Call { .. } => dummy_schema(),
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
        | LogicalAssign { .. }
        | LogicalCall { .. }
        | LogicalExplain { .. }
        | LogicalRewrite { .. } => panic!(
            "schema is not implemented for logical operator {}",
            expr.name()
        ),
    }
}

fn build(input: &mut Node, state: &mut QueryState) -> Option<RecordBatch> {
    let mut batches = vec![];
    loop {
        match input.next(state) {
            None => return RecordBatch::cat(batches),
            Some(batch) => batches.push(batch),
        }
    }
}
