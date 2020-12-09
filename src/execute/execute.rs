use crate::hash_table::HashTable;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use ast::*;
use catalog::Index;
use kernel::Error;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use storage::*;

pub fn compile(expr: Expr) -> Program {
    Program { expr }
}

pub struct Program {
    expr: Expr,
}

impl Program {
    pub fn execute<'a>(&'a self, storage: &'a mut Storage, txn: u64) -> Execute<'a> {
        Execute {
            input: Input::compile(self.expr.clone()),
            state: State {
                storage,
                txn,
                variables: HashMap::new(),
            },
        }
    }
}

pub struct Execute<'a> {
    input: Input,
    state: State<'a>,
}

pub struct State<'a> {
    pub storage: &'a mut Storage,
    pub variables: HashMap<String, Arc<dyn Array>>,
    pub txn: u64,
}

struct Input {
    node: Box<Node>,
    schema: Arc<Schema>,
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
        scan: Option<Vec<Page>>,
    },
    IndexScan {
        include_existing: bool,
        projects: Vec<Column>,
        predicates: Vec<Scalar>,
        lookup: Vec<Scalar>,
        index: Index,
        table: Table,
        input: Input,
    },
    Filter {
        predicates: Vec<Scalar>,
        input: Input,
    },
    Out {
        projects: Vec<Column>,
        input: Input,
    },
    Map {
        include_existing: bool,
        projects: Vec<(Scalar, Column)>,
        input: Input,
    },
    NestedLoop {
        join: Join,
        left: Input,
        build_left: Option<RecordBatch>,
        right: Input,
    },
    HashJoin {
        join: Join,
        partition_left: Vec<Scalar>,
        partition_right: Vec<Scalar>,
        left: Input,
        build_left: Option<HashTable>,
        right: Input,
    },
    CreateTempTable {
        name: String,
        columns: Vec<Column>,
        left: Input,
        right: Input,
    },
    GetTempTable {
        name: String,
        columns: Vec<Column>,
    },
    Aggregate {
        finished: bool,
        group_by: Vec<Column>,
        aggregate: Vec<(AggregateFn, Column)>,
        input: Input,
    },
    Limit {
        limit: usize,
        offset: usize,
        input: Input,
    },
    Sort {
        order_by: Vec<OrderBy>,
        input: Input,
    },
    Union {
        left: Input,
        right: Input,
    },
    Intersect {
        left: Input,
        right: Input,
    },
    Except {
        left: Input,
        right: Input,
    },
    Insert {
        table: Table,
        indexes: Vec<Index>,
        input: Input,
    },
    Values {
        columns: Vec<Column>,
        values: Vec<Vec<Scalar>>,
        input: Input,
    },
    Delete {
        table: Table,
        tid: Column,
        input: Input,
    },
    Script {
        offset: usize,
        statements: Vec<Input>,
    },
    Assign {
        variable: String,
        value: Scalar,
        input: Input,
    },
    Call {
        procedure: Procedure,
        input: Input,
    },
}

impl<'a> Iterator for Execute<'a> {
    type Item = Result<RecordBatch, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.input.next(&mut self.state) {
            Ok(page) => Some(Ok(page)),
            Err(Error::Empty) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

impl Input {
    fn compile(expr: Expr) -> Self {
        let node = Node::compile(expr);
        let schema = node.schema();
        Input {
            node: Box::new(node),
            schema: Arc::new(schema),
        }
    }
}

impl Node {
    fn compile(expr: Expr) -> Self {
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
                input: Input::compile(*input),
            },
            Filter { predicates, input } => Node::Filter {
                predicates,
                input: Input::compile(*input),
            },
            Out { projects, input } => Node::Out {
                projects,
                input: Input::compile(*input),
            },
            Map {
                include_existing,
                projects,
                input,
            } => Node::Map {
                include_existing,
                projects,
                input: Input::compile(*input),
            },
            NestedLoop { join, left, right } => Node::NestedLoop {
                join,
                left: Input::compile(*left),
                build_left: None,
                right: Input::compile(*right),
            },
            HashJoin {
                join,
                partition_left,
                partition_right,
                left,
                right,
            } => {
                let left = Input::compile(*left);
                let right = Input::compile(*right);
                Node::HashJoin {
                    join,
                    partition_left,
                    partition_right,
                    left,
                    build_left: None,
                    right,
                }
            }
            CreateTempTable { .. } => todo!(),
            GetTempTable { .. } => todo!(),
            Aggregate {
                group_by,
                aggregate,
                input,
            } => Node::Aggregate {
                finished: false,
                group_by,
                aggregate,
                input: Input::compile(*input),
            },
            Limit {
                limit,
                offset,
                input,
            } => todo!(),
            Sort { order_by, input } => Node::Sort {
                order_by,
                input: Input::compile(*input),
            },
            Union { .. } => todo!(),
            Intersect { .. } => todo!(),
            Except { .. } => todo!(),
            Insert {
                table,
                indexes,
                input,
            } => Node::Insert {
                table,
                indexes,
                input: Input::compile(*input),
            },
            Values {
                columns,
                values,
                input,
            } => Node::Values {
                columns,
                values,
                input: Input::compile(*input),
            },
            Delete { table, tid, input } => Node::Delete {
                table,
                tid,
                input: Input::compile(*input),
            },
            Script { statements } => {
                let mut compiled = vec![];
                for expr in statements {
                    compiled.push(Input::compile(expr))
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
                input: Input::compile(*input),
            },
            Call { procedure, input } => Node::Call {
                procedure,
                input: Input::compile(*input),
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
            | LogicalGetWith { .. }
            | LogicalAggregate { .. }
            | LogicalLimit { .. }
            | LogicalSort { .. }
            | LogicalUnion { .. }
            | LogicalIntersect { .. }
            | LogicalExcept { .. }
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
            | LogicalCall { .. } => panic!("logical operation"),
        }
    }

    fn schema(&self) -> Schema {
        match self {
            Node::TableFreeScan { .. } => dummy_schema(),
            Node::Filter { input, .. } | Node::Limit { input, .. } | Node::Sort { input, .. } => {
                input.node.schema()
            }
            Node::SeqScan { projects, .. } => {
                let fields = projects
                    .iter()
                    .map(|column| column.into_query_field())
                    .collect();
                Schema::new(fields)
            }
            Node::IndexScan {
                include_existing,
                projects,
                input,
                ..
            } => {
                let mut fields = vec![];
                for column in projects {
                    fields.push(column.into_query_field())
                }
                if *include_existing {
                    fields.extend_from_slice(input.node.schema().fields());
                }
                Schema::new(fields)
            }
            Node::Out { projects, .. } => {
                let mut fields = vec![];
                for column in projects {
                    fields.push(column.into_query_field())
                }
                Schema::new(fields)
            }
            Node::Map {
                include_existing,
                projects,
                input,
            } => {
                let mut fields = vec![];
                if *include_existing {
                    fields.extend_from_slice(input.node.schema().fields());
                }
                for (_, column) in projects {
                    fields.push(column.into_query_field())
                }
                Schema::new(fields)
            }
            Node::NestedLoop {
                join, left, right, ..
            } => {
                let mut fields = vec![];
                fields.extend_from_slice(left.node.schema().fields());
                fields.extend_from_slice(right.node.schema().fields());
                if let Join::Mark(column, _) = join {
                    fields.push(column.into_query_field())
                }
                Schema::new(fields)
            }
            Node::HashJoin {
                join, left, right, ..
            } => {
                let mut fields = vec![];
                fields.extend_from_slice(left.node.schema().fields());
                fields.extend_from_slice(right.node.schema().fields());
                if let Join::Mark(column, _) = join {
                    fields.push(column.into_query_field())
                }
                Schema::new(fields)
            }
            Node::CreateTempTable {
                name,
                columns,
                left,
                right,
            } => todo!(),
            Node::GetTempTable { name, columns } => todo!(),
            Node::Aggregate {
                group_by,
                aggregate,
                ..
            } => {
                let mut fields = vec![];
                for column in group_by {
                    fields.push(column.into_query_field());
                }
                for (_, column) in aggregate {
                    fields.push(column.into_query_field());
                }
                Schema::new(fields)
            }
            Node::Union { left, right } => todo!(),
            Node::Intersect { left, right } => todo!(),
            Node::Except { left, right } => todo!(),
            Node::Insert { .. } => dummy_schema(),
            Node::Values {
                columns,
                values,
                input,
            } => Schema::new(
                columns
                    .iter()
                    .map(|column| column.into_query_field())
                    .collect(),
            ),
            Node::Delete { input, .. } => input.schema.as_ref().clone(),
            Node::Script { .. } | Node::Assign { .. } | Node::Call { .. } => dummy_schema(),
        }
    }
}

impl Input {
    fn next(&mut self, state: &mut State) -> Result<RecordBatch, Error> {
        match self.node.as_mut() {
            Node::TableFreeScan { empty } => {
                if *empty {
                    return Err(Error::Empty);
                }
                *empty = true;
                Ok(dummy_row(self.schema.clone()))
            }
            Node::SeqScan {
                projects,
                predicates,
                table,
                scan,
            } => {
                if scan.is_none() {
                    *scan = Some(state.storage.table(table.id).scan())
                }
                match scan.as_mut().unwrap().pop() {
                    Some(page) => {
                        let column_names = projects.iter().map(|c| c.canonical_name()).collect();
                        let input = page.select(&column_names);
                        let boolean = crate::eval::all(predicates, &input, state)?;
                        Ok(kernel::gather_logical(&input, &boolean))
                    }
                    None => Err(Error::Empty),
                }
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
                let mut keys = vec![];
                for scalar in lookup {
                    let column = crate::eval::eval(scalar, &input, state)?;
                    let key_part = crate::index::bytes(&column);
                    keys.push(key_part);
                }
                let keys = crate::index::zip(&keys);
                // Look up scalars in the index.
                let art = state.storage.index(index.index_id);
                let mut tids = vec![];
                for i in 0..keys.len() {
                    let start = keys.value(i);
                    let end = crate::index::upper_bound(start);
                    let next = art.range(start..end.as_slice());
                    tids.extend(next);
                }
                // Perform a selective scan of the table.
                let projects = projects.iter().map(|c| c.canonical_name()).collect();
                let mut output = state.storage.table(table.id).bitmap_scan(tids, &projects);
                if *include_existing {
                    output = kernel::zip(&output, &input);
                }
                let boolean = crate::eval::all(predicates, &output, state)?;
                Ok(kernel::gather_logical(&output, &boolean))
            }
            Node::Filter { predicates, input } => {
                let input = input.next(state)?;
                let boolean = crate::eval::all(predicates, &input, state)?;
                Ok(kernel::gather_logical(&input, &boolean))
            }
            Node::Out { projects, input } => {
                let input = input.next(state)?;
                let mut columns = vec![];
                for column in projects {
                    columns.push(kernel::find(&input, column));
                }
                Ok(RecordBatch::try_new(self.schema.clone(), columns).unwrap())
            }
            Node::Map {
                include_existing,
                projects,
                input,
            } => {
                let input = input.next(state)?;
                let mut columns = vec![];
                if *include_existing {
                    columns.extend_from_slice(input.columns());
                }
                for (scalar, _) in projects {
                    columns.push(crate::eval::eval(scalar, &input, state)?);
                }
                Ok(RecordBatch::try_new(self.schema.clone(), columns).unwrap())
            }
            Node::NestedLoop {
                join,
                left,
                build_left,
                right,
            } => {
                if build_left.is_none() {
                    let input = build(left, state)?;
                    *build_left = Some(input);
                }
                let right = right.next(state)?;
                crate::join::nested_loop(build_left.as_ref().unwrap(), &right, &join, state)
            }
            Node::HashJoin {
                join,
                partition_left,
                partition_right,
                left,
                build_left,
                right,
            } => {
                if build_left.is_none() {
                    let input = build(left, state)?;
                    let table = HashTable::new(partition_left, state, &input)?;
                    *build_left = Some(table);
                }
                let right = right.next(state)?;
                crate::join::hash_join(
                    build_left.as_mut().unwrap(),
                    &right,
                    partition_right,
                    join,
                    state,
                )
            }
            Node::CreateTempTable {
                name,
                columns,
                left,
                right,
            } => todo!(),
            Node::GetTempTable { name, columns } => todo!(),
            Node::Aggregate {
                finished,
                group_by,
                aggregate,
                input,
            } => {
                if *finished {
                    return Err(Error::Empty);
                } else {
                    *finished = true;
                }
                if group_by.is_empty() {
                    // Allocate state for each aggregator.
                    let mut states: Vec<_> = aggregate
                        .iter()
                        .map(|(aggregate, _)| crate::aggregate::SimpleAggregate::begin(aggregate))
                        .collect();
                    // Consume the entire input.
                    loop {
                        match input.next(state) {
                            Ok(input) => {
                                for state in &mut states {
                                    state.update(&input)?;
                                }
                            }
                            Err(Error::Empty) => {
                                let columns: Vec<_> =
                                    states.drain(..).map(|state| state.finish()).collect();
                                return Ok(
                                    RecordBatch::try_new(self.schema.clone(), columns).unwrap()
                                );
                            }
                            error => return error,
                        }
                    }
                } else {
                    todo!()
                }
            }
            Node::Limit {
                limit,
                offset,
                input,
            } => todo!(),
            Node::Sort { order_by, input } => crate::sort::sort(input.next(state)?, order_by),
            Node::Union { left, right } => todo!(),
            Node::Intersect { left, right } => todo!(),
            Node::Except { left, right } => todo!(),
            Node::Insert {
                table,
                indexes,
                input,
            } => {
                let input = input.next(state)?;
                // Append rows to the table heap.
                let heap = state.storage.table_mut(table.id);
                let tids = heap.insert(&input, state.txn);
                // Append entries to each index.
                for index in indexes {
                    crate::index::insert(
                        state.storage.index_mut(index.index_id),
                        &index.columns,
                        &input,
                        &tids,
                    );
                }
                Ok(dummy_row(self.schema.clone()))
            }
            Node::Values { values, input, .. } => {
                let input = input.next(state)?;
                let mut columns = vec![];
                for i in 0..values.len() {
                    columns.push(crate::eval::evals(&values[i], &input, state)?);
                }
                Ok(RecordBatch::try_new(self.schema.clone(), columns).unwrap())
            }
            Node::Delete { table, tid, input } => {
                let input = input.next(state)?;
                // If no input, try next page.
                if input.num_rows() == 0 {
                    return self.next(state);
                }
                // Identify rows to be updated by tid and sort them.
                let tids = input.column(
                    input
                        .schema()
                        .fields()
                        .iter()
                        .position(|f| f.name() == &tid.canonical_name())
                        .expect(&tid.canonical_name()),
                );
                let tids = kernel::gather(tids, &kernel::sort(tids));
                let tids: &UInt64Array = tids.as_any().downcast_ref::<UInt64Array>().unwrap();
                // Invalidate the old row versions.
                let heap = state.storage.table(table.id);
                let mut i = 0;
                while i < tids.len() {
                    let pid = tids.value(0) as usize / storage::PAGE_SIZE;
                    let page = heap.page(pid);
                    while i < tids.len() && pid == tids.value(i) as usize / storage::PAGE_SIZE {
                        page.delete(tids.value(i) as usize % storage::PAGE_SIZE, state.txn);
                        i += 1;
                    }
                }
                Ok(input)
            }
            Node::Script { offset, statements } => {
                while *offset < statements.len() {
                    match statements[*offset].next(state) {
                        Err(Error::Empty) => {
                            *offset += 1;
                        }
                        Err(error) => {
                            return Err(error);
                        }
                        Ok(batch) => {
                            return Ok(batch);
                        }
                    }
                }
                Err(Error::Empty)
            }
            Node::Assign {
                variable,
                value,
                input,
            } => {
                let input = input.next(state)?;
                let value = crate::eval::eval(value, &input, state)?;
                state.variables.insert(variable.clone(), value);
                Ok(dummy_row(self.schema.clone()))
            }
            Node::Call { procedure, input } => {
                let input = input.next(state)?;
                match procedure {
                    Procedure::CreateTable(id) => {
                        let id = crate::eval::eval(id, &input, state)?;
                        state.storage.create_table(kernel::int64(&id).unwrap());
                    }
                    Procedure::DropTable(id) => {
                        let id = crate::eval::eval(id, &input, state)?;
                        state.storage.drop_table(kernel::int64(&id).unwrap());
                    }
                    Procedure::CreateIndex(id) => {
                        let id = crate::eval::eval(id, &input, state)?;
                        state.storage.create_index(kernel::int64(&id).unwrap());
                    }
                    Procedure::DropIndex(id) => {
                        let id = crate::eval::eval(id, &input, state)?;
                        state.storage.drop_index(kernel::int64(&id).unwrap());
                    }
                };
                Ok(dummy_row(self.schema.clone()))
            }
        }
    }
}

impl fmt::Debug for Input {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.node.fmt(f)
    }
}

fn dummy_row(schema: Arc<Schema>) -> RecordBatch {
    RecordBatch::try_new(schema, vec![Arc::new(BooleanArray::from(vec![false]))]).unwrap()
}

fn dummy_schema() -> Schema {
    Schema::new(vec![Field::new(
        "$dummy", // TODO dummy column is gross
        DataType::Boolean,
        true,
    )])
}

// TODO instead of calling a function, insert a Build operator into the tree.
fn build(input: &mut Input, state: &mut State) -> Result<RecordBatch, Error> {
    let mut batches = vec![];
    loop {
        match input.next(state) {
            Err(Error::Empty) if batches.is_empty() => return Err(Error::Empty),
            Err(Error::Empty) => return Ok(kernel::cat(&batches)),
            Err(other) => return Err(other),
            Ok(batch) => batches.push(batch),
        }
    }
}
