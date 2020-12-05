use crate::hash_table::HashTable;
use crate::state::State;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use ast::*;
use catalog::{Catalog, Index};
use kernel::Error;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use storage::*;

pub fn execute<'a>(
    expr: Expr,
    txn: u64,
    catalog: &'a Catalog,
    storage: &'a mut Storage,
) -> Program<'a> {
    let state = State::new(txn, catalog, storage);
    let input = Input::compile(expr);
    Program { state, input }
}

pub struct Program<'a> {
    state: State<'a>,
    input: Input,
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

impl<'a> Iterator for Program<'a> {
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
                projects,
                predicates,
                lookup,
                index,
                table,
                input,
            } => Node::IndexScan {
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
            } => todo!(),
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
            Insert { table, input } => Node::Insert {
                table,
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
            Node::SeqScan { projects, .. } | Node::IndexScan { projects, .. } => {
                let fields = projects
                    .iter()
                    .map(|column| column.into_query_field())
                    .collect();
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
                input,
            } => todo!(),
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
            Node::Delete { .. } | Node::Script { .. } | Node::Assign { .. } | Node::Call { .. } => {
                dummy_schema()
            }
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
                projects,
                predicates,
                lookup,
                index,
                table,
                input,
            } => todo!(),
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
                Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
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
                for (scalar, column) in projects {
                    columns.push(crate::eval::eval(scalar, &input, state)?);
                }
                Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
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
                group_by,
                aggregate,
                input,
            } => todo!(),
            Node::Limit {
                limit,
                offset,
                input,
            } => todo!(),
            Node::Sort { order_by, input } => crate::sort::sort(input.next(state)?, order_by),
            Node::Union { left, right } => todo!(),
            Node::Intersect { left, right } => todo!(),
            Node::Except { left, right } => todo!(),
            Node::Insert { table, input } => {
                let input = input.next(state)?;
                // Append rows to the table heap.
                let tids = state.storage.table_mut(table.id).insert(&input, state.txn);
                // Append entries to each index.
                for index in state.catalog.indexes.get(&table.id).unwrap_or(&vec![]) {
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
                // Identify rows to be updated by tid.
                let tids: &UInt64Array = input
                    .column(
                        input
                            .schema()
                            .fields()
                            .iter()
                            .position(|f| f.name() == &tid.canonical_name())
                            .expect(&tid.canonical_name()),
                    )
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                // Read the input sequentially, looking up the appropriate page, and invalidating the old row versions.
                let heap = state.storage.table(table.id);
                let mut pid = tids.value(0) as usize / storage::PAGE_SIZE;
                let mut page = heap.update(pid);
                for i in 0..tids.len() {
                    // If we've moved into a different page, look it up.
                    // We're relying on the input to the delete being approximately in page-order.
                    if tids.value(0) as usize / storage::PAGE_SIZE != pid {
                        pid = tids.value(0) as usize / storage::PAGE_SIZE;
                        page = heap.update(pid);
                    }
                    // Mark previous row versions as invalid.
                    page.delete(tids.value(i) as usize, state.txn);
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
                        state.storage.create_table(kernel::int64(&id));
                    }
                    Procedure::DropTable(id) => {
                        let id = crate::eval::eval(id, &input, state)?;
                        state.storage.drop_table(kernel::int64(&id));
                    }
                    Procedure::CreateIndex(id) => {
                        let id = crate::eval::eval(id, &input, state)?;
                        state.storage.create_index(kernel::int64(&id));
                    }
                    Procedure::DropIndex(id) => {
                        let id = crate::eval::eval(id, &input, state)?;
                        state.storage.drop_index(kernel::int64(&id));
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
