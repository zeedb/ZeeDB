use crate::hash_table::HashTable;
use crate::state::State;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use ast::*;
use kernel::Error;
use std::fmt;
use std::sync::Arc;
use storage::*;

pub fn execute(expr: Expr, txn: u64, storage: &mut Storage) -> Program<'_> {
    let state = State::new(txn, storage);
    let input = compile(expr);
    Program { state, input }
}

#[derive(Debug)]
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
        index_predicates: Vec<(Column, Scalar)>,
        table: Table,
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
    LookupJoin {
        join: Join,
        projects: Vec<Column>,
        index_predicates: Vec<(Column, Scalar)>,
        table: Table,
        input: Input,
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
        schema: Arc<Schema>,
        input: Input,
    },
    Values {
        columns: Vec<Column>,
        values: Vec<Vec<Scalar>>,
        input: Input,
    },
    Update {
        updates: Vec<(Column, Option<Column>)>,
        input: Input,
    },
    Delete {
        pid: Column,
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

fn compile(expr: Expr) -> Input {
    let node = compile_node(expr);
    let schema = node.schema();
    Input {
        node: Box::new(node),
        schema: Arc::new(schema),
    }
}

fn compile_node(expr: Expr) -> Node {
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
            index_predicates,
            table,
        } => todo!(),
        Filter { predicates, input } => Node::Filter {
            predicates,
            input: compile(*input),
        },
        Out { projects, input } => Node::Out {
            projects,
            input: compile(*input),
        },
        Map {
            include_existing,
            projects,
            input,
        } => Node::Map {
            include_existing,
            projects,
            input: compile(*input),
        },
        NestedLoop { join, left, right } => Node::NestedLoop {
            join,
            left: compile(*left),
            build_left: None,
            right: compile(*right),
        },
        HashJoin {
            join,
            partition_left,
            partition_right,
            left,
            right,
        } => {
            let left = compile(*left);
            let right = compile(*right);
            Node::HashJoin {
                join,
                partition_left,
                partition_right,
                left,
                build_left: None,
                right,
            }
        }
        LookupJoin {
            join,
            projects,
            index_predicates,
            table,
            input,
        } => todo!(),
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
            input: compile(*input),
        },
        Union { .. } => todo!(),
        Intersect { .. } => todo!(),
        Except { .. } => todo!(),
        Insert {
            table,
            columns,
            input,
        } => Node::Insert {
            table,
            schema: Arc::new(Schema::new(
                columns
                    .iter()
                    .map(|column| column.into_table_field())
                    .collect(),
            )),
            input: compile(*input),
        },
        Values {
            columns,
            values,
            input,
        } => Node::Values {
            columns,
            values,
            input: compile(*input),
        },
        Update { .. } => todo!(),
        Delete { pid, tid, input } => Node::Delete {
            pid,
            tid,
            input: compile(*input),
        },
        Script { statements } => {
            let mut compiled = vec![];
            for expr in statements {
                compiled.push(compile(expr))
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
            input: compile(*input),
        },
        Call { procedure, input } => Node::Call {
            procedure,
            input: compile(*input),
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

impl Node {
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
            Node::LookupJoin {
                join,
                projects,
                index_predicates,
                table,
                input,
            } => todo!(),
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
            Node::Update { .. }
            | Node::Delete { .. }
            | Node::Script { .. }
            | Node::Assign { .. }
            | Node::Call { .. } => dummy_schema(),
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
                        let input = page.select(projects);
                        let boolean = crate::eval::all(predicates, &input, state)?;
                        Ok(kernel::gather_logical(&input, &boolean))
                    }
                    None => Err(Error::Empty),
                }
            }
            Node::IndexScan {
                projects,
                predicates,
                index_predicates,
                table,
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
            Node::LookupJoin {
                join,
                projects,
                index_predicates,
                table,
                input,
            } => todo!(),
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
            Node::Insert {
                table,
                schema,
                input,
            } => {
                let input = input.next(state)?;
                let input = RecordBatch::try_new(
                    schema.clone(),
                    input
                        .columns()
                        .iter()
                        .map(|column| column.clone())
                        .collect(),
                )
                .unwrap();
                state.storage.table_mut(table.id).insert(&input, state.txn);
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
            Node::Update { updates, input } => todo!(),
            Node::Delete { pid, tid, input } => {
                let input = input.next(state)?;
                let pid: &UInt64Array = input
                    .column(
                        input
                            .schema()
                            .fields()
                            .iter()
                            .position(|f| f.name() == &pid.canonical_name())
                            .expect(&pid.canonical_name()),
                    )
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                let tid: &UInt64Array = input
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
                for i in 0..input.num_rows() {
                    unsafe {
                        let pid = pid.value(i);
                        let tid = tid.value(i) as usize;
                        let page = Page::read(pid);
                        page.delete(tid, state.txn);
                    }
                }
                Ok(dummy_row(self.schema.clone()))
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
                    Procedure::CreateIndex(id) => todo!(),
                    Procedure::DropIndex(id) => todo!(),
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
