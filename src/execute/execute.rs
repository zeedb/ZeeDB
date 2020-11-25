use crate::error::Error;
use crate::eval::eval;
use crate::hash_table::HashTable;
use crate::state::State;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use ast::*;
use std::any::Any;
use std::sync::Arc;
use storage::*;

pub fn execute(expr: Expr, storage: &mut Storage) -> Result<Program<'_>, Error> {
    let state = State::new(storage);
    let input = compile(expr)?;
    Ok(Program { state, input })
}

pub struct Program<'a> {
    state: State<'a>,
    input: Input,
}

struct Input {
    node: Box<Node>,
    schema: Arc<Schema>,
}

enum Node {
    TableFreeScan,
    SeqScan {
        projects: Vec<Column>,
        predicates: Vec<Scalar>,
        table: Table,
        scan: Option<Vec<Arc<Page>>>,
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
        columns: Vec<Column>,
        input: Input,
    },
    Values {
        columns: Vec<Column>,
        rows: Vec<Vec<Scalar>>,
        input: Input,
    },
    Update {
        updates: Vec<(Column, Option<Column>)>,
        input: Input,
    },
    Delete {
        table: Table,
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
        procedure: String,
        arguments: Vec<Scalar>,
        returns: Option<DataType>,
        input: Input,
    },
}

impl<'a> Iterator for Program<'a> {
    type Item = Result<RecordBatch, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.input.next(&mut self.state) {
            Ok(page) if page.num_rows() == 0 => None,
            Ok(page) => Some(Ok(page)),
            Err(err) => Some(Err(err)),
        }
    }
}

fn compile(expr: Expr) -> Result<Input, Error> {
    let node = compile_node(expr)?;
    let schema = schema(&node);
    Ok(Input {
        node: Box::new(node),
        schema: Arc::new(schema),
    })
}

fn compile_node(expr: Expr) -> Result<Node, Error> {
    match expr {
        TableFreeScan => Ok(Node::TableFreeScan),
        SeqScan {
            projects,
            predicates,
            table,
        } => Ok(Node::SeqScan {
            projects,
            predicates,
            table,
            scan: None,
        }),
        IndexScan {
            projects,
            predicates,
            index_predicates,
            table,
        } => todo!(),
        Filter { predicates, input } => Ok(Node::Filter {
            predicates,
            input: compile(*input)?,
        }),
        Map {
            include_existing,
            projects,
            input,
        } => Ok(Node::Map {
            include_existing,
            projects,
            input: compile(*input)?,
        }),
        NestedLoop { join, left, right } => Ok(Node::NestedLoop {
            join,
            left: compile(*left)?,
            build_left: None,
            right: compile(*right)?,
        }),
        HashJoin {
            join,
            partition_left,
            partition_right,
            left,
            right,
        } => {
            let left = compile(*left)?;
            let right = compile(*right)?;
            Ok(Node::HashJoin {
                join,
                partition_left,
                partition_right,
                left,
                build_left: None,
                right,
            })
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
        Sort { order_by, input } => Ok(Node::Sort {
            order_by,
            input: compile(*input)?,
        }),
        Union { .. } => todo!(),
        Intersect { .. } => todo!(),
        Except { .. } => todo!(),
        Insert {
            table,
            columns,
            input,
        } => Ok(Node::Insert {
            table,
            columns,
            input: compile(*input)?,
        }),
        Values {
            columns,
            rows,
            input,
        } => Ok(Node::Values {
            columns,
            rows,
            input: compile(*input)?,
        }),
        Update { .. } => todo!(),
        Delete { .. } => todo!(),
        Script { statements } => {
            let mut compiled = vec![];
            for expr in statements {
                compiled.push(compile(expr)?)
            }
            Ok(Node::Script {
                offset: 0,
                statements: compiled,
            })
        }
        Assign {
            variable,
            value,
            input,
        } => Ok(Node::Assign {
            variable,
            value,
            input: compile(*input)?,
        }),
        Call {
            procedure,
            arguments,
            returns,
            input,
        } => Ok(Node::Call {
            procedure,
            arguments,
            returns,
            input: compile(*input)?,
        }),
        Leaf { .. }
        | LogicalSingleGet
        | LogicalGet { .. }
        | LogicalFilter { .. }
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

fn schema(compiled: &Node) -> Schema {
    match compiled {
        Node::TableFreeScan => dummy_schema(),
        Node::Filter { input, .. } | Node::Limit { input, .. } | Node::Sort { input, .. } => {
            schema(input.node.as_ref())
        }
        Node::SeqScan { projects, .. } | Node::IndexScan { projects, .. } => {
            let fields = projects
                .iter()
                .map(|column| {
                    Field::new(column.canonical_name().as_str(), column.data.clone(), false)
                    // TODO allow columns to be nullable
                })
                .collect();
            Schema::new(fields)
        }
        Node::Map {
            include_existing,
            projects,
            input,
        } => {
            let mut fields = vec![];
            if *include_existing {
                fields.extend_from_slice(schema(input.node.as_ref()).fields());
            }
            for (_, column) in projects {
                fields.push(Field::new(
                    column.canonical_name().as_str(),
                    column.data.clone(),
                    false,
                ))
                // TODO allow columns to be nullable
            }
            Schema::new(fields)
        }
        Node::NestedLoop {
            join, left, right, ..
        } => {
            let mut fields = vec![];
            fields.extend_from_slice(schema(left.node.as_ref()).fields());
            fields.extend_from_slice(schema(right.node.as_ref()).fields());
            if let Join::Mark(column, _) = join {
                fields.push(Field::new(
                    column.canonical_name().as_str(),
                    column.data.clone(),
                    false,
                ))
                // TODO allow columns to be nullable
            }
            Schema::new(fields)
        }
        Node::HashJoin {
            join, left, right, ..
        } => {
            let mut fields = vec![];
            fields.extend_from_slice(schema(left.node.as_ref()).fields());
            fields.extend_from_slice(schema(right.node.as_ref()).fields());
            if let Join::Mark(column, _) = join {
                fields.push(Field::new(
                    column.canonical_name().as_str(),
                    column.data.clone(),
                    false,
                ))
                // TODO allow columns to be nullable
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
        Node::Insert {
            table,
            columns,
            input,
        } => todo!(),
        Node::Values {
            columns,
            rows,
            input,
        } => todo!(),
        Node::Update { .. }
        | Node::Delete { .. }
        | Node::Script { .. }
        | Node::Assign { .. }
        | Node::Call { .. } => dummy_schema(),
    }
}

impl Input {
    fn next(&mut self, state: &mut State) -> Result<RecordBatch, Error> {
        match self.node.as_mut() {
            Node::TableFreeScan => Ok(dummy_row(self.schema.clone())),
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
                    Some(page) => Ok(crate::filter::filter(
                        predicates,
                        page.select(projects),
                        state,
                    )?),
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
                crate::filter::filter(predicates, input, state)
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
                    columns.push(eval(scalar, state, &input)?);
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
                crate::join::nested_loop(build_left.as_ref().unwrap(), right, &join, state)
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
                    right,
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
            Node::Sort { order_by, input } => sort(input.next(state)?, order_by),
            Node::Union { left, right } => todo!(),
            Node::Intersect { left, right } => todo!(),
            Node::Except { left, right } => todo!(),
            Node::Insert {
                table,
                columns,
                input,
            } => todo!(),
            Node::Values {
                columns,
                rows,
                input,
            } => todo!(),
            Node::Update { updates, input } => todo!(),
            Node::Delete { table, input } => todo!(),
            Node::Script { offset, statements } => {
                while *offset < statements.len() {
                    let next = statements[*offset].next(state)?;
                    if next.num_rows() > 0 {
                        return Ok(next);
                    } else {
                        *offset += 1;
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
                let value = eval(value, state, &input)?;
                let value = unwrap_scalar(value)?;
                state.variables.insert(variable.clone(), value);
                Err(Error::Empty)
            }
            Node::Call {
                procedure,
                arguments,
                returns,
                input,
            } => todo!(),
        }
    }
}

fn sort(input: RecordBatch, order_by: &Vec<OrderBy>) -> Result<RecordBatch, Error> {
    let sort_options = |order_by: &OrderBy| arrow::compute::SortOptions {
        descending: order_by.descending,
        nulls_first: order_by.nulls_first,
    };
    let sort_column = |order_by: &OrderBy| arrow::compute::SortColumn {
        options: Some(sort_options(order_by)),
        values: find(&input, &order_by.column),
    };
    let order_by: Vec<arrow::compute::SortColumn> = order_by.iter().map(sort_column).collect();
    let indices = arrow::compute::lexsort_to_indices(order_by.as_slice())?;
    let columns = input
        .columns()
        .iter()
        .map(|column| arrow::compute::take(column, &indices, None).unwrap())
        .collect();
    Ok(RecordBatch::try_new(input.schema().clone(), columns)?)
}

// fn empty(schema: Schema) -> RecordBatch {
//     schema
//         .fields()
//         .iter()
//         .map(|column| match column.data_type() {
//             DataType::Boolean => empty_bool_array(),
//             DataType::Int64 => empty_primitive_array::<Int64Type>(),
//             DataType::UInt64 => empty_primitive_array::<UInt64Type>(),
//             DataType::Float64 => empty_primitive_array::<Float64Type>(),
//             DataType::Date32(DateUnit::Day) => empty_primitive_array::<Date32Type>(),
//             DataType::Timestamp(TimeUnit::Microsecond, None) => empty_timestamp_array(),
//             DataType::FixedSizeBinary(16) => todo!(),
//             DataType::Utf8 => empty_string_array(),
//             DataType::Struct(fields) => todo!(),
//             DataType::List(element) => todo!(),
//             other => panic!("{:?} not supported", other),
//         })
//         .collect()
// }

// fn empty_bool_array() -> Arc<dyn Array> {
//     let array = BooleanArray::builder(0).finish();
//     Arc::new(array)
// }

// fn empty_primitive_array<T: ArrowNumericType>() -> Arc<dyn Array> {
//     let array = PrimitiveArray::<T>::builder(0).finish();
//     Arc::new(array)
// }

// fn empty_timestamp_array() -> Arc<dyn Array> {
//     let array = TimestampMicrosecondArray::builder(0).finish();
//     Arc::new(array)
// }

// fn empty_string_array() -> Arc<dyn Array> {
//     let array = StringBuilder::new(0).finish();
//     Arc::new(array)
// }

fn dummy_row(schema: Arc<Schema>) -> RecordBatch {
    RecordBatch::try_new(schema, vec![Arc::new(BooleanArray::from(vec![false]))]).unwrap()
}

fn dummy_schema() -> Schema {
    Schema::new(vec![Field::new(
        "$dummy", // TODO dummy column is gross
        DataType::Boolean,
        false,
    )])
}

fn unwrap_scalar(input: Arc<dyn Array>) -> Result<Box<dyn Any>, Error> {
    todo!()
}

fn build(input: &mut Input, state: &mut State) -> Result<RecordBatch, Error> {
    let mut batches = vec![];
    loop {
        match input.next(state) {
            Err(Error::Empty) if batches.is_empty() => return Err(Error::Empty),
            Err(Error::Empty) => return Ok(crate::common::concat_batches(batches)),
            Err(other) => return Err(other),
            Ok(batch) => batches.push(batch),
        }
    }
}

fn find(input: &RecordBatch, column: &Column) -> Arc<dyn Array> {
    for i in 0..input.num_columns() {
        if input.schema().field(i).name() == &column.canonical_name() {
            return input.column(i).clone();
        }
    }
    panic!("{} is not in {}", column.name, input.schema())
}
