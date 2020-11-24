use crate::error::*;
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
    let mut state = State::new(storage);
    match compile(expr, &mut state) {
        Ok(compiled) => Ok(Program { compiled, state }),
        Err(err) => Err(err),
    }
}

fn compile(expr: Expr, state: &mut State) -> Result<Compiled, Error> {
    match expr {
        TableFreeScan => Ok(Compiled::TableFreeScan),
        SeqScan {
            projects,
            predicates,
            table,
        } => Ok(Compiled::SeqScan {
            projects,
            predicates,
            scan: state.storage.table(table.id as usize).scan(),
        }),
        IndexScan {
            projects,
            predicates,
            index_predicates,
            table,
        } => todo!(),
        Filter { predicates, input } => Ok(Compiled::Filter {
            predicates,
            input: Box::new(compile(*input, state)?),
        }),
        Map {
            include_existing,
            projects,
            input,
        } => Ok(Compiled::Map {
            include_existing,
            projects,
            input: Box::new(compile(*input, state)?),
        }),
        NestedLoop { .. } => todo!(),
        HashJoin {
            join,
            equi_predicates,
            left,
            right,
        } => {
            let mut equi_left = vec![];
            let mut equi_right = vec![];
            for (l, r) in equi_predicates {
                equi_left.push(l);
                equi_right.push(r);
            }
            let left = Box::new(compile(*left, state)?);
            let right = Box::new(compile(*right, state)?);
            Ok(Compiled::HashJoin {
                join,
                equi_left,
                equi_right,
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
        Sort { order_by, input } => Ok(Compiled::Sort {
            order_by,
            input: Box::new(compile(*input, state)?),
        }),
        Union { .. } => todo!(),
        Intersect { .. } => todo!(),
        Except { .. } => todo!(),
        Insert {
            table,
            columns,
            input,
        } => Ok(Compiled::Insert {
            table,
            columns,
            input: Box::new(compile(*input, state)?),
        }),
        Values {
            columns,
            rows,
            input,
        } => Ok(Compiled::Values {
            columns,
            rows,
            input: Box::new(compile(*input, state)?),
        }),
        Update { .. } => todo!(),
        Delete { .. } => todo!(),
        Script { statements } => {
            let mut compiled = vec![];
            for expr in statements {
                compiled.push(compile(expr, state)?)
            }
            Ok(Compiled::Script {
                offset: 0,
                statements: compiled,
            })
        }
        Assign {
            variable,
            value,
            input,
        } => Ok(Compiled::Assign {
            variable,
            value,
            input: Box::new(compile(*input, state)?),
        }),
        Call {
            procedure,
            arguments,
            returns,
            input,
        } => Ok(Compiled::Call {
            procedure,
            arguments,
            returns,
            input: Box::new(compile(*input, state)?),
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

pub struct Program<'a> {
    state: State<'a>,
    compiled: Compiled,
}

impl<'a> Iterator for Program<'a> {
    type Item = Result<RecordBatch, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.compiled.next(&mut self.state) {
            Ok(page) if page.num_rows() == 0 => None,
            Ok(page) => Some(Ok(page)),
            Err(err) => Some(Err(err)),
        }
    }
}

enum Compiled {
    TableFreeScan,
    SeqScan {
        projects: Vec<Column>,
        predicates: Vec<Scalar>,
        scan: Vec<Arc<Page>>,
    },
    IndexScan {
        projects: Vec<Column>,
        predicates: Vec<Scalar>,
        index_predicates: Vec<(Column, Scalar)>,
        table: Table,
    },
    Filter {
        predicates: Vec<Scalar>,
        input: Box<Compiled>,
    },
    Map {
        include_existing: bool,
        projects: Vec<(Scalar, Column)>,
        input: Box<Compiled>,
    },
    NestedLoop {
        join: Join,
        left: Box<Compiled>,
        right: Box<Compiled>,
    },
    HashJoin {
        join: Join,
        equi_left: Vec<Scalar>,
        equi_right: Vec<Scalar>,
        left: Box<Compiled>,
        build_left: Option<HashTable>,
        right: Box<Compiled>,
    },
    LookupJoin {
        join: Join,
        projects: Vec<Column>,
        index_predicates: Vec<(Column, Scalar)>,
        table: Table,
        input: Box<Compiled>,
    },
    CreateTempTable {
        name: String,
        columns: Vec<Column>,
        left: Box<Compiled>,
        right: Box<Compiled>,
    },
    GetTempTable {
        name: String,
        columns: Vec<Column>,
    },
    Aggregate {
        group_by: Vec<Column>,
        aggregate: Vec<(AggregateFn, Column)>,
        input: Box<Compiled>,
    },
    Limit {
        limit: usize,
        offset: usize,
        input: Box<Compiled>,
    },
    Sort {
        order_by: Vec<OrderBy>,
        input: Box<Compiled>,
    },
    Union {
        left: Box<Compiled>,
        right: Box<Compiled>,
    },
    Intersect {
        left: Box<Compiled>,
        right: Box<Compiled>,
    },
    Except {
        left: Box<Compiled>,
        right: Box<Compiled>,
    },
    Insert {
        table: Table,
        columns: Vec<Column>,
        input: Box<Compiled>,
    },
    Values {
        columns: Vec<Column>,
        rows: Vec<Vec<Scalar>>,
        input: Box<Compiled>,
    },
    Update {
        updates: Vec<(Column, Option<Column>)>,
        input: Box<Compiled>,
    },
    Delete {
        table: Table,
        input: Box<Compiled>,
    },
    Script {
        offset: usize,
        statements: Vec<Compiled>,
    },
    Assign {
        variable: String,
        value: Scalar,
        input: Box<Compiled>,
    },
    Call {
        procedure: String,
        arguments: Vec<Scalar>,
        returns: Option<DataType>,
        input: Box<Compiled>,
    },
}

impl Compiled {
    fn schema(&self) -> Schema {
        match self {
            Compiled::TableFreeScan => dummy_schema(),
            Compiled::Filter { input, .. }
            | Compiled::Limit { input, .. }
            | Compiled::Sort { input, .. } => input.schema(),
            Compiled::SeqScan { projects, .. } | Compiled::IndexScan { projects, .. } => {
                let fields = projects
                    .iter()
                    .map(|column| {
                        Field::new(column.canonical_name().as_str(), column.data.clone(), false)
                        // TODO allow columns to be nullable
                    })
                    .collect();
                Schema::new(fields)
            }
            Compiled::Map {
                include_existing,
                projects,
                input,
            } => {
                let mut fields = vec![];
                if *include_existing {
                    fields.extend_from_slice(input.schema().fields());
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
            Compiled::NestedLoop { join, left, right } => {
                let mut fields = vec![];
                fields.extend_from_slice(left.schema().fields());
                fields.extend_from_slice(right.schema().fields());
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
            Compiled::HashJoin {
                join, left, right, ..
            } => {
                let mut fields = vec![];
                fields.extend_from_slice(left.schema().fields());
                fields.extend_from_slice(right.schema().fields());
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
            Compiled::LookupJoin {
                join,
                projects,
                index_predicates,
                table,
                input,
            } => todo!(),
            Compiled::CreateTempTable {
                name,
                columns,
                left,
                right,
            } => todo!(),
            Compiled::GetTempTable { name, columns } => todo!(),
            Compiled::Aggregate {
                group_by,
                aggregate,
                input,
            } => todo!(),
            Compiled::Union { left, right } => todo!(),
            Compiled::Intersect { left, right } => todo!(),
            Compiled::Except { left, right } => todo!(),
            Compiled::Insert {
                table,
                columns,
                input,
            } => todo!(),
            Compiled::Values {
                columns,
                rows,
                input,
            } => todo!(),
            Compiled::Update { .. }
            | Compiled::Delete { .. }
            | Compiled::Script { .. }
            | Compiled::Assign { .. }
            | Compiled::Call { .. } => dummy_schema(),
        }
    }

    fn next(&mut self, state: &mut State) -> Result<RecordBatch, Error> {
        match self {
            Compiled::TableFreeScan => Ok(dummy_row()),
            Compiled::SeqScan {
                projects,
                predicates,
                scan,
            } => {
                if scan.is_empty() {
                    return Ok(empty(self.schema()));
                }
                seq_scan(scan, projects, predicates)
            }
            Compiled::IndexScan {
                projects,
                predicates,
                index_predicates,
                table,
            } => todo!(),
            Compiled::Filter { predicates, input } => {
                let input = input.next(state)?;
                filter(predicates, state, input)
            }
            Compiled::Map {
                include_existing,
                projects,
                input,
            } => {
                let input = input.next(state)?;
                let mut columns = vec![];
                let mut fields = vec![];
                if *include_existing {
                    columns.extend_from_slice(input.columns());
                    fields.extend_from_slice(input.schema().fields())
                }
                for (scalar, column) in projects {
                    columns.push(eval(scalar, state, &input)?);
                    fields.push(Field::new(
                        column.canonical_name().as_str(),
                        column.data.clone(),
                        false,
                        // TODO nullability
                    ));
                }
                Ok(RecordBatch::try_new(
                    Arc::new(Schema::new(fields)),
                    columns,
                )?)
            }
            Compiled::NestedLoop { join, left, right } => todo!(),
            Compiled::HashJoin {
                join,
                equi_left,
                equi_right,
                left,
                build_left,
                right,
            } => {
                if build_left.is_none() {
                    let input = build(left, state)?;
                    let table = HashTable::new(equi_left, state, &input)?;
                    *build_left = Some(table);
                }
                let right = right.next(state)?;
                if right.num_rows() == 0 {
                    return Ok(empty(self.schema()));
                }
                hash_join(
                    build_left.as_mut().unwrap(),
                    equi_left,
                    right,
                    equi_right,
                    state,
                    join,
                )
            }
            Compiled::LookupJoin {
                join,
                projects,
                index_predicates,
                table,
                input,
            } => todo!(),
            Compiled::CreateTempTable {
                name,
                columns,
                left,
                right,
            } => todo!(),
            Compiled::GetTempTable { name, columns } => todo!(),
            Compiled::Aggregate {
                group_by,
                aggregate,
                input,
            } => todo!(),
            Compiled::Limit {
                limit,
                offset,
                input,
            } => todo!(),
            Compiled::Sort { order_by, input } => sort(input.next(state)?, order_by),
            Compiled::Union { left, right } => todo!(),
            Compiled::Intersect { left, right } => todo!(),
            Compiled::Except { left, right } => todo!(),
            Compiled::Insert {
                table,
                columns,
                input,
            } => todo!(),
            Compiled::Values {
                columns,
                rows,
                input,
            } => todo!(),
            Compiled::Update { updates, input } => todo!(),
            Compiled::Delete { table, input } => todo!(),
            Compiled::Script { offset, statements } => {
                while *offset < statements.len() {
                    let next = statements[*offset].next(state)?;
                    if next.num_rows() > 0 {
                        return Ok(next);
                    } else {
                        *offset += 1;
                    }
                }
                Ok(empty(self.schema()))
            }
            Compiled::Assign {
                variable,
                value,
                input,
            } => {
                let input = input.next(state)?;
                let value = eval(value, state, &input)?;
                let value = unwrap(value)?;
                state.variables.insert(variable.clone(), value);
                Ok(empty(self.schema()))
            }
            Compiled::Call {
                procedure,
                arguments,
                returns,
                input,
            } => todo!(),
        }
    }
}

fn seq_scan(
    table: &mut Vec<Arc<Page>>,
    projects: &Vec<Column>,
    predicates: &Vec<Scalar>,
) -> Result<RecordBatch, Error> {
    Ok(table.pop().unwrap().select(projects))
}

fn filter(
    predicates: &Vec<Scalar>,
    state: &mut State,
    input: RecordBatch,
) -> Result<RecordBatch, Error> {
    let mut mask = eval(&predicates[0], state, &input)?;
    for p in &predicates[1..] {
        let next = eval(p, state, &input)?;
        mask = Arc::new(arrow::compute::and(
            mask.as_any().downcast_ref::<BooleanArray>().unwrap(),
            next.as_any().downcast_ref::<BooleanArray>().unwrap(),
        )?);
    }
    let mut columns = vec![];
    for c in input.columns() {
        columns.push(arrow::compute::filter(
            c.as_ref(),
            mask.as_any().downcast_ref::<BooleanArray>().unwrap(),
        )?)
    }
    Ok(RecordBatch::try_new(input.schema().clone(), columns).unwrap())
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
    let columns: Vec<Arc<dyn Array>> = input
        .columns()
        .iter()
        .map(|column| arrow::compute::take(column, &indices, None).unwrap())
        .collect();
    Ok(RecordBatch::try_new(input.schema().clone(), columns).unwrap())
}

fn hash_join(
    left: &HashTable,
    equi_left: &Vec<Scalar>,
    right: RecordBatch,
    equi_right: &Vec<Scalar>,
    state: &mut State,
    join: &Join,
) -> Result<RecordBatch, Error> {
    let buckets = left.hash(equi_right, state, &right)?;
    for i in 0..right.num_rows() {
        todo!()
    }
    todo!()
}

fn empty(schema: Schema) -> RecordBatch {
    let columns = schema
        .fields()
        .iter()
        .map(|column| match column.data_type() {
            DataType::Boolean => empty_bool_array(),
            DataType::Int64 => empty_primitive_array::<Int64Type>(),
            DataType::UInt64 => empty_primitive_array::<UInt64Type>(),
            DataType::Float64 => empty_primitive_array::<Float64Type>(),
            DataType::Date32(DateUnit::Day) => empty_primitive_array::<Date32Type>(),
            DataType::Timestamp(TimeUnit::Microsecond, None) => empty_timestamp_array(),
            DataType::FixedSizeBinary(16) => todo!(),
            DataType::Utf8 => empty_string_array(),
            DataType::Struct(fields) => todo!(),
            DataType::List(element) => todo!(),
            other => panic!("{:?} not supported", other),
        })
        .collect();
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

fn empty_bool_array() -> Arc<dyn Array> {
    let array = BooleanArray::builder(0).finish();
    Arc::new(array)
}

fn empty_primitive_array<T: ArrowNumericType>() -> Arc<dyn Array> {
    let array = PrimitiveArray::<T>::builder(0).finish();
    Arc::new(array)
}

fn empty_timestamp_array() -> Arc<dyn Array> {
    let array = TimestampMicrosecondArray::builder(0).finish();
    Arc::new(array)
}

fn empty_string_array() -> Arc<dyn Array> {
    let array = StringBuilder::new(0).finish();
    Arc::new(array)
}

fn dummy_row() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(dummy_schema()),
        vec![Arc::new(BooleanArray::from(vec![false]))],
    )
    .unwrap()
}

fn dummy_schema() -> Schema {
    Schema::new(vec![Field::new(
        "$dummy", // TODO dummy column is gross
        DataType::Boolean,
        false,
    )])
}

fn unwrap(input: Arc<dyn Array>) -> Result<Box<dyn Any>, Error> {
    todo!()
}

fn build(input: &mut Compiled, state: &mut State) -> Result<Vec<RecordBatch>, Error> {
    let mut acc = vec![];
    loop {
        let next = input.next(state)?;
        let empty = next.num_rows() == 0;
        acc.push(next);
        if empty {
            return Ok(acc);
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
