use crate::error::*;
use crate::eval::eval;
use crate::hash_table::HashTable;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use ast::*;
use std::sync::Arc;
use storage::*;

pub fn execute(expr: Expr, storage: &Storage) -> Result<Program, Error> {
    match expr {
        TableFreeScan => Ok(Program::TableFreeScan),
        SeqScan {
            projects,
            predicates,
            table,
        } => Ok(Program::SeqScan {
            projects,
            predicates,
            scan: storage.table(table.id as usize).scan(),
        }),
        IndexScan {
            projects,
            predicates,
            index_predicates,
            table,
        } => todo!(),
        Filter { predicates, input } => Ok(Program::Filter {
            predicates,
            input: Box::new(execute(*input, storage)?),
        }),
        Map {
            include_existing,
            projects,
            input,
        } => Ok(Program::Map {
            include_existing,
            projects,
            input: Box::new(execute(*input, storage)?),
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
            let left = build(execute(*left, storage)?)?;
            let left = HashTable::new(&equi_left, &left)?;
            let right = Box::new(execute(*right, storage)?);
            Ok(Program::HashJoin {
                join,
                equi_left,
                equi_right,
                left,
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
        Sort { order_by, input } => Ok(Program::Sort {
            order_by,
            input: Box::new(execute(*input, storage)?),
        }),
        Union { .. } => todo!(),
        Intersect { .. } => todo!(),
        Except { .. } => todo!(),
        Insert { .. } => todo!(),
        Values { .. } => todo!(),
        Update { .. } => todo!(),
        Delete { .. } => todo!(),
        Script { statements } => todo!(),
        Assign {
            variable,
            value,
            input,
        } => todo!(),
        Call {
            procedure,
            arguments,
            returns,
            input,
        } => todo!(),
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

pub trait Execute {
    // TODO make sure we always call execute repeatedly
    fn next(&mut self) -> Result<RecordBatch, Error>;
}

pub enum Program {
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
        input: Box<Program>,
    },
    Map {
        include_existing: bool,
        projects: Vec<(Scalar, Column)>,
        input: Box<Program>,
    },
    NestedLoop {
        join: Join,
        left: Box<Program>,
        right: Box<Program>,
    },
    HashJoin {
        join: Join,
        equi_left: Vec<Scalar>,
        equi_right: Vec<Scalar>,
        left: HashTable,
        right: Box<Program>,
    },
    LookupJoin {
        join: Join,
        projects: Vec<Column>,
        index_predicates: Vec<(Column, Scalar)>,
        table: Table,
        input: Box<Program>,
    },
    CreateTempTable {
        name: String,
        columns: Vec<Column>,
        left: Box<Program>,
        right: Box<Program>,
    },
    GetTempTable {
        name: String,
        columns: Vec<Column>,
    },
    Aggregate {
        group_by: Vec<Column>,
        aggregate: Vec<(AggregateFn, Column)>,
        input: Box<Program>,
    },
    Limit {
        limit: usize,
        offset: usize,
        input: Box<Program>,
    },
    Sort {
        order_by: Vec<OrderBy>,
        input: Box<Program>,
    },
    Union {
        left: Box<Program>,
        right: Box<Program>,
    },
    Intersect {
        left: Box<Program>,
        right: Box<Program>,
    },
    Except {
        left: Box<Program>,
        right: Box<Program>,
    },
    Insert {
        table: Table,
        columns: Vec<Column>,
        input: Box<Program>,
    },
    Values {
        columns: Vec<Column>,
        rows: Vec<Vec<Scalar>>,
        input: Box<Program>,
    },
    Update {
        updates: Vec<(Column, Option<Column>)>,
        input: Box<Program>,
    },
    Delete {
        table: Table,
        input: Box<Program>,
    },
}

impl Program {
    fn schema(&self) -> Schema {
        match self {
            Program::TableFreeScan => dummy_schema(),
            Program::Filter { input, .. }
            | Program::Limit { input, .. }
            | Program::Sort { input, .. } => input.schema(),
            Program::SeqScan { projects, .. } | Program::IndexScan { projects, .. } => {
                let fields = projects
                    .iter()
                    .map(|column| {
                        Field::new(column.canonical_name().as_str(), column.data.clone(), false)
                        // TODO allow columns to be nullable
                    })
                    .collect();
                Schema::new(fields)
            }
            Program::Map {
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
            Program::NestedLoop { join, left, right } => {
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
            Program::HashJoin {
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
            Program::LookupJoin {
                join,
                projects,
                index_predicates,
                table,
                input,
            } => todo!(),
            Program::CreateTempTable {
                name,
                columns,
                left,
                right,
            } => todo!(),
            Program::GetTempTable { name, columns } => todo!(),
            Program::Aggregate {
                group_by,
                aggregate,
                input,
            } => todo!(),
            Program::Union { left, right } => todo!(),
            Program::Intersect { left, right } => todo!(),
            Program::Except { left, right } => todo!(),
            Program::Insert {
                table,
                columns,
                input,
            } => todo!(),
            Program::Values {
                columns,
                rows,
                input,
            } => todo!(),
            Program::Update { updates, input } => todo!(),
            Program::Delete { table, input } => todo!(),
        }
    }

    pub fn next(&mut self) -> Result<RecordBatch, Error> {
        match self {
            Program::TableFreeScan => Ok(dummy_row()),
            Program::SeqScan {
                projects,
                predicates,
                scan,
            } => {
                if scan.is_empty() {
                    return Ok(empty(self.schema()));
                }
                seq_scan(scan, projects, predicates)
            }
            Program::IndexScan {
                projects,
                predicates,
                index_predicates,
                table,
            } => todo!(),
            Program::Filter { predicates, input } => filter(input.next()?, predicates),
            Program::Map {
                include_existing,
                projects,
                input,
            } => {
                let input = input.next()?;
                let mut columns = vec![];
                let mut fields = vec![];
                if *include_existing {
                    columns.extend_from_slice(input.columns());
                    fields.extend_from_slice(input.schema().fields())
                }
                for (scalar, column) in projects {
                    columns.push(eval(scalar, &input)?);
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
            Program::NestedLoop { join, left, right } => todo!(),
            Program::HashJoin {
                join,
                equi_left,
                equi_right,
                left,
                right,
            } => {
                let right = right.next()?;
                if right.num_rows() == 0 {
                    return Ok(empty(self.schema()));
                }
                hash_join(left, equi_left, right, equi_right, join)
            }
            Program::LookupJoin {
                join,
                projects,
                index_predicates,
                table,
                input,
            } => todo!(),
            Program::CreateTempTable {
                name,
                columns,
                left,
                right,
            } => todo!(),
            Program::GetTempTable { name, columns } => todo!(),
            Program::Aggregate {
                group_by,
                aggregate,
                input,
            } => todo!(),
            Program::Limit {
                limit,
                offset,
                input,
            } => todo!(),
            Program::Sort { order_by, input } => sort(input.next()?, order_by),
            Program::Union { left, right } => todo!(),
            Program::Intersect { left, right } => todo!(),
            Program::Except { left, right } => todo!(),
            Program::Insert {
                table,
                columns,
                input,
            } => todo!(),
            Program::Values {
                columns,
                rows,
                input,
            } => todo!(),
            Program::Update { updates, input } => todo!(),
            Program::Delete { table, input } => todo!(),
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

fn filter(input: RecordBatch, predicates: &Vec<Scalar>) -> Result<RecordBatch, Error> {
    let mut mask = eval(&predicates[0], &input)?;
    for p in &predicates[1..] {
        let next = eval(p, &input)?;
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
    join: &Join,
) -> Result<RecordBatch, Error> {
    let buckets = left.hash(equi_right, &right)?;
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

fn build(mut input: Program) -> Result<Vec<RecordBatch>, Error> {
    let mut acc = vec![];
    loop {
        let next = input.next()?;
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
