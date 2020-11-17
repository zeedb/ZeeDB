use crate::error::*;
use crate::eval::Eval;
use arrow::array::*;
use arrow::buffer::*;
use arrow::compute::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use ast::*;
use std::sync::Arc;
use storage::*;

pub trait ExecuteProvider {
    type E: Execute;
    fn start(self, storage: &Storage) -> Result<Self::E, Error>;
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
        offset: usize,
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
        equi_right: Vec<Scalar>,
        left: RecordBatch,
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
    CreateDatabase {
        name: Name,
    },
    CreateTable {
        name: Name,
        columns: Vec<(String, DataType)>,
        partition_by: Vec<i64>,
        cluster_by: Vec<i64>,
        primary_key: Vec<i64>,
        input: Option<Box<Program>>,
    },
    CreateIndex {
        name: Name,
        table: Name,
        columns: Vec<String>,
    },
    AlterTable {
        name: Name,
        actions: Vec<Alter>,
    },
    Drop {
        object: ObjectType,
        name: Name,
    },
    Rename {
        object: ObjectType,
        from: Name,
        to: Name,
    },
}

impl ExecuteProvider for Expr {
    type E = Program;

    fn start(self, storage: &Storage) -> Result<Self::E, Error> {
        match self {
            TableFreeScan => todo!(),
            SeqScan {
                projects,
                predicates,
                table,
            } => Ok(Program::SeqScan {
                projects,
                predicates,
                scan: storage.table(table.id as usize).scan(..),
                offset: 0,
            }),
            IndexScan {
                projects,
                predicates,
                index_predicates,
                table,
            } => todo!(),
            Filter(predicates, input) => Ok(Program::Filter {
                predicates,
                input: Box::new(input.start(storage)?),
            }),
            Map {
                include_existing,
                projects,
                input,
            } => todo!(),
            NestedLoop(_, _, _) => todo!(),
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
                Ok(Program::HashJoin {
                    join,
                    equi_right,
                    left: hash_build(equi_left, left.start(storage)?)?,
                    right: Box::new(right.start(storage)?),
                })
            }
            LookupJoin {
                join,
                projects,
                index_predicates,
                table,
                input,
            } => todo!(),
            CreateTempTable(_, _, _, _) => todo!(),
            GetTempTable(_, _) => todo!(),
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
            Sort(order_by, input) => Ok(Program::Sort {
                order_by,
                input: Box::new(input.start(storage)?),
            }),
            Union(_, _) => todo!(),
            Intersect(_, _) => todo!(),
            Except(_, _) => todo!(),
            Insert(_, _, _) => todo!(),
            Values(_, _, _) => todo!(),
            Update(_, _) => todo!(),
            Delete(_, _) => todo!(),
            CreateDatabase(_) => todo!(),
            CreateTable {
                name,
                columns,
                partition_by,
                cluster_by,
                primary_key,
                input,
            } => todo!(),
            CreateIndex {
                name,
                table,
                columns,
            } => todo!(),
            AlterTable { name, actions } => todo!(),
            Drop { object, name } => todo!(),
            Rename { object, from, to } => todo!(),
            Leaf(_)
            | LogicalSingleGet
            | LogicalGet { .. }
            | LogicalFilter(_, _)
            | LogicalMap { .. }
            | LogicalJoin { .. }
            | LogicalDependentJoin { .. }
            | LogicalWith(_, _, _, _)
            | LogicalGetWith(_, _)
            | LogicalAggregate { .. }
            | LogicalLimit { .. }
            | LogicalSort(_, _)
            | LogicalUnion(_, _)
            | LogicalIntersect(_, _)
            | LogicalExcept(_, _)
            | LogicalInsert(_, _, _)
            | LogicalValues(_, _, _)
            | LogicalUpdate(_, _)
            | LogicalDelete(_, _)
            | LogicalCreateDatabase(_)
            | LogicalCreateTable { .. }
            | LogicalCreateIndex { .. }
            | LogicalAlterTable { .. }
            | LogicalDrop { .. }
            | LogicalRename { .. } => panic!("logical operation"),
        }
    }
}

impl Execute for Program {
    fn next(&mut self) -> Result<RecordBatch, Error> {
        match self {
            Program::TableFreeScan => todo!(),
            Program::SeqScan {
                projects,
                predicates,
                scan,
                offset,
            } => {
                if *offset == scan.len() {
                    return Err(Error::Finished);
                }
                *offset += 1;
                Ok(scan[*offset - 1].select())
            }
            Program::IndexScan {
                projects,
                predicates,
                index_predicates,
                table,
            } => todo!(),
            Program::Filter { predicates, input } => {
                let input = input.next()?;
                let mut mask = predicates[0].eval(&input)?;
                for p in &predicates[1..] {
                    let next = p.eval(&input)?;
                    mask = Arc::new(and(
                        mask.as_any().downcast_ref::<BooleanArray>().unwrap(),
                        next.as_any().downcast_ref::<BooleanArray>().unwrap(),
                    )?);
                }
                let mut columns = vec![];
                for c in input.columns() {
                    columns.push(filter(
                        c.as_ref(),
                        mask.as_any().downcast_ref::<BooleanArray>().unwrap(),
                    )?)
                }
                Ok(RecordBatch::try_new(input.schema().clone(), columns).unwrap())
            }
            Program::Map {
                include_existing,
                projects,
                input,
            } => todo!(),
            Program::NestedLoop { join, left, right } => todo!(),
            Program::HashJoin {
                join,
                equi_right,
                left,
                right,
            } => todo!(),
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
            Program::Sort { order_by, input } => {
                let input = input.next()?;
                let sort_options = |order_by: &OrderBy| SortOptions {
                    descending: order_by.descending,
                    nulls_first: order_by.nulls_first,
                };
                let sort_column = |order_by: &OrderBy| SortColumn {
                    options: Some(sort_options(order_by)),
                    values: find(&input, &order_by.column),
                };
                let order_by: Vec<SortColumn> = order_by.iter().map(sort_column).collect();
                let indices = lexsort_to_indices(order_by.as_slice())?;
                let columns: Vec<Arc<dyn Array>> = input
                    .columns()
                    .iter()
                    .map(|column| take(column, &indices, None).unwrap())
                    .collect();
                Ok(RecordBatch::try_new(input.schema().clone(), columns).unwrap())
            }
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
            Program::CreateDatabase { name } => todo!(),
            Program::CreateTable {
                name,
                columns,
                partition_by,
                cluster_by,
                primary_key,
                input,
            } => todo!(),
            Program::CreateIndex {
                name,
                table,
                columns,
            } => todo!(),
            Program::AlterTable { name, actions } => todo!(),
            Program::Drop { object, name } => todo!(),
            Program::Rename { object, from, to } => todo!(),
        }
    }
}

fn find(input: &RecordBatch, column: &Column) -> Arc<dyn Array> {
    for i in 0..input.num_columns() {
        if input.schema().field(i).name() == &column.canonical_name() {
            return input.column(i).clone();
        }
    }
    panic!()
}

fn hash_build(equi_left: Vec<Scalar>, mut input: Program) -> Result<RecordBatch, Error> {
    loop {
        let page = input.next()?;
        let hash_left = hash(equi_left, &page)?;
        let index = sort_to_indices(&hash_left, None)?;
        todo!()
    }
}

fn hash(equi_left: Vec<Scalar>, input: &RecordBatch) -> Result<Arc<dyn Array>, Error> {
    let mut acc = MutableBuffer::new(input.num_rows() * std::mem::size_of::<u64>());
    for scalar in equi_left {
        let next = scalar.eval(input)?;
        bit_xor(
            &mut acc,
            &next.as_any().downcast_ref::<UInt64Array>().unwrap(),
        );
    }
    let data = ArrayDataBuilder::new(DataType::UInt64)
        .add_buffer(acc.freeze())
        .len(input.num_rows())
        .build();
    Ok(make_array(data))
}

fn bit_xor(acc: &mut MutableBuffer, next: &UInt64Array) {
    let acc_raw = acc.raw_data_mut();
    let next_raw = next.raw_values() as *const u8;
    for i in 0..acc.len() {
        unsafe { *acc_raw.offset(i as isize) ^= *next_raw.offset(i as isize) }
    }
}
