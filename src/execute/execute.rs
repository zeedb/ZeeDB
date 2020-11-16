use crate::eval::Eval;
use arrow::array::*;
use arrow::compute::*;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use ast::*;
use std::sync::Arc;

pub trait Execute {
    // TODO make sure we always call execute repeatedly
    fn next(&self, storage: &storage::Storage) -> Result<RecordBatch, ArrowError>;
}

impl Execute for Expr {
    fn next(&self, storage: &storage::Storage) -> Result<RecordBatch, ArrowError> {
        match self {
            TableFreeScan => todo!(),
            SeqScan {
                projects,
                predicates,
                table,
            } => todo!(),
            IndexScan {
                projects,
                predicates,
                index_predicates,
                table,
            } => todo!(),
            Filter(predicates, input) => {
                let input = input.next(storage)?;
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
            } => todo!(),
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
            Sort(order_by, input) => {
                let input = input.next(storage)?;
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

fn find(input: &RecordBatch, column: &Column) -> Arc<dyn Array> {
    for i in 0..input.num_columns() {
        if input.schema().field(i).name() == &name(column) {
            return input.column(i).clone();
        }
    }
    panic!()
}

fn name(column: &Column) -> String {
    format!("{}#{}", column.name, column.id).to_string()
}
