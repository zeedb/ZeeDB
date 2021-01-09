use crate::search_space::*;
use ast::{Index, *};
use std::collections::{HashMap, HashSet};
use storage::Storage;

pub(crate) fn compute_logical_props(
    ss: &SearchSpace,
    statistics: &Storage,
    indexes: &HashMap<i64, Vec<Index>>,
    mid: MultiExprID,
) -> LogicalProps {
    let mexpr = &ss[mid];
    let mut cardinality = 0 as usize;
    let mut column_unique_cardinality: HashMap<Column, usize> = HashMap::new();
    match &mexpr.expr {
        LogicalSingleGet => cardinality = 1,
        LogicalGet {
            projects,
            predicates,
            table,
        } => {
            let index_predicate = indexes
                .get(&table.id)
                .unwrap_or(&vec![])
                .iter()
                .any(|index| index.matches(predicates).is_some());
            if index_predicate {
                cardinality = 1;
            } else {
                cardinality = statistics.table_cardinality(table.id);
            }
            for c in projects {
                column_unique_cardinality.insert(
                    c.clone(),
                    statistics.column_unique_cardinality(table.id, &c.name),
                );
            }
        }
        LogicalFilter { input, .. } => {
            let input = &ss[leaf(input)];
            cardinality = input.props.cardinality;
            column_unique_cardinality = input.props.column_unique_cardinality.clone();
        }
        LogicalOut { projects, input } => {
            let input = &ss[leaf(input)];
            cardinality = input.props.cardinality;
            for c in projects {
                let n = input.props.column_unique_cardinality.get(c).expect(
                    format!(
                        "no column {:?} in {:?}",
                        c,
                        input.props.column_unique_cardinality.keys()
                    )
                    .as_str(),
                );
                column_unique_cardinality.insert(c.clone(), *n);
            }
        }
        LogicalMap {
            include_existing,
            projects,
            input,
        } => {
            let input = &ss[leaf(input)];
            cardinality = input.props.cardinality;
            if *include_existing {
                column_unique_cardinality = input.props.column_unique_cardinality.clone();
            }
            for (x, c) in projects {
                let n = scalar_unique_cardinality(&x, &input.props.column_unique_cardinality);
                column_unique_cardinality.insert(c.clone(), n);
            }
        }
        LogicalJoin {
            join, left, right, ..
        } => {
            let left_cardinality = ss[leaf(left)].props.cardinality;
            let right_cardinality = ss[leaf(right)].props.cardinality;
            let left_scope = &ss[leaf(left)].props.column_unique_cardinality;
            let right_scope = &ss[leaf(right)].props.column_unique_cardinality;
            let is_equi_join = join
                .predicates()
                .iter()
                .any(|p| is_equi_predicate(p, left_scope, right_scope));
            if is_equi_join {
                cardinality = left_cardinality.max(right_cardinality).max(1);
            } else {
                cardinality = (left_cardinality * right_cardinality).max(1);
            }
            for (c, n) in left_scope {
                column_unique_cardinality.insert(c.clone(), *n);
            }
            for (c, n) in right_scope {
                column_unique_cardinality.insert(c.clone(), *n);
            }
            // Mark join projects the $mark attribute
            if let Join::Mark(mark, _) = join {
                column_unique_cardinality.insert(mark.clone(), 2);
            }
        }
        LogicalDependentJoin { .. } | LogicalWith { .. } => panic!(
            "{} should have been eliminated during rewrite phase",
            mexpr.expr.name()
        ),
        LogicalGetWith { columns, .. } => {
            cardinality = 1000; // TODO get from catalog somehow
            for c in columns {
                column_unique_cardinality.insert(c.clone(), cardinality);
            }
        }
        LogicalAggregate {
            group_by,
            aggregate,
            input,
        } => {
            let input = &ss[leaf(input)];
            cardinality = 1;
            for c in group_by {
                let n = input.props.column_unique_cardinality[c];
                column_unique_cardinality.insert(c.clone(), n);
                cardinality *= n;
            }
            cardinality = input.props.cardinality.min(cardinality);
            for a in aggregate {
                column_unique_cardinality.insert(a.output.clone(), cardinality);
            }
        }
        LogicalLimit { limit, input, .. } => {
            let input = &ss[leaf(input)];
            cardinality = *limit;
            for (c, n) in &input.props.column_unique_cardinality {
                if *limit < *n {
                    column_unique_cardinality.insert(c.clone(), *limit);
                } else {
                    column_unique_cardinality.insert(c.clone(), *n);
                }
            }
        }
        LogicalSort { input, .. } => {
            let input = &ss[leaf(input)];
            cardinality = input.props.cardinality;
            column_unique_cardinality = input.props.column_unique_cardinality.clone();
        }
        LogicalUnion { left, right } => {
            cardinality = ss[leaf(left)].props.cardinality + ss[leaf(right)].props.cardinality;
            column_unique_cardinality = max_cuc(
                &ss[leaf(left)].props.column_unique_cardinality,
                &ss[leaf(right)].props.column_unique_cardinality,
            );
        }
        LogicalScript { statements } => {
            let last = statements.last().unwrap();
            cardinality = ss[leaf(last)].props.cardinality;
            column_unique_cardinality = ss[leaf(last)].props.column_unique_cardinality.clone();
        }
        LogicalInsert { .. }
        | LogicalValues { .. }
        | LogicalUpdate { .. }
        | LogicalDelete { .. }
        | LogicalCreateDatabase { .. }
        | LogicalCreateTable { .. }
        | LogicalCreateTempTable { .. }
        | LogicalCreateIndex { .. }
        | LogicalDrop { .. }
        | LogicalAssign { .. }
        | LogicalCall { .. }
        | LogicalExplain { .. }
        | LogicalRewrite { .. } => {}
        Leaf { .. }
        | TableFreeScan { .. }
        | SeqScan { .. }
        | IndexScan { .. }
        | Filter { .. }
        | Out { .. }
        | Map { .. }
        | NestedLoop { .. }
        | HashJoin { .. }
        | CreateTempTable { .. }
        | GetTempTable { .. }
        | Aggregate { .. }
        | Limit { .. }
        | Sort { .. }
        | Union { .. }
        | Broadcast { .. }
        | Exchange { .. }
        | Insert { .. }
        | Values { .. }
        | Delete { .. }
        | Script { .. }
        | Assign { .. }
        | Call { .. }
        | Explain { .. } => panic!("{} is a physical operator", mexpr.expr.name()),
    };
    LogicalProps {
        cardinality,
        column_unique_cardinality,
    }
}

fn is_equi_predicate(
    p: &Scalar,
    left_scope: &HashMap<Column, usize>,
    right_scope: &HashMap<Column, usize>,
) -> bool {
    match p {
        Scalar::Call(f) => match f.as_ref() {
            F::Equal(left, right) | F::Is(left, right) => {
                let left_references = left.references();
                let right_references = right.references();
                (is_subset(&left_references, left_scope)
                    && is_subset(&right_references, right_scope))
                    || (is_subset(&left_references, right_scope)
                        && is_subset(&right_references, left_scope))
            }
            _ => false,
        },
        _ => false,
    }
}

fn is_subset(references: &HashSet<Column>, scope: &HashMap<Column, usize>) -> bool {
    references.iter().all(|column| scope.contains_key(column))
}

fn max_cuc(
    left: &HashMap<Column, usize>,
    right: &HashMap<Column, usize>,
) -> HashMap<Column, usize> {
    let mut max = left.clone();
    for (k, v) in right {
        if v > &left[k] {
            max.insert(k.clone(), *v);
        }
    }
    max
}

fn scalar_unique_cardinality(expr: &Scalar, scope: &HashMap<Column, usize>) -> usize {
    match expr {
        Scalar::Literal(_) => 1,
        Scalar::Column(column) => *scope
            .get(column)
            .unwrap_or_else(|| panic!("no key {:?} in {:?}", column, scope)),
        Scalar::Parameter(_, _) => 1,
        Scalar::Call(_) => 1, // TODO
        Scalar::Cast(value, _) => scalar_unique_cardinality(value, scope),
    }
}
