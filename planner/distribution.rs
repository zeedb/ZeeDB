use ast::*;

pub fn set_hash_columns(mut expr: Expr) -> Expr {
    fn top_down_rewrite(expr: &mut Expr, column: Option<Column>) {
        match (expr, column) {
            (Exchange { hash_column, input }, Some(column)) => {
                *hash_column = Some(column);
                top_down_rewrite(input, None);
            }
            (
                Aggregate {
                    partition_by: Some(partition_by),
                    input,
                    ..
                },
                _,
            ) => {
                top_down_rewrite(input, Some(partition_by.clone()));
            }
            (
                HashJoin {
                    broadcast: false,
                    partition_left,
                    partition_right,
                    left,
                    right,
                    ..
                },
                _,
            ) => {
                top_down_rewrite(left, Some(partition_left.clone()));
                top_down_rewrite(right, Some(partition_right.clone()));
            }
            (expr, column) => {
                for i in 0..expr.len() {
                    top_down_rewrite(&mut expr[i], column.clone())
                }
            }
        }
    }
    top_down_rewrite(&mut expr, None);
    expr
}
