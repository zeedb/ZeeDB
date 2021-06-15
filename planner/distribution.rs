use ast::*;

pub fn set_hash_columns(expr: &mut Expr) {
    fn top_down_rewrite(expr: &mut Expr, column: Option<Column>) {
        match expr {
            Exchange {
                hash_column, input, ..
            } => {
                *hash_column = Some(column.unwrap());
                top_down_rewrite(input, None);
            }
            Aggregate {
                partition_by,
                input,
                ..
            } => {
                top_down_rewrite(input, Some(partition_by.clone()));
            }
            HashJoin {
                broadcast: false,
                partition_left,
                partition_right,
                left,
                right,
                ..
            } => {
                top_down_rewrite(left, Some(partition_left.clone()));
                top_down_rewrite(right, Some(partition_right.clone()));
            }
            _ => {
                for i in 0..expr.len() {
                    top_down_rewrite(&mut expr[i], column.clone())
                }
            }
        }
    }
    top_down_rewrite(expr, None);
}

pub fn set_stages(expr: &mut Expr) {
    fn top_down_rewrite(expr: &mut Expr, next_stage: &mut i32) {
        if let Broadcast { stage, .. } | Exchange { stage, .. } = expr {
            *stage = *next_stage;
            *next_stage += 1;
        }
        for i in 0..expr.len() {
            top_down_rewrite(&mut expr[i], next_stage)
        }
    }
    let mut next_stage = 1;
    top_down_rewrite(expr, &mut next_stage);
}
