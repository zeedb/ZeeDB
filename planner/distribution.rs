use ast::*;
use rand::prelude::*;

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
        if let Broadcast { stage, .. } | Exchange { stage, .. } | Gather { stage, .. } = expr {
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

pub fn set_workers(expr: &mut Expr, txn: i64) {
    fn top_down_rewrite(expr: &mut Expr, txn: i64) {
        if let TableFreeScan { worker } | Gather { worker, .. } = expr {
            *worker = select_worker(txn)
        }
        for i in 0..expr.len() {
            top_down_rewrite(&mut expr[i], txn)
        }
    }
    top_down_rewrite(expr, txn);
}

fn select_worker(txn: i64) -> i32 {
    let mut rng = SmallRng::seed_from_u64(txn as u64);
    let count: i32 = std::env::var("WORKER_COUNT").unwrap().parse().unwrap();
    rng.gen_range(0..count)
}
