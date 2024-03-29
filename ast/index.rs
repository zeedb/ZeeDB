use crate::{Scalar, F};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Index {
    pub index_id: i64,
    pub table_id: i64,
    pub columns: Vec<String>,
}

impl Index {
    pub fn matches(&self, predicates: &Vec<Scalar>) -> Option<(Vec<Scalar>, Vec<Scalar>)> {
        let mut index_predicates = vec![];
        let mut remaining_predicates = vec![];
        for column_name in &self.columns {
            for predicate in predicates {
                match predicate {
                    Scalar::Call(function) => match function.as_ref() {
                        F::Equal(Scalar::Column(column), lookup)
                        | F::Equal(lookup, Scalar::Column(column))
                            if column_name == &column.name
                                && column.table.is_some()
                                && column.table.as_ref().unwrap().id == self.table_id =>
                        {
                            index_predicates.push(lookup.clone());
                        }
                        _ => remaining_predicates.push(predicate.clone()),
                    },
                    _ => remaining_predicates.push(predicate.clone()),
                }
            }
        }
        if self.columns.len() == index_predicates.len() {
            Some((index_predicates, remaining_predicates))
        } else {
            None
        }
    }
}
