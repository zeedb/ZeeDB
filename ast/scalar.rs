use std::collections::{HashMap, HashSet};

use kernel::DataType;
use serde::{Deserialize, Serialize};

use crate::{Column, Value, F};

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Scalar {
    Literal(Value),
    Parameter(String, DataType),
    Column(Column),
    Call(Box<F>),
    Cast(Box<Scalar>, DataType),
}

impl Scalar {
    pub fn data_type(&self) -> DataType {
        match self {
            Scalar::Literal(value) => value.data_type().clone(),
            Scalar::Parameter(_, data_type) => data_type.clone(),
            Scalar::Column(column) => column.data_type.clone(),
            Scalar::Call(function) => function.returns().clone(),
            Scalar::Cast(_, data_type) => data_type.clone(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Scalar::Literal(_) | Scalar::Column(_) | Scalar::Parameter(_, _) => 0,
            Scalar::Call(f) => f.len(),
            Scalar::Cast(_, _) => 1,
        }
    }

    pub fn references(&self) -> HashSet<Column> {
        let mut set = HashSet::new();
        self.collect_references(&mut set);
        set
    }

    pub(crate) fn collect_references(&self, free: &mut HashSet<Column>) {
        match self {
            Scalar::Literal(_) | Scalar::Parameter(_, _) => {}
            Scalar::Column(column) => {
                free.insert(column.clone());
            }
            Scalar::Call(function) => {
                for scalar in function.arguments() {
                    scalar.collect_references(free)
                }
            }
            Scalar::Cast(scalar, _) => scalar.collect_references(free),
        }
    }

    pub fn subst(self, map: &HashMap<Column, Column>) -> Self {
        match self {
            Scalar::Column(c) if map.contains_key(&c) => Scalar::Column(map[&c].clone()),
            Scalar::Call(f) => Scalar::Call(Box::new(f.map(|scalar| scalar.subst(map)))),
            Scalar::Cast(x, t) => Scalar::Cast(Box::new(x.subst(map)), t),
            _ => self,
        }
    }

    pub fn inline(self, expr: &Scalar, column: &Column) -> Self {
        match self {
            Scalar::Column(c) if &c == column => expr.clone(),
            Scalar::Call(f) => Scalar::Call(Box::new(f.map(|scalar| scalar.inline(expr, column)))),
            Scalar::Cast(uncast, data_type) => {
                Scalar::Cast(Box::new(uncast.inline(expr, column)), data_type.clone())
            }
            _ => self.clone(),
        }
    }

    pub fn replace(&mut self, variables: &HashMap<String, Value>) {
        if let Scalar::Parameter(name, _) = self {
            let value = variables.get(name).unwrap().clone();
            *self = Scalar::Literal(value)
        }
        for i in 0..self.len() {
            self[i].replace(variables)
        }
    }

    pub fn is_just(&self, column: &Column) -> bool {
        match self {
            Scalar::Column(c) => c == column,
            _ => false,
        }
    }
}

impl std::ops::Index<usize> for Scalar {
    type Output = Scalar;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            Scalar::Literal(_) | Scalar::Column(_) | Scalar::Parameter(_, _) => panic!("{}", index),
            Scalar::Call(f) => &f[index],
            Scalar::Cast(x, _) => {
                if index == 0 {
                    x.as_ref()
                } else {
                    panic!("{}", index)
                }
            }
        }
    }
}

impl std::ops::IndexMut<usize> for Scalar {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        match self {
            Scalar::Literal(_) | Scalar::Column(_) | Scalar::Parameter(_, _) => panic!("{}", index),
            Scalar::Call(f) => &mut f[index],
            Scalar::Cast(x, _) => {
                if index == 0 {
                    x.as_mut()
                } else {
                    panic!("{}", index)
                }
            }
        }
    }
}
