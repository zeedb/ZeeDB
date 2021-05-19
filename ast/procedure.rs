use std::{collections::HashSet, fmt};

use serde::{Deserialize, Serialize};

use crate::{Column, Scalar};

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Procedure {
    CreateTable(Scalar),
    DropTable(Scalar),
    CreateIndex(Scalar),
    DropIndex(Scalar),
    SetVar(Scalar, Scalar),
    Assert(Scalar, String),
}

impl Procedure {
    pub(crate) fn collect_references(&self, set: &mut HashSet<Column>) {
        match self {
            Procedure::CreateTable(x)
            | Procedure::DropTable(x)
            | Procedure::CreateIndex(x)
            | Procedure::DropIndex(x)
            | Procedure::Assert(x, _) => x.collect_references(set),
            Procedure::SetVar(x, y) => {
                x.collect_references(set);
                y.collect_references(set);
            }
        }
    }
}

impl fmt::Display for Procedure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Procedure::CreateTable(id) => write!(f, "create_table {}", id),
            Procedure::DropTable(id) => write!(f, "drop_table {}", id),
            Procedure::CreateIndex(id) => write!(f, "create_index {}", id),
            Procedure::DropIndex(id) => write!(f, "drop_index {}", id),
            Procedure::SetVar(name, value) => write!(f, "set_var {} {}", name, value),
            Procedure::Assert(test, description) => write!(f, "assert {} {}", test, description),
        }
    }
}
