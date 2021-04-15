use std::{collections::HashSet, fmt};

use serde::{Deserialize, Serialize};

use crate::{Column, Scalar};

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Procedure {
    CreateTable(Scalar),
    DropTable(Scalar),
    CreateIndex(Scalar),
    DropIndex(Scalar),
}

impl Procedure {
    pub(crate) fn collect_references(&self, set: &mut HashSet<Column>) {
        match self {
            Procedure::CreateTable(argument)
            | Procedure::DropTable(argument)
            | Procedure::CreateIndex(argument)
            | Procedure::DropIndex(argument) => argument.collect_references(set),
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
        }
    }
}
