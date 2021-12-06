use std::{collections::HashSet, fmt};

use serde::{Deserialize, Serialize};

use crate::{Column, Scalar};

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Procedure {
    CreateCatalog,
    CreateTable,
    CreateIndex,
    Assert(Scalar, String),
}

impl Procedure {
    pub(crate) fn collect_references(&self, set: &mut HashSet<Column>) {
        match self {
            Procedure::CreateCatalog | Procedure::CreateTable | Procedure::CreateIndex => {}
            Procedure::Assert(x, _) => x.collect_references(set),
        }
    }
}

impl fmt::Display for Procedure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Procedure::CreateCatalog => write!(f, "create_catalog"),
            Procedure::CreateTable => write!(f, "create_table"),
            Procedure::CreateIndex => write!(f, "create_index"),
            Procedure::Assert(test, description) => write!(f, "assert {} {}", test, description),
        }
    }
}
