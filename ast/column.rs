use kernel::DataType;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    fmt,
    fmt::{Debug, Display},
    hash::Hash,
    sync::atomic::AtomicI64,
};
use zetasql::{ResolvedColumnProto, ResolvedComputedColumnProto, TableRefProto};

use crate::Table;

#[derive(Clone, Serialize, Deserialize)]
pub struct Column {
    pub id: i64,
    pub name: String,
    pub table: Option<Table>,
    pub data_type: DataType,
    pub created_late: bool,
}

impl Column {
    // Create a new Column with a unique ID that is different than any ID assigned by ZetaSQL.
    // This Column will only match itself. Any references to this Column must be created by cloning it.
    pub fn fresh(name: &str, data_type: DataType) -> Self {
        Self {
            id: next_column_id(),
            name: name.to_string(),
            table: None,
            data_type,
            created_late: false,
        }
    }

    // Convert a ZetaSQL reference to a Column.
    pub fn reference(c: &ResolvedColumnProto) -> Self {
        Self {
            id: c.column_id.unwrap(),
            name: c.name.clone().unwrap(),
            table: None,
            data_type: DataType::from(c.r#type.as_ref().unwrap()),
            created_late: false,
        }
    }

    // Convert a ZetaSQL computed column to a Column.
    pub fn computed(c: &ResolvedComputedColumnProto) -> Self {
        Self::reference(c.column.as_ref().unwrap())
    }

    // Convert a ZetaSQL table scan column to a Column.
    pub fn table(c: &ResolvedColumnProto, table: &TableRefProto) -> Self {
        Self {
            id: c.column_id.unwrap(),
            name: c.name.clone().unwrap(),
            table: Some(Table::from(table)),
            data_type: DataType::from(c.r#type.as_ref().unwrap()),
            created_late: false,
        }
    }

    pub fn canonical_name(&self) -> String {
        format!("{}#{}", self.escape_name(), self.id)
    }

    fn escape_name(&self) -> String {
        self.name.replace('.', "..")
    }
}

impl From<&ResolvedColumnProto> for Column {
    fn from(c: &ResolvedColumnProto) -> Self {
        Self {
            id: c.column_id.unwrap(),
            name: c.name.clone().unwrap(),
            table: None,
            data_type: DataType::from(c.r#type.as_ref().unwrap()),
            created_late: false,
        }
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Column {}

impl Hash for Column {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_i64(self.id)
    }
}

impl PartialOrd for Column {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Column {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name).then(self.id.cmp(&other.id))
    }
}

impl Debug for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(table) = &self.table {
            write!(f, "{}.{}", table, &self.name)?
        }
        if self.created_late {
            write!(f, "'")?;
        }
        Ok(())
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(table) = &self.table {
            write!(f, "{}.", table)?;
        }
        write!(f, "{}", self.name)?;
        if self.created_late {
            write!(f, "'")?;
        }
        Ok(())
    }
}

fn next_column_id() -> i64 {
    static SEQUENCE: AtomicI64 = AtomicI64::new(0);
    -SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}
