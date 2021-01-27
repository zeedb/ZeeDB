use kernel::DataType;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    fmt,
    fmt::{Debug, Display},
    hash::Hash,
    sync::atomic::AtomicU64,
};

#[derive(Clone, Serialize, Deserialize)]
pub struct Column {
    pub id: u64,
    pub name: String,
    pub table_id: Option<i64>,
    pub table_name: Option<String>,
    pub data_type: DataType,
    pub created_late: bool,
}

impl Column {
    pub fn computed(name: &str, table_name: &Option<String>, data_type: DataType) -> Self {
        Self {
            id: next_column_id(),
            name: name.to_string(),
            table_id: None,
            table_name: table_name.clone(),
            data_type: data_type.clone(),
            created_late: false,
        }
    }

    pub fn table(name: &str, table_id: i64, table_name: &str, data_type: DataType) -> Self {
        Self {
            id: next_column_id(),
            name: name.to_string(),
            table_id: Some(table_id),
            table_name: Some(table_name.to_string()),
            data_type: data_type.clone(),
            created_late: false,
        }
    }

    pub fn fresh(copy: &Column) -> Self {
        Self {
            id: next_column_id(),
            name: copy.name.clone(),
            table_id: copy.table_id.clone(),
            table_name: copy.table_name.clone(),
            data_type: copy.data_type.clone(),
            created_late: true,
        }
    }

    pub fn canonical_name(&self) -> String {
        if let Some(table) = &self.table_name {
            format!("{}.{}#{:#x}", table, self.name, self.id)
        } else {
            format!("{}#{:#x}", self.name, self.id)
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
        state.write_u64(self.id)
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
        match (&self.table_name, self.table_id) {
            (Some(table_name), Some(table_id)) => {
                write!(f, "{}#{}.{}", table_name, table_id, &self.name)?
            }
            (Some(table_name), _) => write!(f, "{}.{}", table_name, &self.name)?,
            (_, _) => write!(f, "{}", &self.name)?,
        }
        if self.created_late {
            write!(f, "'")?;
        }
        Ok(())
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(table) = &self.table_name {
            write!(f, "{}.", table)?;
        }
        write!(f, "{}", self.name)?;
        if self.created_late {
            write!(f, "'")?;
        }
        Ok(())
    }
}

fn next_column_id() -> u64 {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}
