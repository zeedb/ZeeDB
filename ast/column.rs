use kernel::DataType;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    fmt,
    fmt::{Debug, Display},
    hash::Hash,
    sync::Arc,
};

#[derive(Clone, Serialize, Deserialize)]
pub struct Column {
    #[serde(skip)]
    pub id: Arc<()>,
    pub name: String,
    pub table_id: Option<i64>,
    pub table_name: Option<String>,
    pub data_type: DataType,
    pub created_late: bool,
}

impl Column {
    pub fn computed(name: &str, table_name: &Option<String>, data_type: DataType) -> Self {
        Self {
            id: Arc::new(()),
            name: name.to_string(),
            table_id: None,
            table_name: table_name.clone(),
            data_type: data_type.clone(),
            created_late: false,
        }
    }

    pub fn table(name: &str, table_id: i64, table_name: &str, data_type: DataType) -> Self {
        Self {
            id: Arc::new(()),
            name: name.to_string(),
            table_id: Some(table_id),
            table_name: Some(table_name.to_string()),
            data_type: data_type.clone(),
            created_late: false,
        }
    }

    pub fn fresh(copy: &Column) -> Self {
        Self {
            id: Arc::new(()),
            name: copy.name.clone(),
            table_id: copy.table_id.clone(),
            table_name: copy.table_name.clone(),
            data_type: copy.data_type.clone(),
            created_late: true,
        }
    }

    pub fn canonical_name(&self) -> String {
        if let Some(table) = &self.table_name {
            format!("{}.{}#{:?}", table, self.name, Arc::as_ptr(&self.id))
        } else {
            format!("{}#{:?}", self.name, Arc::as_ptr(&self.id))
        }
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.id, &other.id)
    }
}
impl Eq for Column {}

impl Hash for Column {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::ptr::hash(Arc::as_ptr(&self.id), state)
    }
}

impl PartialOrd for Column {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Column {
    fn cmp(&self, other: &Self) -> Ordering {
        let name = self.name.cmp(&other.name);
        let self_ptr = Arc::as_ptr(&self.id);
        let other_ptr = Arc::as_ptr(&other.id);
        let id = self_ptr.cmp(&other_ptr);
        name.then(id)
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
