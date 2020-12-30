use kernel::DataType;
use std::{cmp::Ordering, fmt, fmt::Debug, fmt::Display, hash::Hash, sync::Arc};

#[derive(Clone)]
pub struct Column {
    pub id: Arc<()>,
    pub name: String,
    pub table: Option<String>,
    pub data_type: DataType,
    pub created_late: bool,
}

impl Column {
    pub fn computed(name: &str, data_type: DataType) -> Self {
        Self {
            id: Arc::new(()),
            name: name.to_string(),
            table: None,
            data_type: data_type.clone(),
            created_late: false,
        }
    }

    pub fn table(name: &str, table: &str, data_type: DataType) -> Self {
        Self {
            id: Arc::new(()),
            name: name.to_string(),
            table: Some(table.to_string()),
            data_type: data_type.clone(),
            created_late: false,
        }
    }

    pub fn fresh(copy: &Column) -> Self {
        Self {
            id: Arc::new(()),
            name: copy.name.clone(),
            table: copy.table.as_ref().map(|table| table.clone()),
            data_type: copy.data_type.clone(),
            created_late: true,
        }
    }

    pub fn canonical_name(&self) -> String {
        if let Some(table) = &self.table {
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
        Display::fmt(self, f)
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
