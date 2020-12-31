#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Index {
    pub index_id: i64,
    pub table_id: i64,
    pub columns: Vec<String>,
}
