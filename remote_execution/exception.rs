#[derive(Debug)]
pub enum Exception {
    SqlError(String),
    End,
}
