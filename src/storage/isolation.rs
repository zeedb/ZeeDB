#[derive(Debug, Copy, Clone)]
pub enum Isolation {
    Snapshot(u64),
    Serializable,
}
