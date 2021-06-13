use rpc::TraceEvent;

#[derive(Debug)]
pub enum Exception {
    Error(String),
    Trace(Vec<TraceEvent>),
    End,
}
