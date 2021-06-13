/// Record tracing information about the execution of a query.
/// Our ultimate goal is to output trace events in the format described in
///   https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/edit
/// so that we can view them in chrome://tracing
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use context::Context;
use kernel::AnyArray;
use rpc::TraceEvent;
use storage::Storage;

pub struct QueryState<'a> {
    pub txn: i64,
    pub variables: HashMap<String, AnyArray>,
    pub context: &'a Context,
    pub temp_tables: Storage,
    pub temp_table_ids: HashMap<String, i64>,
    pub start_time: Instant,
    pub trace_events: Vec<TraceSpan>,
}

pub struct TraceLock {
    start_time: Instant,
    end: Arc<AtomicU64>,
}

pub struct TraceSpan {
    pub name: String,
    pub begin: u64,
    pub end: Arc<AtomicU64>,
}

impl<'a> QueryState<'a> {
    pub fn new(txn: i64, variables: HashMap<String, AnyArray>, context: &'a Context) -> Self {
        Self {
            txn,
            variables,
            context,
            temp_tables: Default::default(),
            temp_table_ids: Default::default(),
            start_time: Instant::now(),
            trace_events: Default::default(),
        }
    }

    pub fn begin(&mut self, name: impl Into<String>) -> TraceLock {
        let end = Arc::new(AtomicU64::default());
        self.trace_events.push(TraceSpan {
            name: name.into(),
            begin: now(self.start_time),
            end: end.clone(),
        });
        TraceLock {
            start_time: self.start_time,
            end,
        }
    }

    pub fn trace_events(&self) -> Vec<TraceEvent> {
        self.trace_events
            .iter()
            .map(|span| TraceEvent {
                name: span.name.clone(),
                begin: span.begin,
                end: span.end.load(Ordering::Relaxed),
            })
            .collect()
    }
}

impl Drop for TraceLock {
    fn drop(&mut self) {
        self.end.store(now(self.start_time), Ordering::Relaxed);
    }
}

fn now(start_time: Instant) -> u64 {
    Instant::now().duration_since(start_time).as_micros() as u64
}
