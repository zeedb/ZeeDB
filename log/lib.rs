use std::{cell::RefCell, collections::HashMap, sync::Mutex, time::Instant};

use once_cell::sync::OnceCell;
use rpc::{TraceSpan, TraceStage};
use serde::{Deserialize, Serialize};

static ZERO: OnceCell<Instant> = OnceCell::new();

static CACHE: OnceCell<Mutex<HashMap<CacheKey, CacheValue>>> = OnceCell::new();

thread_local!(static APPEND: RefCell<Vec<Event>> = RefCell::default());

pub fn session(txn: i64, stage: i32, worker: Option<i32>) -> Session {
    Session { txn, stage, worker }
}

pub fn enter(name: impl Into<String>) -> Span {
    ZERO.get_or_init(Instant::now);
    Span {
        name: name.into(),
        start: Instant::now(),
    }
}

pub fn trace(txn: i64, worker: Option<i32>) -> Vec<TraceStage> {
    let key = CacheKey { txn, worker };
    let value = CACHE
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .remove(&key)
        .unwrap_or_default();
    value.stages
}

pub struct Session {
    txn: i64,
    stage: i32,
    worker: Option<i32>,
}

pub struct Span {
    name: String,
    start: Instant,
}

#[derive(Default, Debug, Hash, PartialEq, Eq)]
struct CacheKey {
    txn: i64,
    worker: Option<i32>,
}

#[derive(Default)]
struct CacheValue {
    stages: Vec<TraceStage>,
}

impl Drop for Span {
    fn drop(&mut self) {
        APPEND.with(|cell| {
            cell.borrow_mut().push(Event {
                name: std::mem::take(&mut self.name),
                start: self.start,
                end: Instant::now(),
            })
        });
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let mut events = APPEND.with(|cell| cell.take());
        let zero = *ZERO.get().unwrap();
        let stage = TraceStage {
            stage: self.stage,
            worker: self.worker,
            spans: events
                .drain(..)
                .rev()
                .map(|e| TraceSpan {
                    name: e.name,
                    start: e.start.duration_since(zero).as_micros() as u64,
                    end: e.end.duration_since(zero).as_micros() as u64,
                })
                .collect(),
        };
        let key = CacheKey {
            txn: self.txn,
            worker: self.worker,
        };
        CACHE
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .entry(key)
            .or_default()
            .stages
            .push(stage);
    }
}

struct Event {
    name: String,
    start: Instant,
    end: Instant,
}

/// Record tracing information in the format described in
///   https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/edit
/// so that we can view them in chrome://tracing
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "ph")]
pub enum JsonTraceEvent {
    X {
        name: String,
        ts: u64,
        dur: u64,
        pid: i32,
        tid: i32,
    },
    M {
        name: &'static str,
        pid: i32,
        args: HashMap<&'static str, String>,
    },
}

pub fn to_json(stages: Vec<TraceStage>) -> Vec<JsonTraceEvent> {
    let mut events = vec![];
    for stage in stages {
        let process_name = stage
            .worker
            .map(|i| format!("Worker-{}", i))
            .unwrap_or("Coordinator".to_string());
        let pid = stage.worker.unwrap_or(-1);
        let tid = stage.stage;
        events.push(JsonTraceEvent::process_name(pid, process_name));
        for e in stage.spans {
            events.push(JsonTraceEvent::X {
                name: e.name,
                ts: e.start,
                dur: e.end - e.start,
                pid,
                tid,
            });
        }
    }
    events
}

impl JsonTraceEvent {
    fn process_name(pid: i32, name: String) -> Self {
        let mut args = HashMap::default();
        args.insert("name", name);
        JsonTraceEvent::M {
            name: "process_name",
            pid,
            args,
        }
    }
}

pub use log_attrs::*;
