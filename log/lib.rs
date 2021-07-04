use std::{cell::RefCell, collections::HashMap, time::Instant};

use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};

static ZERO: OnceCell<Instant> = OnceCell::new();

thread_local!(static APPEND: RefCell<Vec<Event>> = RefCell::default());

pub fn session(worker_id: Option<i32>, stage: i32) -> Session {
    Session { worker_id, stage }
}

pub fn enter(name: impl Into<String>) -> Span {
    ZERO.get_or_init(Instant::now);
    Span {
        name: name.into(),
        start: Instant::now(),
    }
}

pub struct Session {
    worker_id: Option<i32>,
    stage: i32,
}

impl Drop for Session {
    fn drop(&mut self) {
        let process_name = self
            .worker_id
            .map(|i| format!("Worker-{}", i))
            .unwrap_or("Coordinator".to_string());
        let pid = self.worker_id.unwrap_or(-1);
        let tid = self.stage;
        let mut events = vec![];
        events.push(JsonTraceEvent::process_name(pid, process_name));
        for e in APPEND.with(|cell| cell.take()).drain(..).rev() {
            events.push(JsonTraceEvent::X {
                name: e.name,
                ts: e.start.duration_since(*ZERO.get().unwrap()).as_micros() as u64,
                dur: e.end.duration_since(e.start).as_micros() as u64,
                pid,
                tid,
            });
        }
        for e in events {
            println!("{},", serde_json::to_string(&e).unwrap());
        }
    }
}

pub struct Span {
    name: String,
    start: Instant,
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
