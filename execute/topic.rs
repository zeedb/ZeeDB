use crate::uuid::Uuid;
use kernel::RecordBatch;
use once_cell::sync::OnceCell;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Mutex,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// Send a batch of records, or null, to topic.
/// This function will block until someone calls `receive(topic)`.
pub(crate) async fn publish(topic: Uuid, message: RecordBatch) {
    sender(topic).send(Message::Next(message)).await.unwrap()
}

/// Receive a batch of records, or null, from topic.
/// This function will block until someone calls `send(topic, _)`.
pub(crate) fn subscribe(topic: Uuid) -> RecordBatchStream {
    RecordBatchStream {
        topic,
        receiver: receiver(topic),
    }
}

/// Schedule a topic to be deleted once all readers have finished.
pub(crate) async fn close(topic: Uuid) {
    sender(topic).send(Message::Close).await.unwrap()
}

static TOPICS: OnceCell<Mutex<HashMap<Uuid, Topic>>> = OnceCell::new();

struct Topic {
    sender: Sender<Message>,
    receiver: Option<Receiver<Message>>,
}

#[derive(Debug)]
enum Message {
    Next(RecordBatch),
    Close,
}

pub(crate) struct RecordBatchStream {
    topic: Uuid,
    receiver: Receiver<Message>,
}

impl RecordBatchStream {
    pub async fn next(&mut self) -> Option<RecordBatch> {
        match self.receiver.recv().await.unwrap() {
            Message::Next(next) => Some(next),
            Message::Close => {
                drop(remove(self.topic));
                None
            }
        }
    }
}

fn sender(topic: Uuid) -> Sender<Message> {
    let queue = TOPICS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut lock = queue.lock().unwrap();
    match lock.entry(topic) {
        // If the subscriber arrives first, the topic will already exist, and we clone the existing sender.
        Entry::Occupied(occupied) => occupied.get().sender.clone(),
        // If the publisher arrives first, create a new topic ready to receive messages.
        Entry::Vacant(vacant) => {
            let (sender, receiver) = channel(1);
            vacant.insert(Topic {
                sender: sender.clone(),
                receiver: Some(receiver),
            });
            sender
        }
    }
}

fn receiver(topic: Uuid) -> Receiver<Message> {
    let queue = TOPICS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut lock = queue.lock().unwrap();
    match lock.entry(topic) {
        // If the publisher arrives first, the topic will already exist, and we take the waiting receiver.
        Entry::Occupied(mut occupied) => occupied.get_mut().receiver.take().unwrap(),
        // If the subscriber arrives first, create a new topic ready to receive messages.
        Entry::Vacant(vacant) => {
            let (sender, receiver) = channel(1);
            vacant.insert(Topic {
                sender,
                receiver: None,
            });
            receiver
        }
    }
}

fn remove(topic: Uuid) {
    let queue = TOPICS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut lock = queue.lock().unwrap();
    drop(lock.remove(&topic))
}
