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
pub(crate) async fn publish(topic: Uuid, message: Option<RecordBatch>) {
    sender(topic).send(message).await.unwrap()
}

/// Close a topic.
pub(crate) fn close(topic: Uuid) {
    let queue = TOPICS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut lock = queue.lock().unwrap();
    lock.remove(&topic)
        .expect("topic has already been closed")
        .receiver
        .expect_none("topic was never subscribed")
}

/// Receive a batch of records, or null, from topic.
/// This function will block until someone calls `send(topic, _)`.
pub(crate) fn subscribe(topic: Uuid) -> Receiver<Option<RecordBatch>> {
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

static TOPICS: OnceCell<Mutex<HashMap<Uuid, Topic>>> = OnceCell::new();

struct Topic {
    sender: Sender<Option<RecordBatch>>,
    receiver: Option<Receiver<Option<RecordBatch>>>,
}

fn sender(topic: Uuid) -> Sender<Option<RecordBatch>> {
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
