use crate::uuid::Uuid;
use kernel::RecordBatch;
use once_cell::sync::OnceCell;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Mutex,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};

static TOPICS: OnceCell<Mutex<HashMap<Uuid, Topic>>> = OnceCell::new();

enum Topic {
    Waiting {
        sender: Sender<Option<RecordBatch>>,
        receiver: Receiver<Option<RecordBatch>>,
    },
    Open {
        sender: Sender<Option<RecordBatch>>,
    },
}

/// Send a batch of records, or null, to topic `uuid`.
/// This function will block until someone calls `receive(uuid)`.
pub(crate) async fn publish(uuid: Uuid, message: Option<RecordBatch>) {
    sender(uuid).send(message).await.unwrap()
}

/// Receive a batch of records, or null, from topic `uuid`.
/// This function will block until someone calls `send(uuid, _)`.
pub(crate) fn subscribe(uuid: Uuid) -> Receiver<Option<RecordBatch>> {
    receiver(uuid)
}

fn sender(uuid: Uuid) -> Sender<Option<RecordBatch>> {
    let queue = TOPICS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut lock = queue.lock().unwrap();
    match lock.entry(uuid) {
        Entry::Occupied(occupied) => match occupied.get() {
            Topic::Open { sender } | Topic::Waiting { sender, .. } => sender.clone(),
        },
        Entry::Vacant(vacant) => {
            let (sender, receiver) = channel(1);
            vacant.insert(Topic::Waiting {
                sender: sender.clone(),
                receiver,
            });
            sender
        }
    }
}

fn receiver(uuid: Uuid) -> Receiver<Option<RecordBatch>> {
    let queue = TOPICS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut lock = queue.lock().unwrap();
    match lock.entry(uuid) {
        // If the publisher arrives first, the topic will already exist, and we take the waiting receiver.
        Entry::Occupied(occupied) => match occupied.remove() {
            Topic::Open { .. } => panic!("Someone else is already subscribed to {:?}", uuid),
            Topic::Waiting { sender, receiver } => {
                lock.insert(uuid, Topic::Open { sender });
                receiver
            }
        },
        // If the subscriber arrives first, create a new topic ready to receive messages.
        Entry::Vacant(vacant) => {
            let (sender, receiver) = channel(1);
            vacant.insert(Topic::Open { sender });
            receiver
        }
    }
}
