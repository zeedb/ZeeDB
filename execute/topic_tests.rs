use crate::{topic::*, uuid::Uuid};
use kernel::{AnyArray, Array, I64Array, RecordBatch};

#[tokio::test]
async fn test_send_with_task() {
    let uuid = Uuid::random();
    let send = tokio::spawn(async move {
        publish(uuid, Some(sample())).await;
    });
    let found = subscribe(uuid).recv().await.unwrap().unwrap();
    send.await.unwrap();
    assert_eq!(format!("{:?}", sample()), format!("{:?}", found))
}

#[tokio::test]
async fn test_receive_with_task() {
    let uuid = Uuid::random();
    let receive = tokio::spawn(async move { subscribe(uuid).recv().await });
    publish(uuid, Some(sample())).await;
    let found = receive.await.unwrap().unwrap().unwrap();
    assert_eq!(format!("{:?}", sample()), format!("{:?}", found))
}

fn sample() -> RecordBatch {
    RecordBatch::new(vec![(
        "id".to_string(),
        AnyArray::I64(I64Array::from_values(vec![1])),
    )])
}
