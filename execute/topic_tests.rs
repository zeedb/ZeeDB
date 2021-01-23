use crate::{topic::*, uuid::Uuid};
use kernel::{AnyArray, Array, I64Array, RecordBatch};

#[tokio::test]
async fn test_send_receive() {
    let uuid = Uuid::random();
    // Spawn an async task that sends a message on topic `uuid`.
    let send_task = tokio::spawn(async move {
        println!("send task started");
        publish(uuid, sample()).await;
        println!("sent message");
        close(uuid).await;
        println!("send task complete");
    });
    // Spawn an async task that receives a message on topic `uuid`.
    let receive_task = tokio::spawn(async move {
        println!("receive task started");
        let mut stream = subscribe(uuid);
        let message = stream.next().await.unwrap();
        println!("received message");
        assert!(stream.next().await.is_none());
        println!("receive task complete");
        message
    });
    send_task.await.unwrap();
    let found = receive_task.await.unwrap();
    assert_eq!(format!("{:?}", sample()), format!("{:?}", found))
}

fn sample() -> RecordBatch {
    RecordBatch::new(vec![(
        "id".to_string(),
        AnyArray::I64(I64Array::from_values(vec![1])),
    )])
}
