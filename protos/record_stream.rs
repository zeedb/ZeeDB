use crate::Page;
use kernel::RecordBatch;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::mpsc::Receiver;
use tonic::{codegen::Stream, Status};

pub struct RecordStream {
    receiver: Receiver<RecordBatch>,
}

impl Stream for RecordStream {
    type Item = Result<Page, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut().receiver.poll_recv(cx) {
            Poll::Ready(next) => match next {
                Some(batch) => Poll::Ready(Some(Ok(Page {
                    // TODO we should really use arrow format rather than bincode format here.
                    record_batch: bincode::serialize(&batch).unwrap(),
                }))),
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordStream {
    pub fn new(receiver: Receiver<RecordBatch>) -> Self {
        Self { receiver }
    }
}
