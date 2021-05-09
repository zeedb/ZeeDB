use std::{pin::Pin, task::Poll};

use futures::Stream;
use tokio::sync::mpsc::Receiver;
use tonic::Status;

use crate::Page;

// TODO name conflicts with other RecordStream.
pub struct RecordStream {
    pub receiver: Receiver<Page>,
}

impl Stream for RecordStream {
    type Item = Result<Page, Status>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.get_mut().receiver.poll_recv(cx) {
            Poll::Ready(next) => match next {
                Some(page) => Poll::Ready(Some(Ok(page))),
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}
