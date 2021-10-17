use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use tokio::sync::mpsc::Receiver;
use tonic::Status;

use crate::Page;

pub struct PageStream {
    pub receiver: Receiver<Page>,
}

impl Stream for PageStream {
    type Item = Result<Page, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut().receiver.poll_recv(cx) {
            Poll::Ready(next) => match next {
                Some(page) => Poll::Ready(Some(Ok(page))),
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}
