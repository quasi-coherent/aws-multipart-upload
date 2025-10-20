use crate::codec::EncodedPart;
use crate::sdk::PartBody;
use crate::upload::{PartProgress, PartState};

use multipart_write::FusedMultipartWrite;
use multipart_write::prelude::*;
use std::pin::Pin;
use std::task::{Context, Poll};

/// `Encoded` is a [`MultipartWrite`] derived from [`EncodedPart`].
///
/// [`MultipartWrite`]: multipart_write::MultipartWrite
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct Encoded<E> {
    encoder: E,
    state: PartState,
}

impl<E> Encoded<E> {
    pub(super) fn new(encoder: E) -> Self {
        Self {
            encoder,
            state: PartState::default(),
        }
    }
}

impl<E, Item> MultipartWrite<Item> for Encoded<E>
where
    E: EncodedPart<Item> + Unpin,
{
    type Ret = PartProgress;
    type Output = PartBody;
    type Error = E::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, part: Item) -> Result<Self::Ret, Self::Error> {
        self.as_mut().encoder.encode(part)?;
        let bytes = self.encoder.current_size();
        self.as_mut().state.update(bytes);
        let progress = PartProgress::new(self.state);
        Ok(progress)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.as_mut().encoder.flush()?;
        Poll::Ready(Ok(()))
    }

    fn poll_complete(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<Self::Output, Self::Error>> {
        let body = self.as_mut().encoder.to_part_body()?;
        self.as_mut().state = PartState::default();
        Poll::Ready(Ok(body))
    }
}

impl<E, Item> FusedMultipartWrite<Item> for Encoded<E>
where
    E: EncodedPart<Item> + Unpin,
{
    fn is_terminated(&self) -> bool {
        false
    }
}
