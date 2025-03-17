use futures::Sink;
use pin_project_lite::pin_project;
use std::task::{ready, Context, Poll};
use std::{io::Error as IoError, pin::Pin, sync::Arc};
use tokio_util::codec::{Encoder, FramedWrite};

use super::write_parts::{UploadState, WriteParts};
use super::{UploadClient, UploadControl};
use crate::{types::api::*, AwsError};

pin_project! {
    /// `Upload` is a sink that implements the lifecycle of a single multipart
    /// upload.  It writes items to an inner `AsyncWrite` that periodically adds
    /// parts to the upload, then completes the upload when the inner writer
    /// has uploaded enough parts, bytes, or whatever else would make the method
    /// `UploadControl::is_upload_ready` return `true`.
    pub struct Upload<E> {
        #[pin]
        inner: FramedWrite<WriteParts, E>,
        client: Arc<dyn UploadClient + Send + Sync>,
        ctrl: Arc<dyn UploadControl + Send + Sync>,
    }
}

impl<E> Upload<E> {
    pub fn new<C, U>(client: U, ctrl: C, encoder: E, params: UploadRequestParams) -> Self
    where
        C: UploadControl + Send + Sync + 'static,
        U: UploadClient + Send + Sync + 'static,
    {
        let client = Arc::new(client);
        let ctrl = Arc::new(ctrl);
        let write = WriteParts::new(Arc::clone(&client), Arc::clone(&ctrl), params);
        let inner = FramedWrite::new(write, encoder);
        Self {
            inner,
            client,
            ctrl,
        }
    }

    pub(crate) fn get_upload_state_ref(&self) -> &UploadState {
        self.inner.get_ref().get_upload_state_ref()
    }

    pub(crate) fn should_complete_upload(&self) -> bool {
        let size = self.inner.get_ref().upload_size();
        let num_parts = self.inner.get_ref().num_parts();
        self.ctrl.is_upload_ready(size, num_parts)
    }

    fn poll_complete_upload<I>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), AwsError>>
    where
        E: Encoder<I>,
        E::Error: From<IoError>,
        AwsError: From<E::Error>,
    {
        let parts = self.inner.get_ref().uploaded_parts();
        let params = self.inner.get_ref().params();
        let this = self.as_mut().project();
        tracing::trace!(?parts, ?params, "completing upload");

        // Flush the framed writer, which has the effect of uploading the last
        // part with whatever was flushed to it.  This is OK with AWS because
        // the last part isn't held to the minimum part size requirement.
        ready!(this.inner.poll_flush(cx))?;

        let etag = ready!(self
            .client
            .complete_upload(&params, &parts)
            .as_mut()
            .poll(cx))?;
        tracing::trace!(%etag, "completed upload, executing callback");
        // Callback with the uploaded object's entity tag.
        ready!(self.client.on_upload_complete(&etag).as_mut().poll(cx))?;

        Poll::Ready(Ok(()))
    }
}

impl<E, I> Sink<I> for Upload<E>
where
    E: Encoder<I> + Clone,
    E::Error: From<IoError>,
    AwsError: From<E::Error>,
{
    type Error = AwsError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.should_complete_upload() {
            tracing::trace!(
                should_complete = self.should_complete_upload(),
                "calling poll_flush to complete",
            );
            self.poll_flush(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        self.project().inner.start_send(item)?;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_complete_upload(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        tracing::trace!("calling poll_close");
        self.poll_complete_upload(cx)
    }
}
