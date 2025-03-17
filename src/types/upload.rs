use futures::Sink;
use pin_project_lite::pin_project;
use std::task::{ready, Context, Poll};
use std::{io::Error as IoError, pin::Pin, sync::Arc};
use tokio_util::codec::{Encoder, FramedWrite};

use crate::{
    types::{api::*, write_parts::WriteParts, UploadClient, UploadControl},
    AwsError,
};

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

    fn should_upload_part(&self) -> bool {
        let part_size = self.inner.write_buffer().len();
        self.ctrl.is_part_ready(part_size)
    }

    fn poll_complete_upload<I>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), AwsError>>
    where
        E: Encoder<I>,
        E::Error: From<IoError>,
        AwsError: From<E::Error>,
    {
        let parts = self.inner.get_ref().uploaded_parts();
        let params = self.inner.get_ref().params();
        tracing::trace!(?parts, ?params, "completing upload");

        let etag = ready!(self
            .client
            .complete_upload(&params, &parts)
            .as_mut()
            .poll(cx))?;
        // Callback with the uploaded object's entity tag.
        ready!(self.client.on_upload_complete(etag).as_mut().poll(cx))?;

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

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.should_upload_part() {
            ready!(self.as_mut().project().inner.poll_flush(cx))?;
        }

        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        self.project().inner.start_send(item)?;
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Flush the framed writer, which has the effect of uploading the last
        // part with whatever was flushed to it.  This is OK with AWS because
        // the last part isn't held to the minimum part size requirement.
        ready!(self.as_mut().project().inner.poll_flush(cx))?;
        self.poll_complete_upload(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().project().inner.poll_flush(cx))?;
        self.poll_complete_upload(cx)
    }
}
