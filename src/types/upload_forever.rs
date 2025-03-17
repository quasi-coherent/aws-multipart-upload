use futures::Sink;
use pin_project_lite::pin_project;
use std::task::{ready, Context, Poll};
use std::{io::Error as IoError, pin::Pin, sync::Arc};
use tokio_util::codec::Encoder;

use super::{upload::Upload, write_parts::UploadState, UploadClient, UploadControl};
use crate::{types::api::*, AwsError};

pin_project! {
    /// `UploadForever` is a sink that extends `Upload` because it has the
    /// ability to produce the next S3 address on its own.  This enables the
    /// sink to start a new upload when one completes, which makes it suitable
    /// for an infinite stream use case.
    ///
    /// The upload address generation takes the form of an iterator over the
    /// `UploadAddress` type.  If `next()` returns `None`, this is an error.
    pub struct UploadForever<C, E, T, U> {
        #[pin]
        inner: Upload<E>,
        client: Arc<U>,
        ctrl: Arc<C>,
        encoder: E,
        upload_addr: T,
    }
}

impl<C, E, T, U> UploadForever<C, E, T, U>
where
    C: UploadControl + Send + Sync + 'static,
    E: Clone,
    T: Iterator<Item = UploadAddress>,
    U: UploadClient + Send + Sync + 'static,
{
    pub async fn new(client: U, ctrl: C, encoder: E, mut upload_addr: T) -> Result<Self, AwsError> {
        let addr = upload_addr.next().ok_or_else(|| AwsError::UploadForever)?;
        let client = Arc::new(client);
        let ctrl = Arc::new(ctrl);
        let params = client.new_upload(&addr).await?;
        let inner = Upload::new(
            Arc::clone(&client),
            Arc::clone(&ctrl),
            encoder.clone(),
            params,
        );

        Ok(Self {
            inner,
            client,
            ctrl,
            encoder,
            upload_addr,
        })
    }

    fn should_complete_upload(&self) -> bool {
        self.inner.should_complete_upload()
    }

    fn get_upload_state_ref(&self) -> &UploadState {
        self.inner.get_upload_state_ref()
    }

    fn poll_new_upload<I>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), AwsError>>
    where
        E: Encoder<I>,
        E::Error: From<IoError>,
        AwsError: From<E::Error>,
    {
        let addr = self
            .as_mut()
            .upload_addr
            .next()
            .ok_or_else(|| AwsError::UploadForever)?;
        let mut this = self.project();

        let params = ready!(this.client.new_upload(&addr).as_mut().poll(cx))?;
        tracing::trace!(params = ?params, "starting new upload");

        let new_client = Arc::clone(this.client);
        let new_ctrl = Arc::clone(this.ctrl);
        let new_inner = Upload::new(new_client, new_ctrl, this.encoder.clone(), params);
        this.inner.as_mut().set(new_inner);
        tracing::trace!(upload_state = ?this.inner.get_upload_state_ref(), "started new upload");

        Poll::Ready(Ok(()))
    }
}

impl<C, E, T, U, I> Sink<I> for UploadForever<C, E, T, U>
where
    C: UploadControl + Send + Sync + 'static,
    E: Encoder<I> + Clone,
    E::Error: From<IoError>,
    AwsError: From<E::Error>,
    T: Iterator<Item = UploadAddress>,
    U: UploadClient + Send + Sync + 'static,
{
    type Error = AwsError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.should_complete_upload() {
            tracing::trace!(
                upload_state = ?self.get_upload_state_ref(),
                should_complete = self.should_complete_upload(),
                "completing upload"
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

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.as_mut().project();
        // Flush the underlying upload sink before starting the next and
        // overwriting the pinned version of the previous sink.
        ready!(this.inner.as_mut().poll_flush(cx))?;
        self.poll_new_upload(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.as_mut().project();
        ready!(this.inner.as_mut().poll_flush(cx))?;
        self.poll_new_upload(cx)
    }
}
