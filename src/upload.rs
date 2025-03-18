mod upload_impl;
mod write_part;

use futures::Sink;
use pin_project_lite::pin_project;
use std::task::{Context, Poll};
use std::{io::Error as IoError, pin::Pin};
use tokio_util::codec::Encoder;

use crate::{
    client::UploadClient,
    types::{UploadAddress, UploadParams},
    AwsError, UploadConfig,
};

pin_project! {
    pub struct Upload<E> {
        #[pin]
        inner: upload_impl::UploadImpl<E>,
    }
}

impl<E> Upload<E> {
    pub async fn new<T, A>(
        client: T,
        codec: E,
        addr: A,
        config: UploadConfig,
    ) -> Result<Self, AwsError>
    where
        T: UploadClient + Send + Sync + 'static,
        A: Into<UploadAddress>,
    {
        let upload_addr = addr.into();
        let params = client.new_upload(&upload_addr).await?;
        let inner = upload_impl::UploadImpl::new(client, codec, params, config).await?;

        Ok(Self { inner })
    }

    /// Get the parameters of this upload.
    pub fn params(&self) -> &UploadParams {
        self.inner.upload_params()
    }

    /// Get the current size in bytes of the total upload.
    pub fn size(&self) -> usize {
        self.inner.upload_size()
    }

    /// Get the number of parts successfully uploaded so far.
    pub fn num_parts(&self) -> usize {
        self.inner.upload_num_parts()
    }

    /// Complete the current upload.
    ///
    /// This _must_ happen after a call to `poll_flush`.  Otherwise, anything
    /// written but not flushed will be lost.
    pub fn poll_complete_upload<I>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), AwsError>>
    where
        E: Encoder<I>,
    {
        self.project().inner.poll_complete_upload(cx)
    }

    /// Start a new multipart upload.
    ///
    /// This _must_ happen after a call to `poll_complete_upload`.  Otherwise,
    /// anything written but not upload will be lost.
    pub fn poll_new_upload<A>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        addr: A,
    ) -> Poll<Result<(), AwsError>>
    where
        A: Into<UploadAddress>,
    {
        self.project().inner.poll_new_upload(cx, addr)
    }
}

impl<E, I> Sink<I> for Upload<E>
where
    E: Encoder<I>,
    E::Error: From<IoError>,
    AwsError: From<E::Error>,
{
    type Error = AwsError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        self.project().inner.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx)
    }
}
