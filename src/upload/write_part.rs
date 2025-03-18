use async_tempfile::TempFile;
use aws_sdk_s3::primitives::ByteStream;
use futures::future::BoxFuture;
use pin_project_lite::pin_project;
use std::task::{ready, Context, Poll};
use std::{io::Error as IoError, pin::Pin, sync::Arc};
use tokio::io::AsyncWrite;

use crate::{
    client::UploadClient,
    types::{EntityTag, UploadParams},
    AwsError,
};

pin_project! {
    /// An implementation of `AsyncWrite` whose `poll_flush` and `poll_close`
    /// operate by uploading the internal buffer as a part in a multipart
    /// upload.
    pub(crate) struct WritePart {
        #[pin]
        inner: TempFile,
        client: Arc<dyn UploadClient + Send + Sync>,
        params: UploadParams,
    }
}

impl WritePart {
    pub(crate) async fn new<T>(client: T, params: UploadParams) -> Result<Self, AwsError>
    where
        T: UploadClient + Send + Sync + 'static,
    {
        let inner = new_inner().await?;
        Ok(Self {
            inner,
            params,
            client: Arc::new(client),
        })
    }

    /// Prepare the upload part request and poll for completion, returning the
    /// entity tag of the part found in the response from AWS.
    pub(crate) fn poll_upload_part(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        part_number: i32,
    ) -> Poll<Result<EntityTag, AwsError>> {
        // Stream the temp file into a retryable `ByteStream` for the AWS SDK
        // upload part request.
        let path = self.inner.file_path();
        tracing::warn!(path = ?path, "making part");
        let part = ready!(new_byte_stream(path).as_mut().poll(cx))?;
        tracing::warn!("made part");

        tracing::trace!(
            upload_id = ?self.params.upload_id(),
            bucket = self.params.bucket(),
            key = self.params.key(),
            part_number,
            "uploading part"
        );
        let etag = ready!(self
            .client
            .upload_part(&self.params, part_number, part)
            .as_mut()
            .poll(cx))?;

        // Replace the pinned temp file with a new one.
        // This drops the last reference to the file and causes it to be deleted
        // through the internals of `async_tempfile`.
        let new_inner = ready!(new_inner().as_mut().poll(cx))?;
        self.as_mut().project().inner.set(new_inner);

        Poll::Ready(Ok(etag))
    }
}

impl AsyncWrite for WritePart {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, IoError>> {
        let n = ready!(self.project().inner.poll_write(cx, buf))?;
        tracing::trace!(bytes = n, "wrote buf");
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        ready!(self.project().inner.poll_flush(cx))?;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        ready!(self.project().inner.poll_shutdown(cx))?;
        Poll::Ready(Ok(()))
    }
}

fn new_inner() -> BoxFuture<'static, Result<TempFile, AwsError>> {
    Box::pin(async move {
        let name = uuid::Uuid::new_v4();
        tracing::trace!(name = ?name, "temp file");
        let inner = TempFile::new_with_uuid(name).await?;
        Ok(inner)
    })
}

fn new_byte_stream<'a, P>(path: P) -> BoxFuture<'a, Result<ByteStream, AwsError>>
where
    P: AsRef<std::path::Path> + Send + Sync + 'a,
{
    Box::pin(async move {
        let part = ByteStream::from_path(path).await?;
        Ok(part)
    })
}
