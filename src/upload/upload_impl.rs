use bytes::{Buf, BytesMut};
use futures::{future, Future, Sink};
use pin_project_lite::pin_project;
use std::task::{ready, Context, Poll};
use std::{io::Error as IoError, pin::Pin, sync::Arc};
use tokio::io::AsyncWrite;
use tokio_util::{codec::Encoder, io};

use crate::{
    client::UploadClient,
    types::{EntityTag, UploadAddress, UploadParams, UploadedParts},
    upload::write_part::WritePart,
    AwsError, UploadConfig,
};

const DEFAULT_BUFFER_SIZE: usize = 8 * 1024;

#[derive(Debug, Clone, Default)]
struct UploadState {
    upload_size: usize,
    uploaded_parts: UploadedParts,
}

impl UploadState {
    fn update_from_part(&mut self, part_size: usize, etag: EntityTag) {
        self.upload_size += part_size;
        self.uploaded_parts.update(etag);
    }

    fn upload_size(&self) -> usize {
        self.upload_size
    }

    fn uploaded_parts(&self) -> &UploadedParts {
        &self.uploaded_parts
    }

    fn part_number(&self) -> i32 {
        self.uploaded_parts.next_part_number()
    }
}

// Possible actions to take in `poll_ready`.
// `PollReady::PollWrite`: The buffer has reached capacity and should be flushed
// to the underlying temp file.
// `PollReady::PollFlush`: The temp file has had enough bytes written to it to
// be uploaded.
// `PollReady::Available`: There is still capacity in the buffer.
#[derive(Debug, Clone)]
enum PollReady {
    PollWrite,
    PollFlush,
    Available,
}

pin_project! {
    pub(crate) struct UploadImpl<E> {
        #[pin]
        inner: WritePart,
        client: Arc<dyn UploadClient + Send + Sync>,
        codec: E,
        buf: BytesMut,
        part_size: usize,
        params: UploadParams,
        state: UploadState,
        min_part_size: usize,
        capacity: usize,
        sent: usize,
    }
}

impl<E> UploadImpl<E> {
    pub(crate) async fn new<T>(
        client: T,
        codec: E,
        params: UploadParams,
        config: UploadConfig,
    ) -> Result<Self, AwsError>
    where
        T: UploadClient + Send + Sync + 'static,
    {
        let client = Arc::new(client);
        let inner = WritePart::new(Arc::clone(&client), params.clone()).await?;
        let capacity = config.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE);
        let buf = BytesMut::with_capacity(capacity);

        Ok(Self {
            inner,
            client,
            codec,
            buf,
            part_size: 0,
            params,
            state: UploadState::default(),
            min_part_size: config.min_part_size,
            capacity,
            sent: 0,
        })
    }

    /// Return the upload's AWS parameters.
    pub(crate) fn upload_params(&self) -> &UploadParams {
        &self.params
    }

    /// Get the state of the upload in bytes.
    pub(crate) fn upload_size(&self) -> usize {
        self.state.upload_size()
    }

    /// Get the state of the upload in number of parts written so far.
    pub(crate) fn upload_num_parts(&self) -> usize {
        self.uploaded_parts().num_parts()
    }

    /// Get the state of the upload in progress uploading parts.
    pub(crate) fn uploaded_parts(&self) -> &UploadedParts {
        self.state.uploaded_parts()
    }

    /// Upload the part being held by the inner writer.
    pub(crate) fn poll_upload_part(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), AwsError>> {
        let part_number = self.state.part_number();
        let part_size = self.part_size;
        tracing::error!(part_number, part_size, "in poll_upload_part");

        let etag = ready!(self
            .as_mut()
            .project()
            .inner
            .poll_upload_part(cx, part_number))?;

        // Execute callback with the uploaded object's entity tag.
        ready!(self
            .client
            .on_upload_complete(self.upload_params().clone(), etag.clone())
            .as_mut()
            .poll(cx))?;

        // Update state from a successful part upload.
        self.as_mut().state.update_from_part(part_size, etag);
        self.as_mut().part_size = 0;

        Poll::Ready(Ok(()))
    }

    /// Start a new upload, resetting all internal state.
    pub(crate) fn poll_new_upload<A>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        addr: A,
    ) -> Poll<Result<(), AwsError>>
    where
        A: Into<UploadAddress>,
    {
        let params = ready!(self.client.new_upload(&addr.into()).as_mut().poll(cx))?;

        self.as_mut().buf.clear();
        self.as_mut().part_size = 0;
        self.params = params;
        self.as_mut().state = UploadState::default();

        Poll::Ready(Ok(()))
    }

    /// Complete the upload.
    pub(crate) fn poll_complete_upload<I>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), AwsError>>
    where
        E: Encoder<I>,
    {
        let parts = self.state.uploaded_parts();
        let params = self.params();

        tracing::trace!(uploaded_parts = ?parts, params = ?params, "completing upload");
        let etag = ready!(self.client.complete_upload(params, parts).as_mut().poll(cx))?;

        // Execute callback with the uploaded object's entity tag.
        ready!(self
            .client
            .on_upload_complete(params.clone(), etag)
            .as_mut()
            .poll(cx))?;

        Poll::Ready(Ok(()))
    }

    fn buf_is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    fn params(&self) -> &UploadParams {
        &self.params
    }

    fn poll_ready_action(&self) -> PollReady {
        if self.buf.len() >= self.capacity {
            PollReady::PollWrite
        } else if self.part_size >= self.min_part_size {
            PollReady::PollFlush
        } else {
            PollReady::Available
        }
    }

    fn write_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<usize, AwsError>> {
        let mut this = self.project();
        let mut written = 0;

        while this.buf.has_remaining() {
            // A future whose output is the bytes written in a chunked write
            // operation provided by the utility function `poll_write_buf`.
            let mut fut =
                future::poll_fn(|cx| io::poll_write_buf(this.inner.as_mut(), cx, this.buf));
            let n = ready!(Pin::new(&mut fut).poll(cx))?;

            if n == 0 {
                return Poll::Ready(Err(IoError::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write to transport",
                )))?;
            }

            written += n;
        }

        tracing::trace!(written, "bytes written");
        Poll::Ready(Ok(written))
    }
}

impl<E, I> Sink<I> for UploadImpl<E>
where
    E: Encoder<I>,
    E::Error: From<IoError>,
    AwsError: From<E::Error>,
{
    type Error = AwsError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.poll_ready_action() {
            PollReady::PollWrite => {
                tracing::debug!("should poll write");
                let n = ready!(self.as_mut().write_buf(cx))?;
                self.as_mut().part_size += n;
                Poll::Ready(Ok(()))
            }
            PollReady::PollFlush => {
                tracing::debug!("should poll flush");
                let n = ready!(self.as_mut().write_buf(cx))?;
                self.as_mut().part_size += n;
                self.as_mut().poll_flush(cx)
            }
            _ => Poll::Ready(Ok(())),
        }
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        let this = self.project();
        this.codec.encode(item, this.buf)?;
        tracing::trace!("sent");
        *this.sent += 1;
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        tracing::warn!(sent = self.sent, part_size = ?self.part_size, state = ?self.state, buf_size = self.buf.len(), "in poll_flush");
        // In case `poll_flush` sidestepped the part of `poll_ready` where the
        // buffer is written, for instance, if the upload happened in one part.
        if !self.buf_is_empty() {
            let n = ready!(self.as_mut().write_buf(cx))?;
            self.as_mut().part_size += n;
        }

        // `poll_flush` the inner temp file to make sure its internal buffer is
        // fully written to the file before calling `poll_upload_part`.
        ready!(self.as_mut().project().inner.poll_flush(cx))?;
        ready!(self.poll_upload_part(cx))?;
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        tracing::warn!(sent = self.sent, part_size = ?self.part_size, state = ?self.state, buf_size = self.buf.len(), "in poll_close");
        if !self.buf_is_empty() {
            let n = ready!(self.as_mut().write_buf(cx))?;
            self.as_mut().part_size += n;
        }

        // To close correctly, the buffer needs to be written and the last part
        // needs to be uploaded.  The size of the last part is not held to the
        // same requirements, so this is OK.  _Then_ it is safe to complete the
        // upload.
        ready!(self.as_mut().project().inner.poll_flush(cx))?;
        ready!(self.as_mut().poll_upload_part(cx))?;
        ready!(self.poll_complete_upload(cx))?;
        Poll::Ready(Ok(()))
    }
}
