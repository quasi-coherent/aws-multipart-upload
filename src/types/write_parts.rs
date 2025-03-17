use aws_sdk_s3::primitives::ByteStream;
use bytes::BytesMut;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::{io::Error as IoError, sync::Arc};
use tokio::io::AsyncWrite;

use super::{api::*, UploadClient, UploadControl};

/// An implementation of `AsyncWrite` whose `poll_flush` uploads the write
/// buffer as a part.
pub(crate) struct WriteParts {
    buf: BytesMut,
    client: Arc<dyn UploadClient + Send + Sync>,
    ctrl: Arc<dyn UploadControl + Send + Sync>,
    params: UploadRequestParams,
    upload_state: UploadState,
}

impl WriteParts {
    pub(crate) fn new<C, U>(client: U, ctrl: C, params: UploadRequestParams) -> Self
    where
        C: UploadControl + Send + Sync + 'static,
        U: UploadClient + Send + Sync + 'static,
    {
        Self {
            buf: Self::init_buf(ctrl.target_part_size()),
            client: Arc::new(client),
            ctrl: Arc::new(ctrl),
            params,
            upload_state: UploadState::default(),
        }
    }

    /// Says whether to call `poll_upload` to upload the current part.
    pub(crate) fn should_upload_part(&self) -> bool {
        self.ctrl.is_part_ready(self.part_size())
    }

    /// Get a reference to the upload request parameters for this upload.
    pub(crate) fn params_ref(&self) -> &UploadRequestParams {
        &self.params
    }

    /// Clone the request parameters.
    pub(crate) fn params(&self) -> UploadRequestParams {
        self.params.clone()
    }

    /// Get a reference to the internal state of this writer.
    pub(crate) fn get_upload_state_ref(&self) -> &UploadState {
        &self.upload_state
    }

    /// Returns the size of the part currently being written.
    pub(crate) fn part_size(&self) -> usize {
        self.buf.len()
    }

    /// Get the total number of bytes uploaded so far.
    pub(crate) fn upload_size(&self) -> usize {
        self.upload_state.upload_size()
    }

    /// Get the number of parts that have been uploaded so far.
    pub(crate) fn num_parts(&self) -> usize {
        self.upload_state.num_parts()
    }

    /// Clone the state's `UploadedParts`.
    pub(crate) fn uploaded_parts(&self) -> UploadedParts {
        self.upload_state.uploaded_parts().clone()
    }

    // We want to create a `BytesMut` with some specified capacity because that
    // avoids reallocations.  But there will always be slightly more bytes
    // written than `target_part_size` so that wouldn't be a good capacity
    // either. `1.5 * target_part_size` is a reasonable figure.
    fn init_buf(target: usize) -> BytesMut {
        let capacity = (1.5 * (target as f64)) as usize;
        BytesMut::with_capacity(capacity)
    }

    fn new_buf(&self) -> BytesMut {
        Self::init_buf(self.ctrl.target_part_size())
    }

    fn poll_upload(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        // Replace the current `BytesMut` buffer with a new one with the same
        // capacity.
        // We need an owned `BytesMut` to call `freeze()` and turn it into a
        // `ByteStream` as the AWS SDK expects.
        let new_buf = self.new_buf();
        let buf = std::mem::replace(&mut self.as_mut().buf, new_buf);
        let part = ByteStream::from(buf.freeze());

        let params = self.params_ref();
        let part_number = self.upload_state.current_part_number();
        let part_size = self.part_size();
        tracing::trace!(?params, part_number, part_size, "uploading part");

        let etag = ready!(self
            .client
            .upload_part(params, part_number, part)
            .as_mut()
            .poll(cx))?;
        self.as_mut().upload_state.update_state(part_size, etag);
        tracing::trace!(upload_state = ?self.upload_state, "updated upload state");

        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for WriteParts {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, IoError>> {
        let should_upload_part = self.should_upload_part();
        self.as_mut().buf.extend_from_slice(buf);
        tracing::trace!(
            buf_size = buf.len(),
            part_size = self.buf.len(),
            "wrote to buffer"
        );

        if should_upload_part {
            tracing::trace!(upload_state = ?self.get_upload_state_ref(), "flushing to upload part");
            ready!(self.poll_flush(cx))?;
        }

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        ready!(self.poll_upload(cx))?;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        ready!(self.poll_upload(cx))?;
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct UploadState {
    upload_size: usize,
    uploaded_parts: UploadedParts,
}

impl UploadState {
    fn update_state(&mut self, part_size: usize, etag: EntityTag) {
        self.upload_size += part_size;
        self.uploaded_parts.update(etag);
    }

    fn upload_size(&self) -> usize {
        self.upload_size
    }

    fn uploaded_parts(&self) -> &UploadedParts {
        &self.uploaded_parts
    }

    fn num_parts(&self) -> usize {
        self.uploaded_parts.num_parts()
    }

    fn current_part_number(&self) -> i32 {
        self.uploaded_parts.current()
    }
}
