use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::client::part::{EntityTag, PartBody};
use crate::client::{UploadApi, UploadClient};
use crate::codec::PartEncoder;
use crate::error::Error as UploadError;
use crate::uri::{EmptyUri, ObjectUri, ObjectUriIter, OneTimeUse};

use bytesize::ByteSize;
use multipart_write::{FusedMultipartWrite, MultipartWrite};

mod part_buffer;
pub mod stream;
mod upload_impl;

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
pub(crate) const AWS_MAX_OBJECT_SIZE: ByteSize = ByteSize::gib(48800);
pub(crate) const AWS_MIN_PART_SIZE: ByteSize = ByteSize::mib(5);
pub(crate) const AWS_MAX_PART_SIZE: ByteSize = ByteSize::gib(5);
pub(crate) const DEFAULT_MAX_OBJECT_SIZE: ByteSize = ByteSize::mib(128);
pub(crate) const DEFAULT_MAX_PART_SIZE: ByteSize = ByteSize::mib(10);

/// `Upload` is a type for asynchronously building a multipart upload.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project]
pub struct Upload<E = PartBody> {
    #[pin]
    inner: upload_impl::UploadImpl<E>,
}

impl Upload {
    /// Return a builder for this type from an `UploadApi` client.
    pub fn builder<C: UploadApi + 'static>(client: C) -> UploadBuilder {
        UploadBuilder::new(client)
    }
}

impl<E> Upload<E> {
    fn new(inner: upload_impl::UploadImpl<E>) -> Self {
        Self { inner }
    }
}

impl<Item, E: PartEncoder<Item>> FusedMultipartWrite<Item> for Upload<E> {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

impl<Item, E: PartEncoder<Item>> MultipartWrite<Item> for Upload<E> {
    type Error = UploadError;
    type Output = Uploaded;
    type Recv = UploadStatus;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx)
    }

    fn start_send(
        self: Pin<&mut Self>,
        part: Item,
    ) -> Result<Self::Recv, Self::Error> {
        self.project().inner.start_send(part).map(UploadStatus::new)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::Output, Self::Error>> {
        self.project().inner.poll_complete(cx).map(|out| out.map(Uploaded::new))
    }
}

/// `UploadBuilder` builds an `Upload`.
#[derive(Debug)]
pub struct UploadBuilder<E = PartBody> {
    client: UploadClient,
    max_bytes: ByteSize,
    max_part_bytes: ByteSize,
    encoder: E,
    iter: ObjectUriIter,
    capacity: Option<usize>,
}

impl UploadBuilder {
    /// New `UploadBuilder` with defaults.
    pub fn new<C>(client: C) -> Self
    where
        C: UploadApi + 'static,
    {
        Self {
            client: UploadClient::new(client),
            max_bytes: DEFAULT_MAX_OBJECT_SIZE,
            max_part_bytes: DEFAULT_MAX_PART_SIZE,
            encoder: PartBody::default(),
            iter: ObjectUriIter::new(EmptyUri),
            capacity: Some(10),
        }
    }

    /// Set an encoder that parts will be written with.
    pub fn with_encoder<E>(self, encoder: E) -> UploadBuilder<E> {
        UploadBuilder {
            client: self.client,
            max_bytes: self.max_bytes,
            max_part_bytes: self.max_part_bytes,
            encoder,
            iter: self.iter,
            capacity: self.capacity,
        }
    }
}

impl<E> UploadBuilder<E> {
    /// Set the target size of the upload. The maximum is 48.8TiB and the
    /// default is 128MiB.
    ///
    /// The reason for the choice is it has to be something, and this was a good
    /// rule of thumb for block size in Hadoop HDFS.
    pub fn with_upload_size(self, limit: ByteSize) -> Self {
        Self { max_bytes: limit.min(AWS_MAX_OBJECT_SIZE), ..self }
    }

    /// Set the target size of a part.  This has to be between 5MiB and 5GiB;
    /// the default is 10MiB.
    pub fn with_part_size(self, limit: ByteSize) -> Self {
        Self {
            // Clamp to AWS_MIN <= max_part_bytes <= min(AWS_MAX, usize::MAX).
            max_part_bytes: limit
                .max(AWS_MIN_PART_SIZE)
                .min(AWS_MAX_PART_SIZE)
                .min(ByteSize::b(usize::MAX as u64)),
            ..self
        }
    }

    /// Set the destination object URI for a single upload.
    ///
    /// The resulting [`Upload`] can be used only once.
    pub fn with_uri<T: Into<ObjectUri>>(self, uri: T) -> Self {
        let inner = OneTimeUse::new(uri.into());
        Self { iter: ObjectUriIter::new(inner), ..self }
    }

    /// Set the destination object URI to be generated using the provided
    /// iterator.
    ///
    /// The resulting [`Upload`] will be reusable for as long as the iterator
    /// can produce the next `ObjectUri`.
    pub fn with_uri_iter<I>(self, inner: I) -> Self
    where
        I: IntoIterator<Item = ObjectUri> + 'static,
    {
        let iter = ObjectUriIter::new(inner);
        Self { iter, ..self }
    }

    /// Set a limit to the number of active part upload requests that can exist
    /// at one time.
    ///
    /// `None` or `Some(0)` is interpreted as "unlimited" capacity.  By
    /// arbitrary choice the default is 10.
    pub fn with_capacity<T: Into<Option<usize>>>(self, capacity: T) -> Self {
        Self { capacity: capacity.into(), ..self }
    }

    /// Build the `Upload` from this configuration.
    pub fn build(self) -> Upload<E> {
        let params = upload_impl::UploadParams::new(
            self.max_bytes.as_u64(),
            self.max_part_bytes.as_u64(),
            self.capacity,
        );
        let inner = upload_impl::UploadImpl::new(
            self.client,
            self.encoder,
            self.iter,
            params,
        );
        Upload::new(inner)
    }
}

/// The value returned in a successful multipart upload.
#[derive(Debug, Clone)]
pub struct Uploaded {
    /// The S3 object URI of the completed upload.
    pub uri: ObjectUri,
    /// The entity tag of the uploaded object.
    pub etag: EntityTag,
    /// The size in bytes of the upload.
    pub bytes: u64,
    /// The number of parts the upload was comprised of.
    pub parts: u64,
    /// The total number of items written to the upload.
    pub items: u64,
    /// The duration of the upload.
    pub duration: Duration,
}

impl Uploaded {
    fn new(output: upload_impl::UploadOutput) -> Self {
        Self {
            uri: output.completed.uri,
            etag: output.completed.etag,
            bytes: output.bytes,
            parts: output.parts,
            items: output.items,
            duration: output.duration,
        }
    }
}

/// The value returned on a successfully written part.
#[derive(Debug, Clone, Copy, Default)]
pub struct UploadStatus {
    /// Total uptime of the current upload.
    pub duration: Duration,
    /// Total bytes written.
    pub total_bytes: u64,
    /// Bytes written to the current part.
    pub part_bytes: u64,
    /// Total bytes in parts successfully added to the upload.
    pub upload_bytes: u64,
    /// Total number of items written.
    pub total_items: u64,
    /// Items written to the current part.
    pub part_items: u64,
    /// Total number of parts built.
    pub total_parts: u64,
    /// Total parts successfully added to the upload.
    pub upload_parts: u64,
    /// Whether the current part should be completed given the supplied target
    /// part size.
    pub should_complete_part: bool,
    /// Whether the upload should be completed given the supplied target size.
    pub should_complete_upload: bool,
}

impl UploadStatus {
    fn new(state: upload_impl::UploadState) -> Self {
        Self {
            duration: state.duration,
            total_bytes: state.total_bytes,
            part_bytes: state.part_bytes,
            upload_bytes: state.upload_bytes,
            total_items: state.total_items,
            part_items: state.part_items,
            total_parts: state.total_parts,
            upload_parts: state.upload_parts,
            should_complete_part: state.is_part_complete(),
            should_complete_upload: state.is_upload_complete(),
        }
    }
}
