//! Core types for multipart uploads.
//!
//! The module defines [`Upload`] and [`EncodeUpload`], which are values that
//! can perform the complete multipart upload, along with a builder for them.
use std::pin::Pin;
use std::task::{Context, Poll};

use bytesize::ByteSize;
use multipart_write::{FusedMultipartWrite, MultipartWrite};

use crate::client::part::PartBody;
use crate::client::{UploadApi, UploadClient};
use crate::encoder::PartEncoder;
use crate::error::Error;
use crate::uri::{EmptyUri, ObjectUri, ObjectUriIter, OneTimeUse};
use crate::write::multipart_upload::MultipartUpload;
use crate::write::with_part_encoder::WithPartEncoder;

pub use crate::write::multipart_upload::UploadStatus;
pub use crate::write::with_part_encoder::EncoderStatus;
pub use crate::write::{ShouldComplete, Uploaded};

pub mod stream {
    //! Extensions to `Stream` for multipart uploads.
    pub use crate::write::{CollectUpload, TryUploadWhen, UploadStreamExt};
}

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
pub(crate) const AWS_MAX_OBJECT_SIZE: ByteSize = ByteSize::gib(48800);
pub(crate) const AWS_MIN_PART_SIZE: ByteSize = ByteSize::mib(5);
pub(crate) const AWS_MAX_PART_SIZE: ByteSize = ByteSize::gib(5);
pub(crate) const DEFAULT_MAX_OBJECT_SIZE: ByteSize = ByteSize::mib(128);
pub(crate) const DEFAULT_MAX_PART_SIZE: ByteSize = ByteSize::mib(10);

/// `UploadBuilder` builds an `Upload`.
#[derive(Debug)]
pub struct UploadBuilder {
    client: UploadClient,
    max_bytes: ByteSize,
    max_part_bytes: ByteSize,
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
            iter: ObjectUriIter::new(EmptyUri.into_iter()),
            capacity: Some(10),
        }
    }

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
        I: Iterator<Item = ObjectUri> + Send + Sync + 'static,
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

    /// Build an `Upload` from this configuration.
    pub fn build(self) -> Upload {
        Upload::new(self.client, self.iter, self.max_bytes, self.capacity)
    }

    /// Build an `EncodeUpload` that constructs the multipart upload from
    /// `Item`s encoded with `E`.
    pub fn build_encoded<Item, E>(self, encoder: E) -> EncodeUpload<Item, E> {
        let bytes = self.max_bytes;
        let part_bytes = self.max_part_bytes;
        let upload = self.build();
        EncodeUpload::new(upload, encoder, bytes, part_bytes)
    }
}

/// `Upload` manages the lifecycle of an AWS S3 multipart upload.
///
/// This type realizes [`MultipartWrite`] through the following:
///
/// * `poll_ready` ensures that there is a valid, active multipart upload, and
///   that the number of pending part upload requests does not exceed configured
///   capacity.
/// * `start_send` accepts a [`PartBody`], creates a part upload request future
///   from it, and pushes it to a collection of these, where the responses are
///   resolved asynchronously and independently.  Current statistics of the
///   upload are returned in the value [`UploadStatus`].
/// * `poll_flush` awaits all pending part upload request futures, draining the
///   collection.
/// * `poll_complete` flushes and then submits the request to complete the
///   multipart upload, returning the response as the value [`Uploaded`].
///
/// On completion, a new upload is started if possible.  This relies on the
/// ability of the [`ObjectUriIter`] that it was created with to generate the
/// next upload URI.  Polling for readiness ensures that the new upload has
/// resolved to an upload ID before allowing `start_send`.
///
/// # Fusing
///
/// If [`ObjectUriIter::next`] ever produces `None`, the multipart upload will
/// transition to a terminated state and it is not polled again.
///
/// [`MultipartWrite`]: multipart_write::MultipartWrite
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project]
pub struct Upload {
    #[pin]
    inner: MultipartUpload,
}

impl Upload {
    fn new<T: Into<Option<usize>>>(
        client: UploadClient,
        iter: ObjectUriIter,
        upload_bytes: ByteSize,
        capacity: T,
    ) -> Self {
        let inner =
            MultipartUpload::new(client, iter, upload_bytes, capacity.into());
        Self { inner }
    }
}

impl FusedMultipartWrite<PartBody> for Upload {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

impl MultipartWrite<PartBody> for Upload {
    type Error = Error;
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
        part: PartBody,
    ) -> Result<Self::Recv, Self::Error> {
        self.project().inner.start_send(part)
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
        self.project().inner.poll_complete(cx)
    }
}

/// `EncodeUpload` manages the lifecycle of an AWS S3 multipart upload built
/// from values `Item` using the value `E` to encode them.
///
/// Whereas [`Upload`] can only accept a [`PartBody`] that is ready to be sent,
/// `EncodeUpload` builds the part from `Item`s first.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project]
pub struct EncodeUpload<Item, E> {
    #[pin]
    inner: WithPartEncoder<Upload, Item, E>,
}

impl<Item, E> EncodeUpload<Item, E> {
    fn new(
        upload: Upload,
        encoder: E,
        bytes: ByteSize,
        part_bytes: ByteSize,
    ) -> EncodeUpload<Item, E> {
        let inner = WithPartEncoder::new(upload, encoder, bytes, part_bytes);
        EncodeUpload { inner }
    }
}

impl<Item, E: PartEncoder<Item>> FusedMultipartWrite<Item>
    for EncodeUpload<Item, E>
{
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

impl<Item, E: PartEncoder<Item>> MultipartWrite<Item>
    for EncodeUpload<Item, E>
{
    type Error = Error;
    type Output = Uploaded;
    type Recv = EncoderStatus;

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
        self.project().inner.start_send(part)
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
        self.project().inner.poll_complete(cx)
    }
}
