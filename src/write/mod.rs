//! A collection of `MultipartWrite` implementations for multipart uploads.
//!
//! This module contains the [`MultipartWrite`] implementations [`Upload`] and
//! [`EncodedUpload`], components for building multipart writers like them, and
//! extension traits for `MultipartWrite` and `Stream` providing useful
//! combinator methods supporting multipart uploads.
use crate::client::UploadClient;
use crate::client::part::{CompletedParts, PartBody};
use crate::client::request::{CompletedUpload, SendUploadPart};
use crate::codec::PartEncoder;
use crate::error::Error as UploadError;
use crate::uri::ObjectUriIter;

use bytesize::ByteSize;
use futures::Stream;
use multipart_write::stream::{Assemble, Assembled};
use multipart_write::{FusedMultipartWrite, MultipartStreamExt as _, MultipartWrite};

mod encoded;
pub use self::encoded::{EncodedUpload, Status};

mod part_buffer;
pub use self::part_buffer::PartBuffer;

mod upload;
pub use self::upload::{Upload, UploadSent};

/// A type for creating, building, and completing a multipart upload.
pub type MultipartUpload<Item, E> = EncodedUpload<Item, E, Upload<PartBuffer>>;

/// Trait alias for a general form of `MultipartUpload`.
pub trait AwsMultipartUpload<Item>
where
    Self: FusedMultipartWrite<Item, Ret = Status, Error = UploadError, Output = CompletedUpload>,
{
}

impl<Item, E: PartEncoder<Item>> AwsMultipartUpload<Item> for MultipartUpload<Item, E> {}

/// Extension trait for `MultipartWrite` adding specializations for S3 uploads.
pub trait UploadWriteExt<Part>: MultipartWrite<Part> {
    /// Returns a new `MultipartWrite` that uploads to a multipart upload, using
    /// this writer as a buffer for request futures.
    fn upload(self, client: &UploadClient, iter: ObjectUriIter) -> Upload<Self>
    where
        Self: MultipartWrite<SendUploadPart, Error = UploadError, Output = CompletedParts> + Sized,
    {
        Upload::new(self, client, iter)
    }

    /// Transform this writer into one that takes an arbitrary input type and
    /// uses the encoder over this type to produce the input for a multipart
    /// upload.
    fn encoded_upload<P, E>(
        self,
        builder: E::Builder,
        bytes: ByteSize,
        part_bytes: ByteSize,
    ) -> EncodedUpload<P, E, Self>
    where
        Self: MultipartWrite<
                PartBody,
                Ret = UploadSent,
                Error = UploadError,
                Output = CompletedUpload,
            > + Sized,
        E: PartEncoder<P>,
    {
        EncodedUpload::new(self, builder, bytes.as_u64(), part_bytes.as_u64())
    }
}

impl<Part, Wr: MultipartWrite<Part>> UploadWriteExt<Part> for Wr {}

/// Future for the result of collecting a stream into a multipart upload.
pub type CollectUpload<St, U> = Assemble<St, U>;

/// Stream of results from sending an input stream to a multipart upload.
pub type IntoUpload<St, U, F> = Assembled<St, U, F>;

/// Extension of `Stream` by methods for uploading it.
pub trait UploadStreamExt: Stream {
    /// Collect this stream into a multipart upload, returning the result of
    /// completing the upload in a future.
    fn collect_upload<U>(self, uploader: U) -> CollectUpload<Self, U>
    where
        Self: Sized,
        U: FusedMultipartWrite<Self::Item, Error = UploadError, Output = CompletedUpload>,
    {
        self.assemble(uploader)
    }

    /// Transform the input stream by writing its items to the uploader `U`,
    /// producing the next item in the stream by completing the upload when the
    /// status indicates the upload is complete.
    ///
    /// The resulting stream ends when either the input stream is exhausted or
    /// the uploader is unable to start the next upload after producing an item.
    fn into_upload<U>(self, uploader: U) -> IntoUpload<Self, U, fn(&Status) -> bool>
    where
        Self: Sized,
        U: FusedMultipartWrite<
                Self::Item,
                Ret = Status,
                Error = UploadError,
                Output = CompletedUpload,
            >,
    {
        self.assembled(uploader, |status| status.should_complete)
    }

    /// Transform the input stream by writing its items to the uploader `U`,
    /// producing the next item in the stream by completing the upload when the
    /// given closure returns true.
    fn into_upload_when<U, F>(self, uploader: U, f: F) -> IntoUpload<Self, U, F>
    where
        Self: Sized,
        U: FusedMultipartWrite<Self::Item, Error = UploadError, Output = CompletedUpload>,
        F: FnMut(&U::Ret) -> bool,
    {
        self.assembled(uploader, f)
    }
}

impl<St: Stream> UploadStreamExt for St {}
