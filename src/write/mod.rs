use std::time::Duration;

use futures::Stream;
use multipart_write::stream::{
    CompleteWith, MultipartStreamExt as _, TryCompleteWhen,
};
use multipart_write::{FusedMultipartWrite, MultipartWrite};

use self::multipart_upload::UploadStatus;
use crate::client::part::EntityTag;
use crate::uri::ObjectUri;

pub mod multipart_upload;
mod part_buffer;
pub mod with_part_encoder;

/// Value returned by multipart uploads when polled for completion.
#[derive(Debug, Clone)]
pub struct Uploaded {
    /// The uploaded object's URI.
    pub uri: ObjectUri,
    /// The entity tag of the uploaded object.
    pub etag: EntityTag,
    /// Size in bytes of the upload.
    pub bytes: u64,
    /// Number of parts that went into the upload.
    pub parts: u64,
    /// Number of items that went into the upload, if known.
    pub items: Option<u64>,
    /// Total duration of the multipart upload.
    pub duration: Duration,
}

/// Whether the upload should be completed.
pub trait ShouldComplete {
    /// Return `true` if the upload should be completed.
    fn should_complete(&self) -> bool;
}

/// Future that consumes a stream in its entirety by adding it in parts to a
/// multipart upload, completing the upload when the stream is exhaused.
pub type CollectUpload<St, U> = CompleteWith<St, U>;

/// Stream that writes its input to a multipart upload and completes it when the
/// condition is met.
pub type TryUploadWhen<St, U, F> = TryCompleteWhen<St, U, F>;

/// Extension of `Stream` by methods for uploading it.
pub trait UploadStreamExt: Stream {
    /// Future that writes a stream in parts to an upload `U`, completing the
    /// upload when the stream is exhausted.
    fn collect_upload<U>(self, upload: U) -> CollectUpload<Self, U>
    where
        Self: Sized,
        U: MultipartWrite<Self::Item>,
    {
        self.complete_with(upload)
    }

    /// Tranforms this stream by writing its items as parts to an upload `U`
    /// with return value `UploadStatus`.
    ///
    /// The resulting stream produces an item from the result of completing `U`
    /// when the status indicates the upload has reached the target size.
    fn try_upload<U, R>(
        self,
        upload: U,
    ) -> TryUploadWhen<Self, U, fn(R) -> bool>
    where
        Self: Sized,
        U: FusedMultipartWrite<Self::Item, Recv = R>,
        R: ShouldComplete,
    {
        self.try_complete_when(upload, |r| r.should_complete())
    }

    /// Tranforms this stream by writing its items as parts to an upload `U`.
    ///
    /// Like [`try_upload`](Self::try_upload) except the predicate `F` is not
    /// prescribed.
    fn try_upload_when<U, F>(
        self,
        uploader: U,
        f: F,
    ) -> TryUploadWhen<Self, U, F>
    where
        Self: Sized,
        U: FusedMultipartWrite<Self::Item>,
        F: FnMut(U::Recv) -> bool,
    {
        self.try_complete_when(uploader, f)
    }
}

impl<St: Stream> UploadStreamExt for St {}
