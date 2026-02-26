//! Stream combinators for multipart uploads.
//!
//! The module provides the stream extension [`UploadStreamExt`] that adds
//! methods for uploading a stream in parts.
use futures::Stream;
use multipart_write::FusedMultipartWrite;
use multipart_write::stream::{
    CompleteWith, MultipartStreamExt as _, TryCompleteWhen,
};

use super::UploadStatus;

/// Future that consumes a stream in its entirety by adding it in parts to a
/// multipart upload, completing the upload when the stream is exhaused.
pub type CollectUpload<St, U> = CompleteWith<St, U>;

/// Stream that writes its input to a multipart upload and completes it when
/// `should_complete_upload` is `true` in the return value `UploadStatus`.
pub type TryUpload<St, U> = TryCompleteWhen<St, U, fn(UploadStatus) -> bool>;

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
        U: FusedMultipartWrite<Self::Item>,
    {
        self.complete_with(upload)
    }

    /// Tranforms this stream by writing its items as parts to an upload `U`
    /// with return value `UploadStatus`.
    ///
    /// The resulting stream produces an item from the result of completing `U`
    /// when the status indicates the upload has reached the target size.
    fn try_upload<U>(self, upload: U) -> TryUpload<Self, U>
    where
        Self: Sized,
        U: FusedMultipartWrite<Self::Item, Recv = UploadStatus>,
    {
        self.try_complete_when(upload, |status| status.should_complete_upload)
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
