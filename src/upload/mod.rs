use crate::client::UploadClient;
use crate::client::part::{CompletedParts, PartBody};
use crate::client::request::{CompletedUpload, SendUploadPart};
use crate::codec::PartEncoder;
use crate::error::Error as UploadError;
use crate::uri::{NewObjectUri, ObjectUri};

use multipart_write::MultipartWrite;

mod encoder;
pub use self::encoder::{Status, WithPartEncoder};

mod part_buffer;
pub use self::part_buffer::PartBuffer;

mod uploader;
pub use self::uploader::{UploadSent, Uploader, UploaderWithUri};

/// Extension of `MultipartWrite` to AWS S3 multipart uploads.
pub trait MultipartUploadWriterExt<Part>
where
    Self: MultipartWrite<Part, Error = UploadError>,
{
    /// A single multipart upload using this writer to buffer requests.
    ///
    /// This writer is one-time-use; it becomes terminated after the upload to
    /// `uri` is completed.
    fn uploader(self, client: &UploadClient, uri: ObjectUri) -> UploaderWithUri<Self>
    where
        Self: MultipartWrite<SendUploadPart, Output = CompletedParts, Error = UploadError> + Sized,
    {
        UploaderWithUri::new_with_uri(self, client, uri)
    }

    /// Repeated multipart uploads using this writer to buffer requests.
    ///
    /// This writer uses the iterator `NewObjectUri` to continue to create new
    /// uploads as the previous one finishes.
    fn repeat_uploader(self, client: &UploadClient, iter: NewObjectUri) -> UploaderWithUri<Self>
    where
        Self: MultipartWrite<SendUploadPart, Output = CompletedParts, Error = UploadError> + Sized,
    {
        UploaderWithUri::new(self, client, iter)
    }

    /// Transform this multipart upload by wrapping it with a `PartEncoder` over
    /// the `P` type, resulting in a new multipart upload whose part upload
    /// request body is sourced by the part encoder.
    fn with_part_encoder<P, E>(
        self,
        builder: E::Builder,
        part_size: Option<usize>,
    ) -> WithPartEncoder<E, E::Builder, Self>
    where
        Self: MultipartWrite<
                PartBody,
                Ret = UploadSent,
                Output = CompletedUpload,
                Error = UploadError,
            > + Sized,
        E: PartEncoder<P>,
    {
        WithPartEncoder::new(self, builder, part_size.into())
    }
}

impl<Part, Wr> MultipartUploadWriterExt<Part> for Wr where
    Wr: MultipartWrite<Part, Error = UploadError>
{
}
