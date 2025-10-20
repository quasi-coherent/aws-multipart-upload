use aws_sdk_s3::primitives::ByteStream;
use bytes::BytesMut;
use std::io::{Result as IoResult, Write};
use std::ops::{Deref, DerefMut};

pub mod api;
mod types;
pub use types::*;

/// `UploadData` is an active `UploadId` with the S3 bucket and object key that
/// it was created with.
#[derive(Debug, Clone, Default)]
pub struct UploadData {
    /// The ID for the upload assigned by AWS.
    pub id: UploadId,
    /// The S3 URI of the object being uploaded.
    pub uri: ObjectUri,
}

impl UploadData {
    /// Create a new value from an upload ID and object URI.
    pub fn new(id: UploadId, uri: ObjectUri) -> Self {
        Self { id, uri }
    }

    /// Get an owned upload ID.
    pub fn get_id(&self) -> UploadId {
        self.id.clone()
    }

    /// Get an owned object URI.
    pub fn get_uri(&self) -> ObjectUri {
        self.uri.clone()
    }

    /// Dereferenced bucket name.
    pub fn deref_bucket(&self) -> &str {
        &*self.uri.bucket
    }

    /// Dereferenced object key.
    pub fn deref_key(&self) -> &str {
        &*self.uri.key
    }
}

/// The value for a successful multipart upload.
#[derive(Debug, Clone)]
pub struct CompletedUpload {
    /// The URI of the created object.
    pub uri: ObjectUri,
    /// The entity tag of the created object.
    pub etag: EntityTag,
}

impl CompletedUpload {
    /// Create a new value from object URI and entity tag.
    pub fn new(uri: ObjectUri, etag: EntityTag) -> Self {
        Self { uri, etag }
    }
}

/// The value for a successful part upload to a multipart upload.
#[derive(Debug, Clone)]
pub struct CompletedPart {
    /// The entity tag of the uploaded part is an opaque string identifying the
    /// object in AWS.
    pub etag: EntityTag,
    /// The incrementing integer starting with 1 that we assigned to this part
    /// when initiating the part upload.
    pub part_number: PartNumber,
    /// The size of the part that was uploaded.
    pub part_size: usize,
}

impl CompletedPart {
    /// Create a new value from entity tag and part number used in the upload.
    pub fn new(etag: EntityTag, part_number: PartNumber, part_size: usize) -> Self {
        Self {
            etag,
            part_number,
            part_size,
        }
    }
}

/// All completed part uploads for a multipart upload.
#[derive(Debug, Clone, Default)]
pub struct CompletedParts(Vec<CompletedPart>);

impl CompletedParts {
    /// Add a new [`CompletedPart`] to this collection.
    pub fn push(&mut self, part: CompletedPart) {
        self.0.push(part);
    }

    /// Extend this [`CompletedParts`] by another.
    pub fn extend(&mut self, other: CompletedParts) {
        self.0.extend(other.0);
    }

    /// Returns the number of parts that have been successfully uploaded.
    pub fn count(&self) -> usize {
        self.0.len()
    }

    /// Returns the current size in bytes of this upload.
    pub fn size(&self) -> usize {
        self.0.iter().fold(0, |n, p| p.part_size + n)
    }
}

impl Deref for CompletedParts {
    type Target = [CompletedPart];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&CompletedParts> for aws_sdk_s3::types::CompletedMultipartUpload {
    fn from(value: &CompletedParts) -> Self {
        let completed_parts = value.0.iter().fold(Vec::new(), |mut acc, v| {
            acc.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .e_tag(v.etag.to_string())
                    .part_number(*v.part_number)
                    .build(),
            );

            acc
        });

        aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build()
    }
}

/// A part body in a multipart upload request.
///
/// # Examples
///
/// `PartBody` can be created from an existing [`BytesMut`].  It can also be
/// created incrementally using its [`Write`] implementation.
///
/// ```rust
/// use aws_multipart_upload::sdk::PartBody;
/// use std::io::Write as _;
///
/// let mut body = PartBody::default();
/// let data = b"some data for a part";
/// let _ = body.write(data).unwrap();
/// ```
///
/// [`Write`]: std::io::Write
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartBody(BytesMut);

impl PartBody {
    /// Construct a body from [`BytesMut`].
    pub fn new(bytes: BytesMut) -> Self {
        Self(bytes)
    }

    /// Returns an empty [`PartBody`] to write to that has a pre-allocated
    /// capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let bytes = BytesMut::with_capacity(capacity);
        Self(bytes)
    }

    /// Size in bytes of the body.
    pub fn size(&self) -> usize {
        self.0.len()
    }

    /// Convert this type into a [`ByteStream`], which is the type required by
    /// the SDK to make a `CompleteMultipartUpload` request to AWS.
    ///
    /// This conversion is zero-cost as it only involves methods that do a
    /// ref-count increment.
    pub fn as_sdk_body(&mut self) -> ByteStream {
        let buf = self.split();
        let bytes = buf.freeze();
        bytes.into()
    }
}

impl Write for PartBody {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let len = buf.len();
        self.extend_from_slice(buf);
        Ok(len)
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl From<BytesMut> for PartBody {
    fn from(value: BytesMut) -> Self {
        Self(value)
    }
}

impl Deref for PartBody {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PartBody {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl AsRef<[u8]> for PartBody {
    fn as_ref(&self) -> &[u8] {
        self.deref().as_ref()
    }
}
