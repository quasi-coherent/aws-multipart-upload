use super::UploadId;
use crate::complete_upload::CompleteMultipartUploadOutput as CompleteResponse;
use crate::error::{ErrorRepr, Result};
use crate::part_upload::UploadPartOutput as UploadResponse;

use aws_sdk_s3::primitives::ByteStream;
use bytes::{BufMut as _, BytesMut};
use std::borrow::Cow;
use std::fmt::{self, Display, Formatter};
use std::io::{Result as IoResult, Write};
use std::ops::{Deref, DerefMut};

/// Body of the multipart upload request.
///
/// This type dereferences to [`BytesMut`], so in particular supports the methods
/// of [`BufMut`], which is the preferred way of writing data to a `PartBody`.
///
/// `PartBody` also implements [`Write`], so it can also be used in combination
/// with the class of external writer types that are parametrized by `Write`.
///
/// [`BufMut`]: bytes::BufMut
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartBody(BytesMut);

impl PartBody {
    /// Construct a body from [`BytesMut`].
    pub fn new(bytes: BytesMut) -> Self {
        Self(bytes)
    }

    /// Returns an empty `PartBody` to write to that has pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let bytes = BytesMut::with_capacity(capacity);
        Self(bytes)
    }

    /// Current size in bytes of the `PartBody`.
    pub fn size(&self) -> usize {
        self.0.len()
    }

    /// Convert this type into a [`ByteStream`], which is the type required by
    /// the SDK in the request to AWS to add a part to a multipart upload.
    ///
    /// This conversion is zero-cost as it only involves methods that do a
    /// ref-count increment on the inner byte buffer structure.
    pub fn as_sdk_body(&mut self) -> ByteStream {
        let buf = self.split();
        let bytes = buf.freeze();
        bytes.into()
    }
}

impl Write for PartBody {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let bytes = buf.len();
        self.reserve(bytes);
        self.put(buf);
        Ok(bytes)
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

/// Number we assign to a part when uploading.
///
/// This, along with the entity tag found in the response, is required in the
/// request to complete a multipart upload because it identifies the where the
/// part goes when assembling the full object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartNumber(i32);

impl Default for PartNumber {
    fn default() -> Self {
        Self(1)
    }
}

impl PartNumber {
    /// Create a new `PartNumber` from a plain integer.
    ///
    /// Note that new uploads are required to start with a part number of 1,
    /// which is how `PartNumber: Default`.
    ///
    /// With a handle on a current upload, [`increment`](PartNumber::increment)
    /// should be used to create the next `PartNumber` when one has just been
    /// added.
    ///
    /// Otherwise, use this when resuming a previous, partial upload.
    pub fn new(n: i32) -> Self {
        Self(n)
    }

    /// Increment the `PartNumber` by 1, returning the previous part number.
    pub fn increment(&mut self) -> PartNumber {
        self.0 += 1;
        PartNumber(self.0 - 1)
    }
}

impl Deref for PartNumber {
    type Target = i32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PartNumber {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Display for PartNumber {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "part_{}", self.0)
    }
}

/// AWS entity tag.
///
/// This value is a hash of an object. It is assigned to an uploaded part and
/// returned in the response from a part upload request.
///
/// It is also assigned to a completed upload and found in a successful complete
/// upload response.
#[derive(Debug, Clone, Default)]
pub struct EntityTag(Cow<'static, str>);

impl EntityTag {
    fn new<T: Into<Cow<'static, str>>>(etag: T) -> Self {
        Self(etag.into())
    }

    pub(crate) fn try_from_upload_resp(value: &UploadResponse) -> Result<Self, ErrorRepr> {
        value
            .e_tag
            .as_deref()
            .map(Self::from)
            .ok_or_else(|| ErrorRepr::Missing("UploadResponse", "e_tag"))
    }

    pub(crate) fn try_from_complete_resp(value: &CompleteResponse) -> Result<Self, ErrorRepr> {
        value
            .e_tag
            .as_deref()
            .map(Self::from)
            .ok_or_else(|| ErrorRepr::Missing("CompleteResponse", "e_tag"))
    }
}

impl Deref for EntityTag {
    type Target = str;

    fn deref(&self) -> &str {
        self.0.deref()
    }
}

impl AsRef<str> for EntityTag {
    fn as_ref(&self) -> &str {
        self.deref()
    }
}

impl Display for EntityTag {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for EntityTag {
    fn from(value: &str) -> Self {
        Self::new(value.to_string())
    }
}

impl From<String> for EntityTag {
    fn from(value: String) -> Self {
        Self(Cow::Owned(value))
    }
}

/// The value for a successful part upload request.
///
/// All `CompletedPart`s need to be retained in order to construct a valid
/// complete upload request.
#[derive(Debug, Clone)]
pub struct CompletedPart {
    /// The ID of the upload this part was added to.
    pub id: UploadId,
    /// The entity tag of the uploaded part is a hash of the object in S3 that
    /// was created by uploading this part.
    pub etag: EntityTag,
    /// The incrementing integer starting with 1 that identifies this part in the
    /// part upload.
    pub part_number: PartNumber,
    /// The size of this part in bytes.
    pub part_size: usize,
}

impl CompletedPart {
    /// Create a new value from entity tag and part number used in the upload.
    pub fn new(id: UploadId, etag: EntityTag, part_number: PartNumber, part_size: usize) -> Self {
        Self {
            id,
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

    /// Extend this `CompletedParts` by the values from another.
    pub fn extend(&mut self, other: CompletedParts) {
        self.0.extend(other.0);
        self.sort_ascending();
    }

    /// Returns the number of parts that have been successfully uploaded.
    pub fn count(&self) -> usize {
        self.0.len()
    }

    /// Returns the current size in bytes of this upload.
    pub fn size(&self) -> usize {
        self.0.iter().map(|p| p.part_size).sum()
    }

    /// Get the largest part number assigned, which ordinarily is the most
    /// recently uploaded part.
    pub fn max_part_number(&self) -> PartNumber {
        match self.0.iter().max_by_key(|p| p.part_number) {
            Some(part) => part.part_number,
            _ => PartNumber::default(),
        }
    }

    /// Sort the `CompletedPart`s in increasing order by part number.
    ///
    /// It is an error to make a request where the completed parts are not in
    /// order.
    pub fn sort_ascending(&mut self) {
        self.sort_by_key(|part| part.part_number);
    }
}

impl Deref for CompletedParts {
    type Target = [CompletedPart];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for CompletedParts {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
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
