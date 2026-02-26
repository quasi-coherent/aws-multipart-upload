use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::io::{Result as IoResult, Write};
use std::ops::{Deref, DerefMut};

use aws_sdk_s3::primitives::ByteStream;
use bytes::{BufMut as _, BytesMut};

use crate::complete_upload::CompleteMultipartUploadOutput as CompleteResponse;
use crate::error::{ErrorRepr, Result};
use crate::part_upload::UploadPartOutput as UploadResponse;

/// Body of the multipart upload request.
///
/// This type dereferences to [`BytesMut`], so in particular supports the
/// methods of [`BufMut`], which is the preferred way of writing data to a
/// `PartBody`.
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
///
/// The `Default` behavior is to start with the part number `1`.  This is
/// because it is a requirement of the API that a part number be an integer
/// between 1 and 10,000.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartNumber(i32);

impl PartNumber {
    /// Initializes the part number at 1, which is a requirement of the API.
    pub fn new() -> Self {
        Self(1)
    }

    /// Create a new `PartNumber` starting with `n`.
    ///
    /// Used when resuming an upload that was suspended with completed parts.
    ///
    /// For a new upload, use [`Self::new`].
    pub fn start_with(n: i32) -> Self {
        Self(n)
    }

    /// Returns whether this is valid to AWS API.
    pub fn is_valid(&self) -> bool {
        let PartNumber(n) = self;
        *n > 0
    }

    /// Increment the `PartNumber` by 1, returning the previous part number.
    pub fn fetch_incr(&mut self) -> PartNumber {
        let curr = PartNumber(self.0);
        self.0 += 1;
        curr
    }
}

impl Default for PartNumber {
    fn default() -> Self {
        Self(1)
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
/// This value is a hash of an object on S3; it is the canonical identifier in
/// AWS of the object and its contents.
///
/// A part in an upload has an e-tag, returned when the part was added
/// successfully.  In this case, it is critical that the e-tag be retained
/// alongside the [`PartNumber`] that it corresponded to.  These are needed in
/// completing the upload.
///
/// It is also assigned to a completed upload, but more generally any S3 object.
/// upload response.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntityTag(Cow<'static, str>);

impl EntityTag {
    fn new<T: Into<Cow<'static, str>>>(etag: T) -> Self {
        Self(etag.into())
    }

    pub(crate) fn try_from_upload_resp(
        value: &UploadResponse,
    ) -> Result<Self, ErrorRepr> {
        value
            .e_tag
            .as_deref()
            .map(Self::from)
            .ok_or_else(|| ErrorRepr::Missing("UploadResponse", "e_tag"))
    }

    pub(crate) fn try_from_complete_resp(
        value: &CompleteResponse,
    ) -> Result<Self, ErrorRepr> {
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
        &self.0
    }
}

impl AsRef<str> for EntityTag {
    fn as_ref(&self) -> &str {
        self
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
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct CompletedPart {
    /// The etag of the object part in S3.
    pub etag: EntityTag,
    /// The incrementing integer starting with 1 that identifies this part in
    /// the part upload.
    pub part_number: PartNumber,
    /// The size of this part in bytes.
    pub part_size: usize,
}

impl CompletedPart {
    /// Create a new value.
    pub fn new(
        etag: EntityTag,
        part_number: PartNumber,
        part_size: usize,
    ) -> Self {
        Self { etag, part_number, part_size }
    }
}

/// Collection of completed part uploads.
#[derive(Debug, Clone, Default)]
pub struct CompletedParts {
    parts: BTreeMap<PartNumber, CompletedPart>,
    total_bytes: usize,
}

impl CompletedParts {
    /// Add a new `CompletedPart` to the collection, skipping the operation if
    /// the part number already exists.
    pub fn insert(&mut self, part: CompletedPart) {
        let k = part.part_number;
        if !self.parts.contains_key(&k) {
            self.total_bytes += part.part_size;
            self.parts.insert(k, part);
        }
    }

    /// Moves all completed parts from `other` into `self`, skipping the
    /// operation if the part number already exists.
    pub fn append(&mut self, other: CompletedParts) {
        other.parts.into_values().for_each(|v| {
            self.insert(v);
        })
    }

    /// Returns the number of parts that have been successfully uploaded.
    pub fn count(&self) -> usize {
        self.parts.len()
    }

    /// Returns the current size in bytes of this upload.
    pub fn size(&self) -> usize {
        self.total_bytes
    }
}

impl From<&CompletedParts> for aws_sdk_s3::types::CompletedMultipartUpload {
    fn from(vs: &CompletedParts) -> Self {
        let parts = vs.parts.values().fold(Vec::new(), |mut acc, v| {
            acc.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .e_tag(&*v.etag)
                    .part_number(*v.part_number)
                    .build(),
            );
            acc
        });
        aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build()
    }
}
