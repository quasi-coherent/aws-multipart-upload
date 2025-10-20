use crate::complete_upload::CompleteMultipartUploadOutput as CompleteResponse;
use crate::create_upload::CreateMultipartUploadOutput as CreateResponse;
use crate::error::ErrorRepr;
use crate::upload_part::UploadPartOutput as UploadResponse;

use std::borrow::Cow;
use std::fmt::{self, Display, Formatter};
use std::ops::{Deref, DerefMut};

/// ID assigned by AWS for this upload.
#[derive(Debug, Clone, Default)]
pub struct UploadId(Cow<'static, str>);

impl UploadId {
    pub fn new<T: Into<Cow<'static, str>>>(id: T) -> Self {
        Self(id.into())
    }

    pub(crate) fn try_from_create_resp(value: CreateResponse) -> Result<Self, ErrorRepr> {
        value
            .upload_id
            .as_deref()
            .map(Self::from)
            .ok_or_else(|| ErrorRepr::Missing("CreateResponse", "upload_id"))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Deref for UploadId {
    type Target = str;

    fn deref(&self) -> &str {
        &*self.0
    }
}

impl Display for UploadId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for UploadId {
    fn from(value: &str) -> Self {
        Self::new(value.to_string())
    }
}

impl From<String> for UploadId {
    fn from(value: String) -> Self {
        Self(Cow::Owned(value))
    }
}

/// The address of an uploaded object in S3.
#[derive(Debug, Clone, Default)]
pub struct ObjectUri {
    pub bucket: Bucket,
    pub key: Key,
}

impl ObjectUri {
    pub fn new(bucket: Bucket, key: Key) -> Self {
        Self { bucket, key }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bucket.is_empty() || self.key.is_empty()
    }
}

impl Display for ObjectUri {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "s3://{}/{}", &self.bucket, &self.key)
    }
}

impl<T: Into<Bucket>, U: Into<Key>> From<(T, U)> for ObjectUri {
    fn from((b, k): (T, U)) -> Self {
        ObjectUri::new(b.into(), k.into())
    }
}

/// The destination bucket for this upload when it is complete.
#[derive(Debug, Clone, Default)]
pub struct Bucket(Cow<'static, str>);

impl Bucket {
    pub fn new<T: Into<Cow<'static, str>>>(bucket: T) -> Self {
        let bucket: Cow<'static, str> = bucket.into();
        match bucket.strip_suffix("/") {
            Some(v) => v.into(),
            _ => Self(bucket),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Deref for Bucket {
    type Target = str;

    fn deref(&self) -> &str {
        &*self.0
    }
}

impl Display for Bucket {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for Bucket {
    fn from(value: &str) -> Self {
        Self::new(value.to_string())
    }
}

impl From<String> for Bucket {
    fn from(value: String) -> Self {
        Self(Cow::Owned(value))
    }
}

/// The name of the object for this upload when it is complete.
#[derive(Debug, Clone, Default)]
pub struct Key(Cow<'static, str>);

impl Key {
    pub fn new<T: Into<Cow<'static, str>>>(key: T) -> Self {
        Self(key.into())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Deref for Key {
    type Target = str;

    fn deref(&self) -> &str {
        &*self.0
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for Key {
    fn from(value: &str) -> Self {
        Self::new(value.to_string())
    }
}

impl From<String> for Key {
    fn from(value: String) -> Self {
        Self(Cow::Owned(value))
    }
}

/// A prefix for S3 object keys.
#[derive(Debug, Clone, Default)]
pub struct KeyPrefix(Cow<'static, str>);

impl KeyPrefix {
    /// Create a new object key prefix.
    pub fn new<T: Into<Cow<'static, str>>>(prefix: T) -> Self {
        let mut prefix: Cow<'static, str> = prefix.into();
        if prefix.ends_with('/') {
            Self(prefix)
        } else {
            *prefix.to_mut() += "/";
            Self(prefix)
        }
    }

    /// Create an object [`Key`] with the name and this prefix.
    pub fn to_key(mut self, name: String) -> Key {
        *self.0.to_mut() += &name;
        Key::new(self.0)
    }
}

impl Deref for KeyPrefix {
    type Target = str;

    fn deref(&self) -> &str {
        &*self.0
    }
}

impl Display for KeyPrefix {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for KeyPrefix {
    fn from(value: &str) -> Self {
        Self::new(value.to_string())
    }
}

impl From<String> for KeyPrefix {
    fn from(value: String) -> Self {
        Self(Cow::Owned(value))
    }
}

/// AWS object etag.
#[derive(Debug, Clone)]
pub struct EntityTag(Cow<'static, str>);

impl EntityTag {
    fn new<T: Into<Cow<'static, str>>>(etag: T) -> Self {
        Self(etag.into())
    }

    pub(crate) fn try_from_upload_resp(value: UploadResponse) -> Result<Self, ErrorRepr> {
        value
            .e_tag
            .map(Self::new)
            .ok_or_else(|| ErrorRepr::Missing("UploadResponse", "e_tag"))
    }

    pub(crate) fn try_from_complete_resp(value: CompleteResponse) -> Result<Self, ErrorRepr> {
        value
            .e_tag
            .as_deref()
            .map(Self::from)
            .ok_or_else(|| ErrorRepr::Missing("CompleteResponse", "e_tag"))
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

impl Deref for EntityTag {
    type Target = str;

    fn deref(&self) -> &str {
        &*self.0
    }
}

impl Display for EntityTag {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The part number for an S3 part upload request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartNumber(i32);

impl Default for PartNumber {
    fn default() -> Self {
        Self(1)
    }
}

impl PartNumber {
    /// Create a new `PartNumber` from a bare integer.
    pub fn new(n: i32) -> Self {
        Self(n)
    }

    /// Increment `PartNumber`.
    pub fn incr(&mut self) {
        self.0 += 1;
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
