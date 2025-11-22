//! `ObjectUri` iterators.
//!
//! This module provides types that can help in building iterators of URIs to
//! to a multipart upload type with [`NewObjectUri`].
//!
//! The only thing required to create a new upload is the URI of the object to be
//! uploaded, so given an iterator of `ObjectUri`s, this defines a sequence of
//! multipart uploads that can be created as the previous one is completed by
//! calling `next` on the iterator.  [`OneTimeUse`], an iterator that only
//! produces one `ObjectUri`, is capable of serving a single multipart upload.
//!
//! # Example
//!
//! This is an iterator of `ObjectUri`s that writes to a prefix based on the
//! current date and time.
//!
//! ```rust
//! use aws_multipart_upload::{Bucket, Key, KeyPrefix, NewObjectUri};
//! use aws_multipart_upload::uri::ObjectUriIterExt as _;
//!
//! const BUCKET: &str = "my-bucket";
//! const PREFIX: &str = "static/object/prefix";
//!
//! let iter_pfx = std::iter::repeat_with(|| KeyPrefix::from(PREFIX));
//! let iter = iter_pfx.map_key(BUCKET, |prefix| {
//!     let now = chrono::Utc::now();
//!     let now_str = now.format("%Y/%m/%d/%H").to_string();
//!     let us = now.timestamp_micros();
//!     let root = format!("{now_str}/{us}.csv")
//!     prefix.to_key(&root)
//! });
//!
//! let mut uri = NewObjectUri::uri_iter(iter);
//! let new_uri = uri.new_uri().unwrap();
//!
//! println!("{new_uri}");
//! // "s3://my-bucket/static/object/prefix/2025/11/11/11/01/1763683634194850.csv"
//! ```
//! [`NewObjectUri`]: super::NewObjectUri
use crate::client::UploadClient;
use crate::client::request::{CreateRequest, SendCreateUpload};

use std::borrow::Cow;
use std::fmt::{self, Formatter};
use std::ops::Deref;

/// The address of an uploaded object in S3.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ObjectUri {
    /// The S3 bucket for the object.
    ///
    /// This should be the plain bucket name, e.g., "my-s3-bucket".
    pub bucket: Bucket,
    /// The full key of this object within the bucket.
    pub key: Key,
}

impl ObjectUri {
    /// Create a new `ObjectUri` from bucket and object key.
    pub fn new(bucket: Bucket, key: Key) -> Self {
        Self { bucket, key }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bucket.is_empty() || self.key.is_empty()
    }
}

impl fmt::Display for ObjectUri {
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
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Bucket(Cow<'static, str>);

impl Bucket {
    /// Create a new `Bucket`.
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
        &self.0
    }
}

impl fmt::Display for Bucket {
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

/// The key within the associated bucket for this object.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Key(Cow<'static, str>);

impl Key {
    /// Create a new object `Key`.
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
        &self.0
    }
}

impl fmt::Display for Key {
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

/// A prefix of S3 object keys.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct KeyPrefix(Cow<'static, str>);

impl KeyPrefix {
    /// Create a new object key prefix.
    ///
    /// Normalized to end with a single `'/'` and have no leading `'/'`.
    pub fn new<T: Into<Cow<'static, str>>>(prefix: T) -> Self {
        let raw: Cow<'static, str> = prefix.into();
        let trimmed = raw.trim_matches('/');
        Self(format!("{trimmed}/").into())
    }

    /// Extend this prefix by another.
    pub fn append(&self, other: &KeyPrefix) -> Self {
        format!("{self}{other}").into()
    }

    /// Create an object [`Key`] with this prefix and the given suffix.
    pub fn to_key(&self, suffix: &str) -> Key {
        format!("{self}{suffix}").into()
    }
}

impl Deref for KeyPrefix {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for KeyPrefix {
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
        Self::new(value)
    }
}

/// Produce an `ObjectUri` for a new upload from an iterator.
pub struct NewObjectUri {
    inner: Box<dyn Iterator<Item = ObjectUri>>,
}

impl NewObjectUri {
    /// Create a new `NewObjectUri` from an arbitrary iterator of `ObjectUri`.
    pub fn uri_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = ObjectUri> + 'static,
    {
        Self {
            inner: Box::new(iter.into_iter()),
        }
    }

    /// Produce the next value from the inner iterator.
    pub fn new_uri(&mut self) -> Option<ObjectUri> {
        self.inner.next()
    }

    /// Construct the request future to create a new multipart upload using the
    /// next `ObjectUri` produced by this `NewObjectUri` value.
    pub fn new_upload(&mut self, client: &UploadClient) -> Option<SendCreateUpload> {
        let uri = self.inner.next()?;
        let req = CreateRequest::new(uri);
        let fut = SendCreateUpload::new(client, req);
        Some(fut)
    }
}

impl Default for NewObjectUri {
    fn default() -> Self {
        Self::uri_iter(EmptyUri)
    }
}

impl fmt::Debug for NewObjectUri {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("NewObjectUri")
            .field("inner", &"Iterator<Item = ObjectUri>")
            .finish()
    }
}

/// Adds the method `map_key` to iterators over `KeyPrefix`.
pub trait ObjectUriIterExt: Iterator {
    /// Returns an iterator of `ObjectUri` by applying the function `F` to each
    /// `KeyPrefix` to produce the object `Key`.
    fn map_key<B, F>(self, bucket: B, f: F) -> MapKey<Self, F>
    where
        Self: Iterator<Item = KeyPrefix> + Sized,
        F: FnMut(KeyPrefix) -> Key,
        B: Into<Bucket>,
    {
        MapKey::new(self, bucket, f)
    }
}

impl<I: Iterator> ObjectUriIterExt for I {}

/// Iterator for [`map_key`](ObjectUriIterExt::map_key).
pub struct MapKey<I, F> {
    bucket: Bucket,
    inner: I,
    f: F,
}

impl<I, F> MapKey<I, F> {
    fn new<B: Into<Bucket>>(inner: I, bucket: B, f: F) -> Self {
        Self {
            inner,
            bucket: bucket.into(),
            f,
        }
    }
}

impl<I, F> Iterator for MapKey<I, F>
where
    I: Iterator<Item = KeyPrefix>,
    F: FnMut(KeyPrefix) -> Key,
{
    type Item = ObjectUri;

    fn next(&mut self) -> Option<Self::Item> {
        let prefix = self.inner.next()?;
        let key = (self.f)(prefix);
        let uri = ObjectUri::new(self.bucket.clone(), key);
        Some(uri)
    }
}

/// An empty iterator of `ObjectUri`s.
#[derive(Debug, Clone, Copy, Default)]
pub struct EmptyUri;
impl IntoIterator for EmptyUri {
    type IntoIter = std::iter::Empty<ObjectUri>;
    type Item = ObjectUri;

    fn into_iter(self) -> Self::IntoIter {
        std::iter::empty()
    }
}

/// Iterator that is exhausted after one `ObjectUri`.
#[derive(Debug, Clone, Default)]
pub struct OneTimeUse(Option<ObjectUri>);

impl OneTimeUse {
    /// Use the given `uri` as the one produced.
    pub fn new(uri: ObjectUri) -> Self {
        Self(Some(uri))
    }
}

impl Iterator for OneTimeUse {
    type Item = ObjectUri;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.take()
    }
}
