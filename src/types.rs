use aws_sdk_s3::types as s3;

use crate::{
    aws_ops::{create, upload_part},
    AwsError,
};

/// The ID given to this upload by AWS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadId(String);

impl std::fmt::Display for UploadId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for UploadId {
    fn from(value: String) -> Self {
        UploadId(value)
    }
}

impl From<UploadId> for String {
    fn from(value: UploadId) -> Self {
        value.to_string()
    }
}
impl TryFrom<create::CreateMultipartUploadOutput> for UploadId {
    type Error = AwsError;

    fn try_from(value: create::CreateMultipartUploadOutput) -> Result<Self, Self::Error> {
        let id = value.upload_id().ok_or(AwsError::Missing("upload_id"))?;
        Ok(Self(id.to_string()))
    }
}

/// The destination for the multipart upload in S3.
#[derive(Debug, Clone, PartialEq)]
pub struct UploadAddress {
    bucket: String,
    key: String,
}

impl UploadAddress {
    pub fn new(bucket: &str, key: &str) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
        }
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

impl<'a> From<(&'a str, &'a str)> for UploadAddress {
    fn from(value: (&'a str, &'a str)) -> Self {
        Self::new(value.0, value.1)
    }
}

/// The ID and destination, which appear in calls to AWS.
#[derive(Debug, Clone, PartialEq)]
pub struct UploadParams {
    upload_id: UploadId,
    addr: UploadAddress,
}

impl UploadParams {
    pub fn new(upload_id: UploadId, addr: UploadAddress) -> Self {
        Self { upload_id, addr }
    }

    pub fn upload_id(&self) -> UploadId {
        self.upload_id.clone()
    }

    pub fn bucket(&self) -> &str {
        self.addr.bucket()
    }

    pub fn key(&self) -> &str {
        self.addr.key()
    }
}

/// An ID for a part that has been uploaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityTag(String);

impl EntityTag {
    pub fn new(etag: String) -> Self {
        Self(etag)
    }
}

impl std::fmt::Display for EntityTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for EntityTag {
    fn from(value: String) -> Self {
        EntityTag(value)
    }
}

impl TryFrom<upload_part::UploadPartOutput> for EntityTag {
    type Error = AwsError;

    fn try_from(value: upload_part::UploadPartOutput) -> Result<Self, Self::Error> {
        Ok(Self(value.e_tag.ok_or(AwsError::Missing("e_tag"))?))
    }
}

/// A type holding the history of parts already uploaded, expressed as a vector
/// of `(EntityTag, i32)`.
///
/// The second coordinate is a monotonically increasing sequence of integers for
/// each uploaded part and is set automatically.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct UploadedParts {
    pub parts: Vec<(EntityTag, i32)>,
}

impl UploadedParts {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add the ID of a new completed part, incrementing the integer index.
    pub fn update(&mut self, etag: EntityTag) {
        let part_number = self.next_part_number();
        self.parts.push((etag, part_number));
    }

    /// Get the number of parts.
    pub fn num_parts(&self) -> usize {
        self.parts.len()
    }

    /// Get the part number of the last uploaded part.
    pub fn last_part_number(&self) -> i32 {
        self.parts
            .iter()
            .fold(0, |acc, (_, p)| if acc >= *p { acc } else { *p })
    }

    /// Get the next part number, i.e., the one being built currently for the
    /// next upload part call.
    pub fn next_part_number(&self) -> i32 {
        self.last_part_number() + 1
    }
}

impl<'a> From<&'a UploadedParts> for s3::CompletedMultipartUpload {
    fn from(val: &'a UploadedParts) -> s3::CompletedMultipartUpload {
        let parts = val.parts.iter().fold(Vec::new(), |mut acc, (t, n)| {
            acc.push(
                s3::CompletedPart::builder()
                    .e_tag(t.to_string())
                    .part_number(*n)
                    .build(),
            );

            acc
        });

        s3::CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build()
    }
}

impl Extend<(EntityTag, i32)> for UploadedParts {
    fn extend<T: IntoIterator<Item = (EntityTag, i32)>>(&mut self, iter: T) {
        self.parts.extend(iter)
    }
}
