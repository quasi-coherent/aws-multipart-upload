use crate::config::*;
use crate::sdk::{ObjectUri, UploadData, UploadId};

use std::fmt::{self, Display, Formatter};

/// `TargetUpload` sets the ideal size of the final uploaded object.
///
/// Note that AWS [limits] apply to parts and the object being built and this
/// value cannot exceed them.
///
/// [limits]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
#[derive(Debug, Clone, Copy)]
pub struct TargetUpload {
    /// Target size of an upload in bytes.
    ///
    /// Defaults to 5GiB.
    pub upload_bytes: usize,
    /// Target number of parts in the upload.
    ///
    /// Must be at most 10,000, which is the default limit.
    pub upload_parts: usize,
    /// Limit to the number of concurrent part uploads.
    ///
    /// Default is no limit.
    pub capacity: Option<usize>,
}

impl Default for TargetUpload {
    fn default() -> Self {
        Self {
            upload_bytes: DEFAULT_MAX_OBJECT_SIZE,
            upload_parts: AWS_MAX_PART_COUNT,
            capacity: None,
        }
    }
}

impl TargetUpload {
    /// Set the target number of bytes in the upload.
    pub fn target_bytes(self, limit: usize) -> Self {
        let upload_bytes = std::cmp::min(limit, AWS_MAX_OBJECT_SIZE);
        Self {
            upload_bytes,
            ..self
        }
    }

    /// Set the target number of parts to add to the upload.
    pub fn target_parts(self, limit: usize) -> Self {
        let upload_parts = std::cmp::min(limit, AWS_MAX_PART_COUNT);
        Self {
            upload_parts,
            ..self
        }
    }

    /// Set a limit to concurrent part upload tasks.
    pub fn max_capacity<T: Into<Option<usize>>>(self, limit: T) -> Self {
        let capacity = limit.into();
        Self { capacity, ..self }
    }

    pub(super) fn upload_complete(
        &self,
        current_bytes: usize,
        current_parts: usize,
        last_part_bytes: usize,
    ) -> bool {
        let enough_bytes = current_bytes + last_part_bytes >= self.upload_bytes;
        let enough_parts = current_parts + 1 >= AWS_MAX_PART_COUNT;
        enough_bytes || enough_parts
    }
}

/// Empty writer return value.
#[derive(Debug, Clone, Copy)]
pub struct UnitReturn;

impl Display for UnitReturn {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "()")
    }
}

/// `UploadProgress` is the return type of the `MultipartWrite` for
/// [`MultipartUpload`].
#[derive(Debug, Clone)]
pub struct UploadProgress<R = UnitReturn> {
    pub id: UploadId,
    pub uri: ObjectUri,
    pub last_part_bytes: usize,
    pub total_bytes: usize,
    pub total_parts: usize,
    pub inner: R,
}

impl<R> UploadProgress<R> {
    pub(super) fn new(data: &UploadData, state: UploadState, inner: R) -> Self {
        Self {
            id: data.get_id(),
            uri: data.get_uri(),
            last_part_bytes: state.last_part_bytes,
            total_bytes: state.total_bytes,
            total_parts: state.total_parts,
            inner,
        }
    }
}

impl<R: Display> Display for UploadProgress<R> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            r#"{{ "id": "{}", "uri": "{}", "last_part_bytes": {}, "total_bytes": {}, "total_parts": {}, "inner": {} }}"#,
            &self.id,
            &self.uri,
            self.last_part_bytes,
            self.total_bytes,
            self.total_parts,
            &self.inner
        )
    }
}

/// The current state of the upload being built.
#[derive(Debug, Clone, Copy, Default)]
pub struct UploadState {
    pub total_bytes: usize,
    pub total_parts: usize,
    pub last_part_bytes: usize,
}

impl UploadState {
    pub(super) fn update(&mut self, part_bytes: usize) {
        self.total_bytes += part_bytes;
        self.last_part_bytes = part_bytes;
        self.total_parts += 1;
    }
}

impl Display for UploadState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            r#"{{ "total_bytes": {}, "total_parts": {}, "last_part_bytes": {} }}"#,
            self.total_bytes, self.total_parts, self.last_part_bytes,
        )
    }
}

/// Return type when a part is written by [`Encoded`].
#[derive(Debug, Clone, Copy, Default)]
pub struct PartProgress {
    pub total_bytes: usize,
    pub total_items: usize,
}

impl PartProgress {
    pub(super) fn new(state: PartState) -> Self {
        Self {
            total_bytes: state.total_bytes,
            total_items: state.total_items,
        }
    }
}

impl Display for PartProgress {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            r#"{{ "total_bytes": {}, "total_items": {} }}"#,
            self.total_bytes, self.total_items
        )
    }
}

/// The current state of the part being written.
#[derive(Debug, Clone, Copy, Default)]
pub struct PartState {
    pub total_bytes: usize,
    pub total_items: usize,
}

impl PartState {
    pub(super) fn update(&mut self, total_bytes: usize) {
        self.total_bytes = total_bytes;
        self.total_items += 1;
    }
}

/// `TargetPart` sets the ideal size of one part in an upload.
#[derive(Debug, Clone, Copy)]
pub struct TargetPart {
    /// Target size of a part in bytes.
    ///
    /// Defaults to 10MiB.
    pub part_bytes: usize,
    /// Target number of items written to the part.
    ///
    /// By default there is no target limit to the number of items.
    pub part_items: usize,
    /// Minimum part size in bytes.
    ///
    /// Must be at least 5MiB, which is the default.
    pub min_part_bytes: usize,
}

impl Default for TargetPart {
    fn default() -> Self {
        Self {
            part_bytes: DEFAULT_MAX_PART_SIZE,
            part_items: usize::MAX,
            min_part_bytes: AWS_MIN_PART_SIZE,
        }
    }
}

impl TargetPart {
    /// Set the target number of bytes in a part.
    pub fn target_part_bytes(self, limit: usize) -> Self {
        let part_bytes = std::cmp::min(limit, AWS_MAX_PART_SIZE);
        Self { part_bytes, ..self }
    }

    /// Set the target number of items written to a part.
    pub fn with_target_items(self, limit: usize) -> Self {
        Self {
            part_items: limit,
            ..self
        }
    }

    /// Set the minimum number of bytes in a part.
    pub fn with_min_part_bytes(self, limit: usize) -> Self {
        let min_part_bytes = std::cmp::max(limit, AWS_MIN_PART_SIZE);
        Self {
            min_part_bytes,
            ..self
        }
    }

    pub(super) fn part_complete(&self, current_bytes: usize, current_items: usize) -> bool {
        let enough_bytes = self.part_bytes >= current_bytes;
        let enough_items = self.part_items >= current_items && current_bytes >= self.min_part_bytes;
        enough_bytes || (!enough_bytes && enough_items)
    }
}
