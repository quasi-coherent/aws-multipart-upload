use crate::client::{SendCreateUpload, UploadClient};
use crate::error::{ErrorRepr, Result};
use crate::sdk::ObjectUri;
use crate::sdk::api::CreateRequest;

/// `ObjectUriIterator` is an iterator that represents configuration for
/// constructing the only value that is required to initiate a multipart upload:
/// the destination object URI.
///
/// A value that implements this type can be used in the API provided by this
/// crate to build an upload that continues indefinitely, suitable for the
/// scenario of uploading items from a stream in parts.
pub trait ObjectUriIterator {
    /// Create a new destination object URI.
    ///
    /// Returns `None` if a new upload is not desired.
    fn next(&mut self) -> Option<ObjectUri>;

    /// Use the next `ObjectUri` to create a new multipart upload.
    ///
    /// # Errors
    ///
    /// Returns an error if the next `ObjectUri` was `None`.
    fn new_send_create_upload(&mut self, client: &UploadClient) -> Result<SendCreateUpload> {
        let uri = self.next().ok_or_else(|| ErrorRepr::MissingNextUri)?;
        let req = CreateRequest::new(uri);
        let fut = SendCreateUpload::new(client, req);
        Ok(fut)
    }
}

/// `OneTimeUse` provides the parameters for a one-time-use multipart upload.
#[derive(Debug, Clone, Default)]
pub struct OneTimeUse(Option<ObjectUri>);

impl OneTimeUse {
    /// Create a new `OneTimeUse`.
    ///
    /// This URI will be used for the first and only multipart upload.
    pub fn new(uri: ObjectUri) -> Self {
        Self(Some(uri))
    }

    /// Returns whether this value does not hold an object URI.
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }
}

impl ObjectUriIterator for OneTimeUse {
    fn next(&mut self) -> Option<ObjectUri> {
        self.0.take()
    }
}
