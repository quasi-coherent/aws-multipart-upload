//! Types for working with errors.
use crate::client::UploadId;
use crate::client::part::PartNumber;
use crate::codec::{EncodeError, EncodeErrorKind};
use crate::uri::ObjectUri;

use aws_sdk::error::SdkError;
use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};

/// A specialized `Result` type for errors originating in this crate.
pub type Result<T, E = Error> = ::std::result::Result<T, E>;

/// The value returned when some operation in this crate fails.
#[derive(Debug, thiserror::Error)]
pub struct Error(pub(crate) ErrorRepr);

impl Error {
    /// Returns the details of the upload that failed if available.
    pub fn failed_upload(&self) -> Option<&FailedUpload> {
        if let ErrorRepr::UploadFailed { failed, .. } = &self.0 {
            return Some(failed);
        }
        None
    }

    /// Returns the category under which this error falls.
    pub fn kind(&self) -> ErrorKind {
        match self.0 {
            ErrorRepr::Sdk(_) => ErrorKind::Sdk,
            ErrorRepr::Missing(_, _) => ErrorKind::Config,
            ErrorRepr::Encoding(_, _) => ErrorKind::Encoding,
            ErrorRepr::UploadFailed { .. } => ErrorKind::Upload,
            ErrorRepr::DynStd(_) => ErrorKind::Unknown,
            ErrorRepr::Other { kind, .. } => kind,
        }
    }

    /// Convert an arbitrary [`std::error::Error`] to this error type.
    pub fn from_dyn_std<E>(e: E) -> Self
    where
        E: StdError + 'static,
    {
        let err = Box::new(e);
        Self(ErrorRepr::DynStd(err))
    }

    /// Create this error from a category and message.
    pub fn other(kind: ErrorKind, msg: &'static str) -> Self {
        Self(ErrorRepr::Other { kind, msg })
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<ErrorRepr> for Error {
    fn from(value: ErrorRepr) -> Self {
        Self(value)
    }
}

impl<E: EncodeError> From<E> for Error {
    fn from(value: E) -> Self {
        ErrorRepr::from(value).into()
    }
}

/// The category of the error.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum ErrorKind {
    /// There was an error in configuration.
    Config,
    /// There was an error encoding an item in a part.
    Encoding,
    /// An error was returned by the underlying SDK.
    Sdk,
    /// There was an error operating the upload.
    Upload,
    /// The origin of the error is not known.
    Unknown,
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config => write!(f, "config"),
            Self::Encoding => write!(f, "encoding"),
            Self::Sdk => write!(f, "sdk"),
            Self::Upload => write!(f, "upload"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// The data of an upload that failed.
///
/// This may be found using [`Error::failed_upload`] on the error returned by
/// some operation.
///
/// The data is what would be required to resume a multipart upload or abort it.
#[derive(Debug, Clone)]
pub struct FailedUpload {
    /// The ID of the upload assigned on creation.
    pub id: UploadId,
    /// The destination URI of the upload.
    pub uri: ObjectUri,
    /// The part number that was in progress when the error occurred.
    pub part: PartNumber,
}

impl FailedUpload {
    pub(crate) fn new(id: &UploadId, uri: &ObjectUri, part: PartNumber) -> Self {
        Self {
            id: id.clone(),
            uri: uri.clone(),
            part,
        }
    }
}

impl Display for FailedUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            r#"{{ "id": "{}", "uri": "{}", "part": "{}" }}"#,
            &self.id, &self.uri, self.part
        )
    }
}

/// Appending upload data to the error if available.
pub(crate) trait UploadContext<T> {
    fn upload_ctx(self, id: &UploadId, uri: &ObjectUri, part: PartNumber) -> Result<T>;
}

impl<T, E> UploadContext<T> for Result<T, E>
where
    E: StdError + 'static,
{
    fn upload_ctx(self, id: &UploadId, uri: &ObjectUri, part: PartNumber) -> Result<T> {
        match self {
            Ok(t) => Ok(t),
            Err(e) => {
                let failed = FailedUpload::new(id, uri, part);
                let err = ErrorRepr::UploadFailed {
                    failed,
                    source: Box::new(e),
                };
                Err(err.into())
            }
        }
    }
}

/// Internal error representation.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ErrorRepr {
    #[error("{0} missing required field: {1}")]
    Missing(&'static str, &'static str),
    #[error("encoding error: {0} {1}")]
    Encoding(String, EncodeErrorKind),
    #[error("upload failed: {failed}: {source}")]
    UploadFailed {
        failed: FailedUpload,
        source: Box<dyn StdError>,
    },
    #[error("error from aws_sdk: {0}")]
    Sdk(#[source] Box<dyn StdError>),
    #[error("{kind} error: {msg}")]
    Other { kind: ErrorKind, msg: &'static str },
    #[error(transparent)]
    DynStd(Box<dyn StdError>),
}

impl<E, R> From<SdkError<E, R>> for ErrorRepr
where
    E: StdError + 'static,
    R: std::fmt::Debug + 'static,
{
    fn from(value: SdkError<E, R>) -> Self {
        Self::Sdk(Box::new(value))
    }
}

impl<E: EncodeError> From<E> for ErrorRepr {
    fn from(value: E) -> Self {
        ErrorRepr::Encoding(value.message(), value.kind())
    }
}
