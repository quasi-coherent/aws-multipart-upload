//! Working with errors in this crate.
use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};

use aws_sdk::error::{ProvideErrorMetadata, SdkError};
use aws_sdk::operation::RequestId as _;

use crate::client::UploadId;
use crate::client::part::CompletedParts;
use crate::codec::{EncodeError, EncodeErrorKind};
use crate::uri::ObjectUri;

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
            ErrorRepr::Sdk { .. } => ErrorKind::Sdk,
            ErrorRepr::Missing(..) => ErrorKind::Config,
            ErrorRepr::Encoding(..) => ErrorKind::Encoding,
            ErrorRepr::State(_) | ErrorRepr::UploadFailed { .. } => {
                ErrorKind::Upload
            },
            ErrorRepr::DynStd(_) => ErrorKind::Unknown,
            ErrorRepr::Other { kind, .. } => kind,
        }
    }

    /// Convert an arbitrary [`std::error::Error`] to this error type.
    pub fn from_std<E>(e: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        let err = Box::new(e);
        Self(ErrorRepr::DynStd(err))
    }

    /// Create this error from a category and message.
    pub fn other(kind: ErrorKind, msg: &'static str) -> Self {
        Self(ErrorRepr::Other { kind, msg })
    }

    pub(crate) fn state(msg: &'static str) -> Self {
        Self(ErrorRepr::State(msg))
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

impl<E> From<SdkError<E>> for Error
where
    E: ProvideErrorMetadata + StdError + Send + Sync + 'static,
{
    fn from(value: SdkError<E>) -> Self {
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
/// some operation. The data is what would be required to resume a multipart
/// upload or abort it.
///
/// Resuming an upload requires the full `CompletedParts`, but the failure that
/// led to this value being created may have prevented that from being obtained.
/// In this case the value cannot be used to resume an upload, only abort it.
#[derive(Debug, Clone)]
pub struct FailedUpload {
    /// The ID of the upload assigned on creation.
    pub id: UploadId,
    /// The destination URI of the upload.
    pub uri: ObjectUri,
    /// Collection of parts that were successfully uploaded.
    pub completed: CompletedParts,
}

impl FailedUpload {
    pub(crate) fn new(
        id: &UploadId,
        uri: &ObjectUri,
        completed: &CompletedParts,
    ) -> Self {
        Self { id: id.clone(), uri: uri.clone(), completed: completed.clone() }
    }
}

impl Display for FailedUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            r#"{{ "id": "{}", "uri": "{}", "completed": {:?} }}"#,
            &self.id, &self.uri, &self.completed
        )
    }
}

/// Appending upload data to the error if available.
pub(crate) trait ErrorWithUpload<T> {
    fn err_with_upl(
        self,
        id: &UploadId,
        uri: &ObjectUri,
        completed: &CompletedParts,
    ) -> Result<T>;
}

impl<T, E> ErrorWithUpload<T> for Result<T, E>
where
    E: StdError + Send + Sync + 'static,
{
    fn err_with_upl(
        self,
        id: &UploadId,
        uri: &ObjectUri,
        completed: &CompletedParts,
    ) -> Result<T> {
        match self {
            Ok(t) => Ok(t),
            Err(e) => {
                let failed = FailedUpload::new(id, uri, completed);
                let err =
                    ErrorRepr::UploadFailed { failed, source: Box::new(e) };
                Err(err.into())
            },
        }
    }
}

/// Internal error representation.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ErrorRepr {
    #[error("{0} missing required field: {1}")]
    Missing(&'static str, &'static str),
    #[error("{1} error encoding value: {0}")]
    Encoding(String, EncodeErrorKind),
    #[error("{failed}: {source}")]
    UploadFailed {
        failed: FailedUpload,
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("corrupted upload state: {0}")]
    State(&'static str),
    #[error("request {rid} returned {code}: {msg}")]
    Sdk { code: String, msg: String, rid: String },
    #[error("{kind} error: {msg}")]
    Other { kind: ErrorKind, msg: &'static str },
    #[error(transparent)]
    DynStd(Box<dyn StdError + Send + Sync>),
}

impl<E> From<SdkError<E>> for ErrorRepr
where
    E: ProvideErrorMetadata + StdError + Send + Sync + 'static,
{
    fn from(value: SdkError<E>) -> Self {
        let rid = value.request_id().unwrap_or("-1").to_string();
        let code = value.code().unwrap_or_default().to_string();
        let msg = value.message().unwrap_or_default().to_string();
        Self::Sdk { code, msg, rid }
    }
}

impl<E: EncodeError> From<E> for ErrorRepr {
    fn from(value: E) -> Self {
        ErrorRepr::Encoding(value.message(), value.kind())
    }
}
