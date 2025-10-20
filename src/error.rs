//! Errors this crate can emit.
use crate::codec::{EncodeError, EncodeErrorKind};
use crate::complete_upload::CompleteMultipartUploadError;
use crate::create_upload::CreateMultipartUploadError;
use crate::sdk::{CompletedParts, ObjectUri, PartNumber, UploadId};
use crate::upload_part::UploadPartError;

use aws_sdk_s3::error::SdkError;
use std::fmt::{self, Display, Formatter};

/// A specialized `Result` type for this crate.
pub type Result<T, E = Error> = ::std::result::Result<T, E>;

/// The value returned in this crate when an error occurs.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error(pub(crate) ErrorRepr);

impl Error {
    pub fn kind(&self) -> ErrorKind {
        match self.0 {
            ErrorRepr::Create { .. }
            | ErrorRepr::UploadPart { .. }
            | ErrorRepr::Complete { .. } => ErrorKind::Sdk,
            ErrorRepr::Missing(_, _) => ErrorKind::Config,
            ErrorRepr::Encoding(_, _) => ErrorKind::Encoding,
            ErrorRepr::MissingNextUri | ErrorRepr::UploadStillActive => ErrorKind::Write,
            ErrorRepr::StdDyn(_) => ErrorKind::Unknown,
            ErrorRepr::Any { kind, .. } => kind,
        }
    }

    pub fn from_dyn<E>(e: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let err = Box::new(e);
        Self(ErrorRepr::StdDyn(err))
    }

    pub fn from_kind(kind: ErrorKind, msg: &'static str) -> Self {
        Self(ErrorRepr::Any { kind, msg })
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
    Config,
    Encoding,
    Sdk,
    Write,
    Unknown,
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config => write!(f, "config"),
            Self::Encoding => write!(f, "encoding"),
            Self::Sdk => write!(f, "sdk"),
            Self::Write => write!(f, "write"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Internal error type that we are free to change at will.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ErrorRepr {
    #[error("{0} missing required field: {1}")]
    Missing(&'static str, &'static str),
    #[error("encoding error: {0} {1}")]
    Encoding(String, EncodeErrorKind),
    #[error("cannot start new upload while previous upload active")]
    UploadStillActive,
    #[error("could not start new upload, missing object uri")]
    MissingNextUri,
    #[error("creating multipart upload failed: {source}")]
    Create {
        uri: ObjectUri,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    #[error("uploading {part} to upload {id} failed: {source}")]
    UploadPart {
        id: UploadId,
        uri: ObjectUri,
        part: PartNumber,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    #[error("completing upload {id} failed: {source}")]
    Complete {
        id: UploadId,
        uri: ObjectUri,
        parts: CompletedParts,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    #[error("{kind} error: {msg}")]
    Any { kind: ErrorKind, msg: &'static str },
    #[error(transparent)]
    StdDyn(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl ErrorRepr {
    pub(crate) fn from_create_err(
        uri: &ObjectUri,
    ) -> impl FnMut(SdkError<CreateMultipartUploadError>) -> Self {
        move |e| Self::Create {
            uri: uri.clone(),
            source: Box::new(e),
        }
    }

    pub(crate) fn from_upload_err(
        id: &UploadId,
        uri: &ObjectUri,
        part: PartNumber,
    ) -> impl FnMut(SdkError<UploadPartError>) -> Self {
        move |e| Self::UploadPart {
            id: id.clone(),
            uri: uri.clone(),
            part,
            source: Box::new(e),
        }
    }

    pub(crate) fn from_complete_err(
        id: &UploadId,
        uri: &ObjectUri,
        parts: &CompletedParts,
    ) -> impl FnMut(SdkError<CompleteMultipartUploadError>) -> Self {
        move |e| Self::Complete {
            id: id.clone(),
            uri: uri.clone(),
            parts: parts.clone(),
            source: Box::new(e),
        }
    }
}

impl<E: EncodeError> From<E> for ErrorRepr {
    fn from(value: E) -> Self {
        ErrorRepr::Encoding(value.message(), value.kind())
    }
}
