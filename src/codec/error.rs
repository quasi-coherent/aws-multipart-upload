use std::convert::Infallible;
use std::fmt::{Display, Formatter, Result};

/// A trait that implements errors coming from encoding an item in a part.
pub trait EncodeError: std::error::Error {
    /// Human-readable error message.
    fn message(&self) -> String;

    /// Category of error.
    fn kind(&self) -> EncodeErrorKind;
}

/// Categorizes the cause of an encoding error.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum EncodeErrorKind {
    /// An I/O error.
    Io,
    /// Error in a representation of the data.
    Data,
    /// Received fewer bytes than expected.
    Eof,
    /// The origin of the error is not known.
    #[default]
    Unknown,
}

impl Display for EncodeErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let x = match self {
            Self::Io => "io",
            Self::Data => "data",
            Self::Eof => "eof",
            Self::Unknown => "unknown",
        };
        write!(f, "{x}")
    }
}

impl EncodeError for std::io::Error {
    fn message(&self) -> String {
        self.to_string()
    }

    fn kind(&self) -> EncodeErrorKind {
        EncodeErrorKind::Io
    }
}

impl EncodeError for Infallible {
    fn message(&self) -> String {
        "unbelievable".into()
    }

    fn kind(&self) -> EncodeErrorKind {
        EncodeErrorKind::Unknown
    }
}

impl EncodeError for serde_json::Error {
    fn message(&self) -> String {
        self.to_string()
    }

    fn kind(&self) -> EncodeErrorKind {
        match self.classify() {
            serde_json::error::Category::Data | serde_json::error::Category::Syntax => {
                EncodeErrorKind::Data
            }
            serde_json::error::Category::Eof => EncodeErrorKind::Eof,
            serde_json::error::Category::Io => EncodeErrorKind::Io,
        }
    }
}
