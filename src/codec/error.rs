use std::fmt::{Display, Formatter, Result};

/// A trait that implements errors coming from encoding an item in a part.
pub trait EncodeError {
    /// Human-readable error message.
    fn message(&self) -> String;

    /// Category of error.
    fn kind(&self) -> EncodeErrorKind;
}

/// Categorizes the cause of an encoding error.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum EncodeErrorKind {
    Io,
    Syntax,
    Data,
    Eof,
    #[default]
    Unknown,
}

impl Display for EncodeErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let x = match self {
            Self::Io => "io",
            Self::Syntax => "syntax",
            Self::Data => "data",
            Self::Eof => "eof",
            Self::Unknown => "unknown",
        };
        write!(f, "{x}")
    }
}
