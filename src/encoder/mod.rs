//! Encoding parts in an upload.
//!
//! This module defines [`PartEncoder`] and a few select implementations. A
//! `PartEncoder` describes how an item should be written as bytes to a part
//! upload request body.
use std::convert::Infallible;
use std::fmt::{self, Display, Formatter};

use crate::client::part::{PartBody, PartNumber};

#[cfg(feature = "csv")]
#[cfg_attr(docsrs, doc(cfg(feature = "csv")))]
mod csv_encoder;
#[cfg(feature = "csv")]
#[cfg_attr(docsrs, doc(cfg(feature = "csv")))]
pub use csv_encoder::CsvEncoder;

#[cfg(feature = "tokio-util")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio-util")))]
mod framed_encoder;
#[cfg(feature = "tokio-util")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio-util")))]
pub use framed_encoder::FramedEncoder;

mod json_encoder;
pub use json_encoder::JsonLinesEncoder;

mod lines_encoder;
pub use lines_encoder::LinesEncoder;

/// Encodes items in the body of a part upload request.
pub trait PartEncoder<Item> {
    /// The type of value returned when encoding fails.
    type Error: EncodeError;

    /// Encodes an item in the provided part.
    ///
    /// The part is in the context of an active upload, and its part number is
    /// provided in case the encoding behavior depends on it.
    fn encode(
        &mut self,
        part: &mut PartBody,
        part_number: PartNumber,
        item: Item,
    ) -> Result<(), Self::Error>;
}

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
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
            serde_json::error::Category::Data
            | serde_json::error::Category::Syntax => EncodeErrorKind::Data,
            serde_json::error::Category::Eof => EncodeErrorKind::Eof,
            serde_json::error::Category::Io => EncodeErrorKind::Io,
        }
    }
}
