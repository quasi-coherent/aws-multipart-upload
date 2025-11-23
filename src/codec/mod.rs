//! Encoding data in the body of a part upload request.
//!
//! This module defines `PartEncoder` and a few select implementations.
//! `PartEncoder` describes how an item should be written as bytes to a part
//! upload request body.
use crate::client::part::PartBody;

use bytes::BufMut;

#[cfg(feature = "csv")]
#[cfg_attr(docsrs, doc(cfg(feature = "csv")))]
mod csv_writer;
#[cfg(feature = "csv")]
#[cfg_attr(docsrs, doc(cfg(feature = "csv")))]
pub use csv_writer::{CsvBuilder, CsvEncoder};

mod error;
pub use error::{EncodeError, EncodeErrorKind};

mod json_writer;
pub use json_writer::{JsonLinesBuilder, JsonLinesEncoder};

mod lines_writer;
pub use lines_writer::{LinesBuilder, LinesEncoder};

/// Encoding for items in a part of a multipart upload.
pub trait PartEncoder<Item> {
    /// The builder for this encoder.
    type Builder;

    /// The type of value returned when encoding items is not successful.
    type Error: EncodeError;

    /// Build this encoder for a new upload.
    fn build(builder: &Self::Builder, part_size: usize) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Encode this item in the part, returning the number of bytes written.
    fn encode(&mut self, item: Item) -> Result<usize, Self::Error>;

    /// Flush the items in any internal buffer.
    fn flush(&mut self) -> Result<(), Self::Error>;

    /// Reset the encoder for a new part.
    ///
    /// Override this method to provide an alternative means of building the
    /// encoder, for example if preparing one for a new part is different than
    /// preparing for a new upload.
    fn reset(builder: &Self::Builder, part_size: usize) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Self::build(builder, part_size)
    }

    /// Convert the encoder to a `PartBody`.
    fn into_body(self) -> Result<PartBody, Self::Error>;
}

impl<T: AsRef<[u8]>> PartEncoder<T> for PartBody {
    type Builder = ();
    type Error = std::convert::Infallible;

    fn build(_: &Self::Builder, part_size: usize) -> Result<Self, Self::Error> {
        Ok(Self::with_capacity(part_size))
    }

    fn encode(&mut self, item: T) -> Result<usize, Self::Error> {
        let buf = item.as_ref();
        let bytes = buf.len();
        self.reserve(bytes);
        self.put(buf);
        Ok(bytes)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn into_body(self) -> Result<PartBody, Self::Error> {
        Ok(self)
    }
}
