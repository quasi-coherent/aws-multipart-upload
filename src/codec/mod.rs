//! Encoding data in the body of a part upload request.
//!
//! This module defines `PartEncoder` and a few select implementations.
//! `PartEncoder` describes how an item should be written as bytes to a part
//! upload request body.
use bytes::BufMut;

use crate::client::part::PartBody;

#[cfg(feature = "csv")]
#[cfg_attr(docsrs, doc(cfg(feature = "csv")))]
mod csv_writer;
#[cfg(feature = "csv")]
#[cfg_attr(docsrs, doc(cfg(feature = "csv")))]
pub use csv_writer::CsvEncoder;

mod error;
pub use error::{EncodeError, EncodeErrorKind};

mod json_writer;
pub use json_writer::JsonLinesEncoder;

mod lines_writer;
pub use lines_writer::LinesEncoder;

/// Encoding for items in a part of a multipart upload.
pub trait PartEncoder<Item> {
    /// The type of value returned when encoding an item is not successful.
    type Error: EncodeError;

    /// Return an encoder that is ready for a new upload.
    fn new_upload(&self) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Encode this item in the part, returning the number of bytes written.
    fn encode(&mut self, item: Item) -> Result<usize, Self::Error>;

    /// Flush the items in any internal buffer.
    fn flush(&mut self) -> Result<(), Self::Error>;

    /// Convert the encoder to a `PartBody`.
    fn into_body(self) -> Result<PartBody, Self::Error>;

    /// Return an encoder that is ready for a new part.  Defers to `new_upload`
    /// in the provided implementation.
    ///
    /// Override this to provide a different means of preparing for a new part
    /// if `new_upload` is specifically for the first part.
    ///
    /// For example, an encoder may implement `new_upload` to write a header row
    /// but this is only valid for the first part.  So `new_part` would instead
    /// skip writing a header.
    fn new_part(&self) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        self.new_upload()
    }
}

impl<T: AsRef<[u8]>> PartEncoder<T> for PartBody {
    type Error = std::convert::Infallible;

    fn new_upload(&self) -> Result<Self, Self::Error> {
        let capacity = self.capacity();
        Ok(Self::with_capacity(capacity))
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
