use crate::sdk::PartBody;

mod error;
pub use error::{EncodeError, EncodeErrorKind};

#[cfg(feature = "arrow")]
#[cfg_attr(docsrs, doc(cfg(feature = "arrow")))]
mod arrow_writer;
#[cfg(feature = "arrow")]
#[cfg_attr(docsrs, doc(cfg(feature = "arrow")))]
#[doc(inline)]
pub use arrow_writer::ParquetEncoder;

#[cfg(feature = "csv")]
#[cfg_attr(docsrs, doc(cfg(feature = "csv")))]
mod csv_writer;
#[cfg(feature = "csv")]
#[cfg_attr(docsrs, doc(cfg(feature = "csv")))]
#[doc(inline)]
pub use csv_writer::CsvEncoder;

mod json_writer;
pub use json_writer::JsonLinesEncoder;

/// `EncodedPart` describes how a value should be stored as bytes when being
/// added to a part in an upload.
pub trait EncodedPart<Item> {
    /// The type of value returned when encoding items is not successful.
    type Error: EncodeError;

    /// Encode this item in the part.
    fn encode(&mut self, item: Item) -> Result<(), Self::Error>;

    /// Flush the items in any internal buffer.
    fn flush(&mut self) -> Result<(), Self::Error>;

    /// Return the current size of the part.
    fn current_size(&self) -> usize;

    /// Convert the encoding to a [`PartBody`].
    fn to_part_body(&mut self) -> Result<PartBody, Self::Error>;
}
