use crate::client::part::PartBody;
use crate::codec::{EncodeError, EncodeErrorKind, PartEncoder};

use csv::{Error as CsvError, Writer, WriterBuilder};
use serde::Serialize;

/// Builder for `CsvEncoder`.
#[derive(Debug, Clone, Default)]
pub struct CsvBuilder;

impl CsvBuilder {
    fn build(&self, part_size: usize) -> CsvEncoder {
        let mut builder = WriterBuilder::new();
        let body = PartBody::with_capacity(part_size);
        let writer = builder.buffer_capacity(part_size).from_writer(body);
        CsvEncoder { writer }
    }

    fn reset(&self, part_size: usize) -> CsvEncoder {
        let mut builder = WriterBuilder::new();
        let body = PartBody::with_capacity(part_size);
        // Don't add a header--this is a part of the same object and we wrote a
        // header row in the first part.
        let writer = builder.has_headers(false).from_writer(body);
        CsvEncoder { writer }
    }
}

/// `CsvEncoder` implements `PartEncoder` by writing items to the part in CSV
/// format.
pub struct CsvEncoder {
    writer: Writer<PartBody>,
}

impl<Item: Serialize> PartEncoder<Item> for CsvEncoder {
    type Builder = CsvBuilder;
    type Error = CsvError;

    fn build(builder: &Self::Builder, part_size: usize) -> Result<Self, Self::Error> {
        Ok(builder.build(part_size))
    }

    fn encode(&mut self, item: Item) -> Result<usize, Self::Error> {
        let before = self.writer.get_ref().size();
        self.writer.serialize(item)?;
        let after = self.writer.get_ref().size();
        Ok(after - before)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.writer.flush()?;
        Ok(())
    }

    fn reset(builder: &Self::Builder, part_size: usize) -> Result<Self, Self::Error> {
        Ok(builder.reset(part_size))
    }

    fn into_body(self) -> Result<PartBody, Self::Error> {
        Ok(self
            .writer
            .into_inner()
            .map_err(csv::IntoInnerError::into_error)?)
    }
}

impl EncodeError for CsvError {
    fn message(&self) -> String {
        self.to_string()
    }

    fn kind(&self) -> EncodeErrorKind {
        match self.kind() {
            csv::ErrorKind::Io(_) => EncodeErrorKind::Io,
            csv::ErrorKind::UnequalLengths { .. }
            | csv::ErrorKind::Utf8 { .. }
            | csv::ErrorKind::Deserialize { .. }
            | csv::ErrorKind::Serialize(_) => EncodeErrorKind::Data,
            csv::ErrorKind::Seek => EncodeErrorKind::Eof,
            _ => EncodeErrorKind::Unknown,
        }
    }
}
