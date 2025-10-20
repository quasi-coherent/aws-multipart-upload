use crate::codec::{EncodeError, EncodeErrorKind, EncodedPart};
use crate::config::DEFAULT_MAX_PART_SIZE;
use crate::sdk::PartBody;

use csv::{Error as CsvError, Writer, WriterBuilder};
use serde::Serialize;

/// `CsvEncoder` implements [`EncodedPart`] by writing items to the part in CSV
/// format.
///
/// Build this from a configured [`WriterBuilder`].
pub struct CsvEncoder {
    builder: WriterBuilder,
    writer: Writer<PartBody>,
    capacity: usize,
}

impl CsvEncoder {
    /// Create a new `CsvEncoder` from a [`WriterBuilder`] and part capacity.
    /// The default capacity is 10MiB.
    pub fn new(capacity: impl Into<Option<usize>>, builder: WriterBuilder) -> Self {
        let capacity = capacity.into().unwrap_or(DEFAULT_MAX_PART_SIZE);
        let body = PartBody::with_capacity(capacity);
        let writer = builder.from_writer(body);

        Self {
            builder,
            writer,
            capacity,
        }
    }
}

impl<Item: Serialize> EncodedPart<Item> for CsvEncoder {
    type Error = CsvError;

    fn encode(&mut self, item: Item) -> Result<(), Self::Error> {
        self.writer.serialize(item)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.writer.flush()?;
        Ok(())
    }

    fn current_size(&self) -> usize {
        self.writer.get_ref().size()
    }

    fn to_part_body(&mut self) -> Result<PartBody, Self::Error> {
        let body = PartBody::with_capacity(self.capacity);
        let new_writer = self.builder.from_writer(body);
        let writer = std::mem::replace(&mut self.writer, new_writer);
        let body = writer.into_inner().map_err(|e| e.into_error())?;
        Ok(body)
    }
}

impl EncodeError for CsvError {
    fn message(&self) -> String {
        self.to_string()
    }

    fn kind(&self) -> EncodeErrorKind {
        match self.kind() {
            csv::ErrorKind::Io(_) => EncodeErrorKind::Io,
            csv::ErrorKind::UnequalLengths { .. } => EncodeErrorKind::Data,
            csv::ErrorKind::Utf8 { .. }
            | csv::ErrorKind::Deserialize { .. }
            | csv::ErrorKind::Serialize(_) => EncodeErrorKind::Data,
            csv::ErrorKind::Seek => EncodeErrorKind::Eof,
            _ => EncodeErrorKind::Unknown,
        }
    }
}
