use crate::client::part::PartBody;
use crate::codec::{EncodeError, EncodeErrorKind, PartEncoder};

use csv::{Error as CsvError, Writer, WriterBuilder};
use serde::Serialize;

/// Builder for `CsvEncoder`.
#[derive(Debug, Clone, Default)]
pub struct CsvBuilder;

impl CsvBuilder {
    fn build(&self, part_size: usize) -> CsvEncoder {
        let writer = PartBody::with_capacity(part_size);
        CsvEncoder {
            writer,
            write_header: true,
        }
    }

    fn reset(&self, part_size: usize) -> CsvEncoder {
        let writer = PartBody::with_capacity(part_size);
        // Don't add a header--this is a part of the same object and we wrote a
        // header row in the first part.
        CsvEncoder {
            writer,
            write_header: false,
        }
    }
}

/// `CsvEncoder` implements `PartEncoder` by writing items to the part in CSV
/// format.
pub struct CsvEncoder {
    writer: PartBody,
    write_header: bool,
}

impl<Item: Serialize> PartEncoder<Item> for CsvEncoder {
    type Builder = CsvBuilder;
    type Error = CsvError;

    fn build(builder: &Self::Builder, part_size: usize) -> Result<Self, Self::Error> {
        Ok(builder.build(part_size))
    }

    fn encode(&mut self, item: Item) -> Result<usize, Self::Error> {
        let before = self.writer.size();
        if self.write_header {
            self.write_header = false;
            let mut wtr = Writer::from_writer(&mut self.writer);
            wtr.serialize(item)?;
            wtr.flush()?;
        } else {
            let mut builder = WriterBuilder::new();
            let mut wtr = builder.has_headers(false).from_writer(&mut self.writer);
            wtr.serialize(item)?;
            wtr.flush()?;
        }
        let after = self.writer.size();
        Ok(after - before)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn reset(builder: &Self::Builder, part_size: usize) -> Result<Self, Self::Error> {
        Ok(builder.reset(part_size))
    }

    fn into_body(self) -> Result<PartBody, Self::Error> {
        Ok(self.writer)
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
