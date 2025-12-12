use crate::AWS_MIN_PART_SIZE;
use crate::client::part::PartBody;
use crate::codec::{EncodeError, EncodeErrorKind, PartEncoder};

use bytesize::ByteSize;
use csv::{Error as CsvError, Writer, WriterBuilder};
use serde::Serialize;

/// `CsvEncoder` implements `PartEncoder` by writing items to the part in CSV
/// format.
#[derive(Debug)]
pub struct CsvEncoder {
    writer: Writer<PartBody>,
    write_header: bool,
    capacity: u64,
}

impl CsvEncoder {
    /// Write a header row from the item as the first line in the upload.
    pub fn with_header(self) -> Self {
        Self {
            write_header: true,
            ..self
        }
    }

    /// Initial capacity allocated for the CSV writer.
    pub fn with_capacity(self, capacity: ByteSize) -> Self {
        Self {
            capacity: capacity.as_u64(),
            ..self
        }
    }
}

impl Default for CsvEncoder {
    fn default() -> Self {
        let capacity = AWS_MIN_PART_SIZE.as_u64();
        let part = PartBody::with_capacity(capacity as usize);
        let mut builder = WriterBuilder::new();
        let writer = builder
            .buffer_capacity(capacity as usize)
            .has_headers(false)
            .from_writer(part);

        Self {
            writer,
            write_header: false,
            capacity,
        }
    }
}

impl<Item: Serialize> PartEncoder<Item> for CsvEncoder {
    type Error = CsvError;

    fn restore(&self) -> Result<Self, Self::Error> {
        let cap = self.writer.get_ref().capacity();
        let part = PartBody::with_capacity(cap);
        let mut builder = WriterBuilder::new();
        let writer = if self.write_header {
            builder
                .buffer_capacity(self.capacity as usize)
                .from_writer(part)
        } else {
            builder
                .buffer_capacity(self.capacity as usize)
                .has_headers(false)
                .from_writer(part)
        };

        Ok(Self {
            writer,
            write_header: self.write_header,
            capacity: self.capacity,
        })
    }

    fn encode(&mut self, item: Item) -> Result<usize, Self::Error> {
        let before = self.writer.get_ref().size();
        self.writer.serialize(item)?;
        self.writer.flush()?;
        let after = self.writer.get_ref().size();
        Ok(after - before)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.writer.flush()?;
        Ok(())
    }

    fn into_body(self) -> Result<PartBody, Self::Error> {
        match self.writer.into_inner() {
            Ok(body) => Ok(body),
            Err(e) => Err(e.into_error())?,
        }
    }

    fn clear(&self) -> Result<Self, Self::Error> {
        let cap = self.writer.get_ref().capacity();
        let part = PartBody::with_capacity(cap);
        let mut builder = WriterBuilder::new();
        let writer = builder
            .buffer_capacity(self.capacity as usize)
            .has_headers(false)
            .from_writer(part);

        Ok(Self {
            writer,
            write_header: self.write_header,
            capacity: self.capacity,
        })
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
