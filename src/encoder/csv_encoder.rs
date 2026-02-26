use csv::{Error as CsvError, Writer, WriterBuilder};
use serde::Serialize;

use super::{EncodeError, EncodeErrorKind, PartEncoder};
use crate::request::{PartBody, PartNumber};

/// `CsvEncoder` implements `Encoder` with a CSV writer.
#[derive(Debug, Clone, Copy)]
pub struct CsvEncoder {
    has_header: bool,
    double_quotes: bool,
}

impl CsvEncoder {
    /// Create a new `CsvEncoder` from a configured `CsvEncoderBuilder`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether to skip writing a header row in the first part.
    ///
    /// By default a header row is written as the first row in the first part.
    pub fn skip_header(self) -> Self {
        Self { has_header: false, double_quotes: self.double_quotes }
    }

    /// When there are quotes in field data, escape them instead of writing
    /// double quotes.
    ///
    /// By default, quotes in fields are converted to `""`.  With this enabled,
    /// this instead becomes `\"`.
    pub fn escape_quotes(self) -> Self {
        Self { double_quotes: false, has_header: self.has_header }
    }

    fn csv_writer(
        self,
        wr: &mut PartBody,
        is_first: bool,
    ) -> Writer<&mut PartBody> {
        let mut builder = WriterBuilder::new();
        if is_first && wr.is_empty() {
            builder.has_headers(self.has_header)
        } else {
            // Otherwise every row would be written with a header above it.
            builder.has_headers(false)
        }
        .double_quote(self.double_quotes)
        .from_writer(wr)
    }
}

impl Default for CsvEncoder {
    fn default() -> Self {
        Self { has_header: true, double_quotes: true }
    }
}

impl<Item: Serialize> PartEncoder<Item> for CsvEncoder {
    type Error = CsvError;

    fn encode(
        &mut self,
        part: &mut PartBody,
        part_number: PartNumber,
        item: Item,
    ) -> Result<(), Self::Error> {
        let mut writer = self.csv_writer(part, part_number.is_first());
        writer.serialize(item)?;
        writer.flush()?;
        Ok(())
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
