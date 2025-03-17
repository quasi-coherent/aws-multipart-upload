use bytes::{BufMut, BytesMut};
use csv::{Terminator, Writer as CsvWriter, WriterBuilder};
use serde::Serialize;
use std::io::Write;
use tokio_util::codec::Encoder;

use crate::AwsError;

#[derive(Debug, thiserror::Error)]
pub enum CsvCodecError {
    #[error("csv error in encoding bytes {0}")]
    CsvWriter(#[from] csv::Error),
    #[error("io error {0}")]
    Io(#[from] std::io::Error),
}

impl From<CsvCodecError> for AwsError {
    fn from(value: CsvCodecError) -> Self {
        Self::Codec(value.to_string())
    }
}

/// An encoder that encodes items as CSV.
#[derive(Debug, Clone, Default)]
pub struct CsvCodec {
    pub has_headers: bool,
    pub term: Terminator,
}

impl CsvCodec {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_headers(mut self) -> Self {
        self.has_headers = true;
        self
    }

    pub fn set_terminator(mut self, term: Terminator) -> Self {
        self.term = term;
        self
    }

    pub fn from_writer<W: Write>(&self, wtr: W) -> CsvWriter<W> {
        WriterBuilder::new()
            .has_headers(self.has_headers)
            .terminator(self.term)
            .from_writer(wtr)
    }
}

impl<Item> Encoder<Item> for CsvCodec
where
    Item: Serialize,
{
    type Error = CsvCodecError;

    fn encode(&mut self, item: Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let wtr: &mut [u8] = &mut [];
        let mut csv = self.from_writer(wtr);
        // This writes a CSV row with newline or CLRF character as the line
        // terminator, so there is no need to reserve "+1" and write out the
        // line terminating character ourselves.
        csv.serialize(item)?;
        csv.flush()?;
        let bytes = csv.into_inner().map_err(|e| e.into_error())?;
        dst.reserve(bytes.len());
        dst.put_slice(bytes);
        Ok(())
    }
}
