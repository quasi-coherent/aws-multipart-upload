use crate::codec::{EncodeError, EncodeErrorKind, EncodedPart};
use crate::config::DEFAULT_MAX_PART_SIZE;

use crate::sdk::PartBody;

use serde::Serialize;
use std::io::Write;

/// `JsonLinesEncoder` implements [`EncodedPart`] by writing lines of JSON to
/// the part.
#[derive(Debug, Clone)]
pub struct JsonLinesEncoder {
    writer: PartBody,
}

impl JsonLinesEncoder {
    /// Create a new `JsonLinesEncoder` with part capacity.
    /// The default capacity is 10MiB.
    pub fn new(capacity: impl Into<Option<usize>>) -> Self {
        let capacity = capacity.into().unwrap_or(DEFAULT_MAX_PART_SIZE);
        Self {
            writer: PartBody::with_capacity(capacity),
        }
    }
}

impl<Item: Serialize> EncodedPart<Item> for JsonLinesEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: Item) -> Result<(), Self::Error> {
        serde_json::to_writer(&mut self.writer, &item)?;
        self.writer.write(b"\n")?;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn current_size(&self) -> usize {
        self.writer.size()
    }

    fn to_part_body(&mut self) -> Result<PartBody, Self::Error> {
        let inner = self.writer.split();
        Ok(PartBody::new(inner))
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
