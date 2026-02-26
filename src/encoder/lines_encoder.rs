use std::convert::Infallible;

use bytes::BufMut as _;

use super::PartEncoder;
use crate::request::{PartBody, PartNumber};

/// `LinesEncoder` implements `Encoder` by writing input items delimited by the
/// newline character `\n` on all platforms.
#[derive(Debug, Clone, Default)]
pub struct LinesEncoder {
    header: Option<String>,
}

impl LinesEncoder {
    /// Creates a new `LinesEncoder` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Write the given header row as the first row of the first part in each
    /// upload.
    pub fn with_header<T: Into<String>>(self, header: T) -> Self {
        Self { header: Some(header.into()) }
    }
}

impl<Item: AsRef<str>> PartEncoder<Item> for LinesEncoder {
    type Error = Infallible;

    fn encode(
        &mut self,
        part: &mut PartBody,
        part_number: PartNumber,
        item: Item,
    ) -> Result<(), Self::Error> {
        let item = item.as_ref().as_bytes();
        if let Some(header) = self.header.as_ref()
            && part.is_empty()
            && part_number.is_first()
        {
            let header = header.as_bytes();
            // header_bytes\nitem_bytes\n
            part.reserve(item.len() + header.len() + 2);
            part.put(header);
            part.put_u8(b'\n');
        } else {
            part.reserve(item.len() + 1);
        }
        part.put(item);
        part.put_u8(b'\n');
        Ok(())
    }
}
