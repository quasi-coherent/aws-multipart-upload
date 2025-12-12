use crate::AWS_MIN_PART_SIZE;
use crate::client::part::PartBody;
use crate::codec::PartEncoder;

use bytes::BufMut as _;
use serde::Serialize;
use std::ops::DerefMut;

/// `JsonLinesEncoder` implements `PartEncoder` by writing lines of JSON to the
/// part.
#[derive(Debug, Clone)]
pub struct JsonLinesEncoder {
    writer: PartBody,
}

impl JsonLinesEncoder {
    /// Create a `JsonLinesEncoder`.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for JsonLinesEncoder {
    fn default() -> Self {
        Self {
            writer: PartBody::with_capacity(AWS_MIN_PART_SIZE.as_u64() as usize),
        }
    }
}

impl<Item: Serialize> PartEncoder<Item> for JsonLinesEncoder {
    type Error = serde_json::Error;

    fn restore(&self) -> Result<Self, Self::Error> {
        let capacity = self.writer.capacity();
        Ok(Self {
            writer: PartBody::with_capacity(capacity),
        })
    }

    fn encode(&mut self, item: Item) -> Result<usize, Self::Error> {
        let it = serde_json::to_vec(&item)?;
        let bytes = it.len();
        self.writer.deref_mut().reserve(bytes + 1);
        self.writer.deref_mut().put(it.as_ref());
        self.writer.deref_mut().put_u8(b'\n');
        Ok(bytes + 1)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn into_body(self) -> Result<PartBody, Self::Error> {
        Ok(self.writer)
    }
}
