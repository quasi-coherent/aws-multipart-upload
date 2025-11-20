use crate::client::part::PartBody;
use crate::codec::PartEncoder;

use bytes::BufMut as _;
use serde::Serialize;
use std::ops::DerefMut;

/// Builder for `JsonLinesEncoder`.
#[derive(Debug, Clone, Default)]
pub struct JsonLinesBuilder;

impl JsonLinesBuilder {
    fn build(&self, part_size: usize) -> JsonLinesEncoder {
        JsonLinesEncoder {
            writer: PartBody::with_capacity(part_size),
        }
    }
}

/// `JsonLinesEncoder` implements `PartEncoder` by writing lines of JSON to the
/// part.
#[derive(Debug, Clone)]
pub struct JsonLinesEncoder {
    writer: PartBody,
}

impl<Item: Serialize> PartEncoder<Item> for JsonLinesEncoder {
    type Builder = JsonLinesBuilder;
    type Error = serde_json::Error;

    fn build(builder: &Self::Builder, part_size: usize) -> Result<Self, Self::Error> {
        Ok(builder.build(part_size))
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
