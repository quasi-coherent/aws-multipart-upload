use bytes::BufMut as _;
use serde::Serialize;

use super::PartEncoder;
use crate::request::{PartBody, PartNumber};

/// `JsonLinesEncoder` implements `Encoder` by writing items as lines of JSON to
/// the part.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonLinesEncoder {
    _p: (),
}

impl JsonLinesEncoder {
    /// Create a new `JsonLinesEncoder`.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<Item: Serialize> PartEncoder<Item> for JsonLinesEncoder {
    type Error = serde_json::Error;

    fn encode(
        &mut self,
        part: &mut PartBody,
        _part_number: PartNumber,
        item: Item,
    ) -> Result<(), Self::Error> {
        let item = serde_json::to_vec(&item)?;
        part.reserve(item.len() + 1);
        part.put(item.as_ref());
        part.put_u8(b'\n');
        Ok(())
    }
}
