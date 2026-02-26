use tokio_util::codec::Encoder;

use super::{EncodeError, PartEncoder};
use crate::request::{PartBody, PartNumber};

/// `FramedEncoder` implements `PartEncoder` for [`FramedWrite`] types.
///
/// [`FramedWrite`]: tokio_util::codec::FramedWrite
#[derive(Debug, Clone, Copy, Default)]
pub struct FramedEncoder<E> {
    encoder: E,
}

impl<E> FramedEncoder<E> {
    /// Create a new `FramedEncoder`.
    pub fn new(encoder: E) -> FramedEncoder<E> {
        Self { encoder }
    }
}

impl<E, Item> PartEncoder<Item> for FramedEncoder<E>
where
    E: Encoder<Item>,
    E::Error: EncodeError,
{
    type Error = E::Error;

    fn encode(
        &mut self,
        part: &mut PartBody,
        _part_number: PartNumber,
        item: Item,
    ) -> Result<(), Self::Error> {
        self.encoder.encode(item, part)
    }
}
