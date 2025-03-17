use bytes::{BufMut, BytesMut};
use serde::Serialize;
use tokio_util::codec::Encoder;

use crate::AwsError;

#[derive(Debug, thiserror::Error)]
pub enum JsonlinesCodecError {
    #[error("serde_json encoding error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("io error {0}")]
    Io(#[from] std::io::Error),
}

impl From<JsonlinesCodecError> for AwsError {
    fn from(value: JsonlinesCodecError) -> Self {
        Self::Codec(value.to_string())
    }
}

/// An encoder that encodes items as lines of JSON.
#[derive(Debug, Clone, Default)]
pub struct JsonlinesCodec {
    _private: (),
}

impl JsonlinesCodec {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<Item> Encoder<Item> for JsonlinesCodec
where
    Item: Serialize,
{
    type Error = JsonlinesCodecError;

    fn encode(&mut self, item: Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytevec = serde_json::to_vec(&item)?;
        dst.reserve(&bytevec.len() + 1);
        dst.put(bytevec.as_slice());
        dst.put_u8(b'\n');
        Ok(())
    }
}
