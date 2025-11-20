use crate::client::part::PartBody;
use crate::codec::PartEncoder;

use bytes::BufMut as _;
use std::convert::Infallible;
use std::ops::DerefMut;

/// Builder for `LinesEncoder`.
#[derive(Debug, Clone, Default)]
pub struct LinesBuilder {
    header: Option<String>,
}

impl LinesBuilder {
    /// Set the header to write as the first row of the first part.
    pub fn with_header<T: Into<String>>(header: T) -> Self {
        Self {
            header: Some(header.into()),
        }
    }

    fn build(&self, part_size: usize) -> LinesEncoder {
        let mut writer = PartBody::with_capacity(part_size);
        if let Some(header) = self.header.as_deref() {
            writer.put(header.as_bytes());
        }
        LinesEncoder { writer }
    }

    fn reset(&self, part_size: usize) -> LinesEncoder {
        LinesEncoder {
            writer: PartBody::with_capacity(part_size),
        }
    }
}

/// `LinesEncoder` implements `PartEncoder` by writing the input items delimited
/// by the newline character `\n` on all platforms.
#[derive(Debug, Clone)]
pub struct LinesEncoder {
    writer: PartBody,
}

impl<Item: AsRef<str>> PartEncoder<Item> for LinesEncoder {
    type Builder = LinesBuilder;
    type Error = Infallible;

    fn build(builder: &Self::Builder, part_size: usize) -> Result<Self, Self::Error> {
        Ok(builder.build(part_size))
    }

    fn encode(&mut self, item: Item) -> Result<usize, Self::Error> {
        let item = item.as_ref();
        let bytes = item.len();
        self.writer.deref_mut().reserve(bytes + 1);
        self.writer.deref_mut().put(item.as_bytes());
        self.writer.deref_mut().put_u8(b'\n');
        Ok(bytes + 1)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn reset(builder: &Self::Builder, part_size: usize) -> Result<Self, Self::Error> {
        Ok(builder.reset(part_size))
    }

    fn into_body(self) -> Result<PartBody, Self::Error> {
        Ok(self.writer)
    }
}
