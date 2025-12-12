use crate::AWS_MIN_PART_SIZE;
use crate::client::part::PartBody;
use crate::codec::PartEncoder;

use bytes::BufMut as _;
use std::convert::Infallible;
use std::ops::DerefMut;

/// `LinesEncoder` implements `PartEncoder` by writing the input items delimited
/// by the newline character `\n` on all platforms.
#[derive(Debug, Clone)]
pub struct LinesEncoder {
    writer: PartBody,
    header: Option<String>,
}

impl LinesEncoder {
    /// Set the header to write as the first row of the first part.
    pub fn with_header<T: Into<String>>(self, header: T) -> Self {
        Self {
            header: Some(header.into()),
            ..self
        }
    }
}

impl Default for LinesEncoder {
    fn default() -> Self {
        Self {
            writer: PartBody::with_capacity(AWS_MIN_PART_SIZE.as_u64() as usize),
            header: None,
        }
    }
}

impl<Item: AsRef<str>> PartEncoder<Item> for LinesEncoder {
    type Error = Infallible;

    fn restore(&self) -> Result<Self, Self::Error> {
        let capacity = self.writer.capacity();
        let mut writer = PartBody::with_capacity(capacity);
        if let Some(h) = self.header.as_deref() {
            writer.put(h.as_bytes());
        }
        Ok(Self {
            writer,
            header: self.header.clone(),
        })
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

    fn into_body(self) -> Result<PartBody, Self::Error> {
        Ok(self.writer)
    }

    fn clear(&self) -> Result<Self, Self::Error> {
        let capacity = self.writer.capacity();
        Ok(Self {
            writer: PartBody::with_capacity(capacity),
            header: self.header.clone(),
        })
    }
}
