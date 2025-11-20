use crate::client::part::PartBody;
use crate::codec::{EncodeError, EncodeErrorKind, PartEncoder};

use arrow::array::RecordBatch;
use arrow::datatypes::{Fields, Schema};
use parquet::arrow::ArrowWriter;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

/// Builder for `ParquetEncoder`.
#[derive(Debug, Clone)]
pub struct ParquetBuilder {
    properties: WriterProperties,
    schema: Schema,
}

impl ParquetBuilder {
    /// Initialize the builder from fields to build an Arrow [`Schema`].
    ///
    /// [`Schema`]: arrow::datatypes::Schema
    pub fn new<F: Into<Fields>>(fields: F) -> Self {
        let schema = Schema::new(fields);
        Self {
            schema,
            properties: WriterProperties::default(),
        }
    }

    /// Set the properties for [`ArrowWriter`].
    ///
    /// [`ArrowWriter`]: parquet::arrow::ArrowWriter
    pub fn with_properties(self, properties: WriterProperties) -> Self {
        Self { properties, ..self }
    }

    fn build(&self, part_size: usize) -> Result<ParquetEncoder, ParquetError> {
        let body = PartBody::with_capacity(part_size);
        let writer = ArrowWriter::try_new(
            body,
            Arc::new(self.schema.clone()),
            Some(self.properties.clone()),
        )?;
        Ok(ParquetEncoder { writer })
    }
}

/// `ParquetEncoder` implements `PartEncoder` by writing parquet records.
pub struct ParquetEncoder {
    writer: ArrowWriter<PartBody>,
}

impl PartEncoder<&RecordBatch> for ParquetEncoder {
    type Builder = ParquetBuilder;
    type Error = ParquetError;

    fn build(builder: &Self::Builder, part_size: usize) -> Result<Self, Self::Error> {
        builder.build(part_size)
    }

    fn encode(&mut self, item: &RecordBatch) -> Result<usize, Self::Error> {
        let bytes = item.get_array_memory_size();
        self.writer.write(item)?;
        Ok(bytes)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.writer.flush()
    }

    fn into_body(mut self) -> Result<PartBody, Self::Error> {
        self.writer.finish()?;
        let body = self.writer.into_inner()?;
        Ok(body)
    }
}

impl EncodeError for ParquetError {
    fn message(&self) -> String {
        self.to_string()
    }

    fn kind(&self) -> EncodeErrorKind {
        match self {
            ParquetError::EOF(_) => EncodeErrorKind::Eof,
            ParquetError::NeedMoreData(_) | ParquetError::NeedMoreDataRange(_) => {
                EncodeErrorKind::Data
            }
            _ => EncodeErrorKind::Unknown,
        }
    }
}
