use crate::codec::{EncodeError, EncodeErrorKind, EncodedPart};
use crate::config::DEFAULT_MAX_PART_SIZE;
use crate::sdk::PartBody;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::errors::ParquetError;

/// `ParquetEncoder` implements [`EncodedPart`] by writing parquet records.
pub struct ParquetEncoder {
    schema: SchemaRef,
    options: ArrowWriterOptions,
    writer: ArrowWriter<PartBody>,
    capacity: usize,
}

impl ParquetEncoder {
    pub fn try_new(
        capacity: impl Into<Option<usize>>,
        schema: SchemaRef,
        options: ArrowWriterOptions,
    ) -> Result<Self, ParquetError> {
        let capacity = capacity.into().unwrap_or(DEFAULT_MAX_PART_SIZE);
        let inner = PartBody::with_capacity(capacity);
        let writer = ArrowWriter::try_new_with_options(inner, schema.clone(), options.clone())?;
        Ok(Self {
            schema,
            options,
            writer,
            capacity,
        })
    }
}

impl EncodedPart<&RecordBatch> for ParquetEncoder {
    type Error = ParquetError;

    fn encode(&mut self, item: &RecordBatch) -> Result<(), Self::Error> {
        self.writer.write(item)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.writer.flush()
    }

    fn current_size(&self) -> usize {
        self.writer.bytes_written()
    }

    fn to_part_body(&mut self) -> Result<PartBody, Self::Error> {
        let inner = PartBody::with_capacity(self.capacity);
        let new_writer =
            ArrowWriter::try_new_with_options(inner, self.schema.clone(), self.options.clone())?;
        let writer = std::mem::replace(&mut self.writer, new_writer);
        let body = writer.into_inner()?;

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
