mod csv;
pub use self::csv::{CsvCodec, CsvCodecError};

mod jsonlines;
pub use self::jsonlines::JsonlinesCodec;

pub use tokio_util::codec::BytesCodec;

use crate::AwsError;

impl From<serde_json::Error> for AwsError {
    fn from(value: serde_json::Error) -> Self {
        AwsError::Serde(value.to_string())
    }
}
