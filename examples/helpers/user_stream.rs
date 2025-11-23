use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, stream};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Outcome {
    Success,
    Deny,
}

/// An item in a message stream we wish to archive in S3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserLogin {
    user_id: u64,
    display_name: String,
    timestamp: DateTime<Utc>,
    outcome: Outcome,
}

impl UserLogin {
    pub fn stream() -> impl Stream<Item = UserLogin> {
        stream::iter(0..).map(|n| UserLogin {
            user_id: n % 50,
            display_name: format!("user_{}", n % 50),
            timestamp: Utc::now(),
            outcome: if n % 24 == 0 {
                Outcome::Deny
            } else {
                Outcome::Success
            },
        })
    }
}

impl std::fmt::Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Success => write!(f, "SUCCESS"),
            Self::Deny => write!(f, "DENY"),
        }
    }
}
