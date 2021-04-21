use crate::{ClockValue, database_rules::WriterId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// The summary information for a writer that has data in a segment
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct WriterSummary {
    pub start_sequence: u64,
    pub end_sequence: u64,
    pub missing_sequence: bool,
}

/// The persistence metadata associated with a given segment
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct SegmentPersistence {
    pub location: String,
    pub time: DateTime<Utc>,
}

/// The summary information for a segment
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct SegmentSummary {
    pub size: u64,
    pub persisted: Option<SegmentPersistence>,
    pub sequenced_entries: Vec<WriterSequence>,
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize)]
pub struct WriterSequence {
    // order of these fields is important for sort order created by derive(PartialOrd)
    pub clock_value: ClockValue,
    pub writer_id: WriterId,
}
