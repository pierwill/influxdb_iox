use serde::{Deserialize, Serialize};

#[derive(Debug, PartialOrd, PartialEq, Ord, Eq, Copy, Clone, Default, Serialize, Deserialize)]
pub struct ClockValue(u64);

impl ClockValue {
    pub fn get(&self) -> u64 {
        self.0
    }

    pub fn new(v: u64) -> Self {
        Self(v)
    }
}
