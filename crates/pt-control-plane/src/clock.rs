//! Time source, injectable so lifecycle rules are testable.

use std::sync::{Arc, Mutex};

use jiff::{SignedDuration, Timestamp};

pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Timestamp;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        Timestamp::now()
    }
}

/// A clock that only moves when told to. For tests.
#[derive(Debug, Clone)]
pub struct ManualClock(Arc<Mutex<Timestamp>>);

impl ManualClock {
    pub fn new(start: Timestamp) -> Self {
        Self(Arc::new(Mutex::new(start)))
    }

    pub fn advance(&self, by: SignedDuration) {
        let mut t = self.0.lock().unwrap_or_else(|e| e.into_inner());
        *t += by;
    }

    pub fn set(&self, to: Timestamp) {
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = to;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        *self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}
