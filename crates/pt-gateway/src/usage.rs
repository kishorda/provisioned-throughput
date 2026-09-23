//! Usage record sinks.
//!
//! Production publishes to Redpanda (docs/09 §1). P0 writes JSONL, which the same
//! ClickHouse schema can ingest.

use std::io::Write;
use std::sync::Mutex;

use pt_core::UsageRecord;

pub trait UsageSink: Send + Sync {
    fn emit(&self, record: UsageRecord);
}

/// Writes one JSON record per line.
pub struct JsonlSink {
    out: Mutex<Box<dyn Write + Send>>,
}

impl JsonlSink {
    pub fn new(out: Box<dyn Write + Send>) -> Self {
        Self {
            out: Mutex::new(out),
        }
    }

    pub fn stdout() -> Self {
        Self::new(Box::new(std::io::stdout()))
    }

    pub fn file(path: &str) -> std::io::Result<Self> {
        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self::new(Box::new(f)))
    }
}

impl UsageSink for JsonlSink {
    fn emit(&self, record: UsageRecord) {
        let mut out = self.out.lock().unwrap_or_else(|e| e.into_inner());
        let result = serde_json::to_writer(&mut *out, &record)
            .map_err(std::io::Error::from)
            .and_then(|()| out.write_all(b"\n"))
            .and_then(|()| out.flush());
        if let Err(e) = result {
            tracing::error!(error = %e, request_id = %record.request_id, "failed to write usage record");
        }
    }
}

/// Keeps records in memory. For tests.
#[derive(Default)]
pub struct MemorySink {
    records: Mutex<Vec<UsageRecord>>,
}

impl MemorySink {
    pub fn records(&self) -> Vec<UsageRecord> {
        self.records
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

impl UsageSink for MemorySink {
    fn emit(&self, record: UsageRecord) {
        self.records
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(record);
    }
}
