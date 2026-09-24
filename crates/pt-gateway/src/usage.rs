//! Usage record sinks.
//!
//! Production publishes to Redpanda (docs/09 §1). Here, records go to JSONL (which the same
//! ClickHouse schema can ingest) and/or are pushed to the control plane's telemetry API.

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;

use crate::config::UsageExportConfig;

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

/// Sends each record to several sinks.
pub struct TeeSink(pub Vec<Arc<dyn UsageSink>>);

impl UsageSink for TeeSink {
    fn emit(&self, record: UsageRecord) {
        if let Some((last, rest)) = self.0.split_last() {
            for s in rest {
                s.emit(record.clone());
            }
            last.emit(record);
        }
    }
}

/// Pushes records to `POST /internal/v1/usage` in batches, from a background task.
///
/// `emit` never blocks the request path. Records are buffered up to `buffer`, sent in
/// batches of `batch_size` or every `flush_interval_ms`, and retried with backoff while the
/// control plane is unreachable. Ingest de-duplicates by request id, so retries are safe.
/// When the buffer is full, new records are dropped and counted.
pub struct HttpSink {
    tx: mpsc::Sender<UsageRecord>,
    dropped: AtomicU64,
}

impl HttpSink {
    /// Start the sender. Must be called inside a Tokio runtime.
    pub fn start(config: UsageExportConfig) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(config.buffer);
        tokio::spawn(send_loop(rx, config));
        Arc::new(Self {
            tx,
            dropped: AtomicU64::new(0),
        })
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl UsageSink for HttpSink {
    fn emit(&self, record: UsageRecord) {
        if self.tx.try_send(record).is_err() {
            let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if n == 1 || n % 1_000 == 0 {
                tracing::error!(
                    dropped = n,
                    "usage export buffer full; dropping usage records"
                );
            }
        }
    }
}

async fn send_loop(mut rx: mpsc::Receiver<UsageRecord>, config: UsageExportConfig) {
    let url = format!(
        "{}/internal/v1/usage",
        config.control_plane_url.trim_end_matches('/')
    );
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("http client");
    let flush = Duration::from_millis(config.flush_interval_ms);
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        let deadline = tokio::time::Instant::now() + flush;
        while batch.len() < config.batch_size {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(r)) => batch.push(r),
                Ok(None) | Err(_) => break,
            }
        }
        let body = serde_json::json!({ "records": batch });
        let mut backoff = Duration::from_millis(500);
        loop {
            let result = http
                .post(&url)
                .bearer_auth(&config.token)
                .json(&body)
                .send()
                .await;
            match result {
                Ok(r) if r.status().is_success() => break,
                // Retrying won't fix a bad token or a rejected payload.
                Ok(r)
                    if r.status().is_client_error()
                        && r.status() != reqwest::StatusCode::TOO_MANY_REQUESTS =>
                {
                    tracing::error!(status = %r.status(), records = batch.len(), "usage export rejected; dropping batch");
                    break;
                }
                Ok(r) => {
                    tracing::warn!(status = %r.status(), retry_in = ?backoff, "usage export failed")
                }
                Err(e) => tracing::warn!(error = %e, retry_in = ?backoff, "usage export failed"),
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    }
}
