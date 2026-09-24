//! Usage record storage. In-memory now; ClickHouse later (docs/09 §1), behind the same trait.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;

use pt_core::UsageRecord;
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StoredRecord {
    /// Region whose gateway sent the record, from its push token.
    pub region: String,
    /// When the request was received, Unix milliseconds. Falls back to ingest time.
    pub at_ms: u64,
    pub record: UsageRecord,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct IngestResult {
    pub accepted: usize,
    /// Already stored (same `request_id`). Retries are safe.
    pub duplicates: usize,
}

/// The usage store couldn't be reached or failed. Retryable. Never treat it as "no usage":
/// invoices and SLA credits would silently lose data.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("usage store unavailable: {0}")]
pub struct UsageError(pub String);

pub trait UsageStore: Send + Sync + 'static {
    /// Store records, skipping any whose `request_id` is already stored.
    fn append(
        &self,
        region: &str,
        records: Vec<UsageRecord>,
        now_ms: u64,
    ) -> impl Future<Output = Result<IngestResult, UsageError>> + Send;

    /// One reservation's records in `[from_ms, to_ms)`, oldest first, each `request_id`
    /// once.
    fn range(
        &self,
        tenant: &str,
        reservation: &str,
        from_ms: u64,
        to_ms: u64,
    ) -> impl Future<Output = Result<Vec<StoredRecord>, UsageError>> + Send;

    /// Drop records older than `before_ms`. Returns how many were removed, if known.
    fn prune(&self, before_ms: u64) -> impl Future<Output = Result<usize, UsageError>> + Send;
}

#[derive(Default)]
struct Inner {
    /// Sorted by `at_ms` per reservation.
    by_reservation: HashMap<String, Vec<StoredRecord>>,
    /// request_id → at_ms, for de-duplication until pruned.
    seen: HashMap<Uuid, u64>,
}

#[derive(Default)]
pub struct MemoryUsageStore {
    inner: Mutex<Inner>,
}

impl MemoryUsageStore {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn len(&self) -> usize {
        self.lock().seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl UsageStore for MemoryUsageStore {
    async fn append(
        &self,
        region: &str,
        records: Vec<UsageRecord>,
        now_ms: u64,
    ) -> Result<IngestResult, UsageError> {
        let mut inner = self.lock();
        let mut result = IngestResult::default();
        for record in records {
            let at_ms = if record.received_at_ms == 0 {
                now_ms
            } else {
                record.received_at_ms
            };
            if inner.seen.contains_key(&record.request_id) {
                result.duplicates += 1;
                continue;
            }
            inner.seen.insert(record.request_id, at_ms);
            let list = inner
                .by_reservation
                .entry(record.reservation.clone())
                .or_default();
            let pos = list.partition_point(|r| r.at_ms <= at_ms);
            list.insert(
                pos,
                StoredRecord {
                    region: region.to_string(),
                    at_ms,
                    record,
                },
            );
            result.accepted += 1;
        }
        Ok(result)
    }

    async fn range(
        &self,
        tenant: &str,
        reservation: &str,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<StoredRecord>, UsageError> {
        let inner = self.lock();
        let Some(list) = inner.by_reservation.get(reservation) else {
            return Ok(vec![]);
        };
        let start = list.partition_point(|r| r.at_ms < from_ms);
        let end = list.partition_point(|r| r.at_ms < to_ms);
        Ok(list[start..end]
            .iter()
            .filter(|r| r.record.tenant == tenant)
            .cloned()
            .collect())
    }

    async fn prune(&self, before_ms: u64) -> Result<usize, UsageError> {
        let mut inner = self.lock();
        let mut removed = 0;
        for list in inner.by_reservation.values_mut() {
            let cut = list.partition_point(|r| r.at_ms < before_ms);
            removed += cut;
            list.drain(..cut);
        }
        inner.by_reservation.retain(|_, l| !l.is_empty());
        inner.seen.retain(|_, at| *at >= before_ms);
        Ok(removed)
    }
}
