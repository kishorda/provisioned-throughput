//! The usage store chosen at startup: in memory, or ClickHouse (ADR-019).

use pt_core::UsageRecord;

use crate::clickhouse::ClickHouseUsageStore;
use crate::store::{IngestResult, MemoryUsageStore, StoredRecord, UsageError, UsageStore};

pub enum UsageBackend {
    /// Lost on restart. For tests and local runs.
    Memory(MemoryUsageStore),
    ClickHouse(ClickHouseUsageStore),
}

impl Default for UsageBackend {
    fn default() -> Self {
        UsageBackend::Memory(MemoryUsageStore::default())
    }
}

impl UsageBackend {
    pub fn kind(&self) -> &'static str {
        match self {
            UsageBackend::Memory(_) => "memory",
            UsageBackend::ClickHouse(_) => "clickhouse",
        }
    }
}

impl UsageStore for UsageBackend {
    async fn append(
        &self,
        region: &str,
        records: Vec<UsageRecord>,
        now_ms: u64,
    ) -> Result<IngestResult, UsageError> {
        match self {
            UsageBackend::Memory(s) => s.append(region, records, now_ms).await,
            UsageBackend::ClickHouse(s) => s.append(region, records, now_ms).await,
        }
    }

    async fn range(
        &self,
        tenant: &str,
        reservation: &str,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<StoredRecord>, UsageError> {
        match self {
            UsageBackend::Memory(s) => s.range(tenant, reservation, from_ms, to_ms).await,
            UsageBackend::ClickHouse(s) => s.range(tenant, reservation, from_ms, to_ms).await,
        }
    }

    async fn prune(&self, before_ms: u64) -> Result<usize, UsageError> {
        match self {
            UsageBackend::Memory(s) => s.prune(before_ms).await,
            UsageBackend::ClickHouse(s) => s.prune(before_ms).await,
        }
    }
}
