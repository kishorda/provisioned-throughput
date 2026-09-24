//! Persistence. [`MemoryStore`] backs local runs and tests. The CockroachDB schema for the
//! production store is in `migrations/`.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;

use crate::model::{ProvisionedThroughput, RegionIncident};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    #[error("{0} already exists")]
    AlreadyExists(String),
    #[error("{0} was modified concurrently")]
    VersionConflict(String),
    #[error("{0} not found")]
    NotFound(String),
}

/// What an idempotency key was first used for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyRecord {
    pub resource_id: String,
    /// SHA-256 of the canonical request body, to detect a key reused for a different request.
    pub fingerprint: String,
}

pub trait Store: Send + Sync + 'static {
    fn insert(
        &self,
        pt: ProvisionedThroughput,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> impl Future<Output = Option<ProvisionedThroughput>> + Send;

    fn list(&self, tenant: &str) -> impl Future<Output = Vec<ProvisionedThroughput>> + Send;

    /// Live resources across all tenants, for the lifecycle loop.
    fn list_live(&self) -> impl Future<Output = Vec<ProvisionedThroughput>> + Send;

    /// Replace `pt` if the stored version is `expected_version`.
    fn update(
        &self,
        pt: ProvisionedThroughput,
        expected_version: u64,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    fn idempotency_get(
        &self,
        tenant: &str,
        key: &str,
    ) -> impl Future<Output = Option<IdempotencyRecord>> + Send;

    /// Record a key. Fails if the key already exists.
    fn idempotency_put(
        &self,
        tenant: &str,
        key: &str,
        record: IdempotencyRecord,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    fn insert_incident(
        &self,
        incident: RegionIncident,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Replace an existing incident.
    fn update_incident(
        &self,
        incident: RegionIncident,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// All incidents, oldest first.
    fn list_incidents(&self) -> impl Future<Output = Vec<RegionIncident>> + Send;
}

#[derive(Default)]
struct Inner {
    by_id: HashMap<String, ProvisionedThroughput>,
    idempotency: HashMap<(String, String), IdempotencyRecord>,
    incidents: Vec<RegionIncident>,
}

#[derive(Default)]
pub struct MemoryStore {
    inner: Mutex<Inner>,
}

impl MemoryStore {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Store for MemoryStore {
    async fn insert(&self, pt: ProvisionedThroughput) -> Result<(), StoreError> {
        let mut inner = self.lock();
        if inner.by_id.contains_key(&pt.id) {
            return Err(StoreError::AlreadyExists(pt.id));
        }
        inner.by_id.insert(pt.id.clone(), pt);
        Ok(())
    }

    async fn get(&self, tenant: &str, id: &str) -> Option<ProvisionedThroughput> {
        self.lock()
            .by_id
            .get(id)
            .filter(|pt| pt.tenant == tenant)
            .cloned()
    }

    async fn list(&self, tenant: &str) -> Vec<ProvisionedThroughput> {
        let mut out: Vec<_> = self
            .lock()
            .by_id
            .values()
            .filter(|pt| pt.tenant == tenant)
            .cloned()
            .collect();
        out.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.id.cmp(&b.id))
        });
        out
    }

    async fn list_live(&self) -> Vec<ProvisionedThroughput> {
        self.lock()
            .by_id
            .values()
            .filter(|pt| pt.state.is_live())
            .cloned()
            .collect()
    }

    async fn update(
        &self,
        pt: ProvisionedThroughput,
        expected_version: u64,
    ) -> Result<(), StoreError> {
        let mut inner = self.lock();
        match inner.by_id.get(&pt.id) {
            None => Err(StoreError::NotFound(pt.id)),
            Some(current) if current.version != expected_version => {
                Err(StoreError::VersionConflict(pt.id))
            }
            Some(_) => {
                inner.by_id.insert(pt.id.clone(), pt);
                Ok(())
            }
        }
    }

    async fn idempotency_get(&self, tenant: &str, key: &str) -> Option<IdempotencyRecord> {
        self.lock()
            .idempotency
            .get(&(tenant.to_string(), key.to_string()))
            .cloned()
    }

    async fn idempotency_put(
        &self,
        tenant: &str,
        key: &str,
        record: IdempotencyRecord,
    ) -> Result<(), StoreError> {
        let mut inner = self.lock();
        let k = (tenant.to_string(), key.to_string());
        if inner.idempotency.contains_key(&k) {
            return Err(StoreError::AlreadyExists(format!("idempotency key {key}")));
        }
        inner.idempotency.insert(k, record);
        Ok(())
    }

    async fn insert_incident(&self, incident: RegionIncident) -> Result<(), StoreError> {
        let mut inner = self.lock();
        if inner.incidents.iter().any(|i| i.id == incident.id) {
            return Err(StoreError::AlreadyExists(incident.id));
        }
        let pos = inner
            .incidents
            .partition_point(|i| i.started_at <= incident.started_at);
        inner.incidents.insert(pos, incident);
        Ok(())
    }

    async fn update_incident(&self, incident: RegionIncident) -> Result<(), StoreError> {
        let mut inner = self.lock();
        match inner.incidents.iter_mut().find(|i| i.id == incident.id) {
            Some(slot) => {
                *slot = incident;
                Ok(())
            }
            None => Err(StoreError::NotFound(incident.id)),
        }
    }

    async fn list_incidents(&self) -> Vec<RegionIncident> {
        self.lock().incidents.clone()
    }
}
