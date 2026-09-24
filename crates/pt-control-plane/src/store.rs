//! Persistence. [`MemoryStore`] backs local runs and tests. [`crate::sql::SqlStore`] is the
//! durable store over the Postgres protocol (CockroachDB in production, PostgreSQL works
//! too). Its schema is in `migrations/`.
//!
//! Reads are fallible: a store that can't be reached must never look like "nothing
//! there", or the snapshot endpoint would publish empty entitlements and gateways would
//! drop every key.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;

use crate::billing::Invoice;
use crate::model::{ProvisionedThroughput, RegionIncident};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    #[error("{0} already exists")]
    AlreadyExists(String),
    #[error("{0} was modified concurrently")]
    VersionConflict(String),
    #[error("{0} not found")]
    NotFound(String),
    /// The store couldn't be reached or failed. Retryable.
    #[error("store unavailable: {0}")]
    Unavailable(String),
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
    ) -> impl Future<Output = Result<Option<ProvisionedThroughput>, StoreError>> + Send;

    /// A tenant's resources, oldest first.
    fn list(
        &self,
        tenant: &str,
    ) -> impl Future<Output = Result<Vec<ProvisionedThroughput>, StoreError>> + Send;

    /// Live resources across all tenants, for the lifecycle loop and snapshots.
    fn list_live(
        &self,
    ) -> impl Future<Output = Result<Vec<ProvisionedThroughput>, StoreError>> + Send;

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
    ) -> impl Future<Output = Result<Option<IdempotencyRecord>, StoreError>> + Send;

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
    fn list_incidents(
        &self,
    ) -> impl Future<Output = Result<Vec<RegionIncident>, StoreError>> + Send;

    /// Store a final invoice. Fails if the tenant already has one for the period: final
    /// invoices never change.
    fn insert_invoice(
        &self,
        invoice: Invoice,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    fn get_invoice(
        &self,
        tenant: &str,
        period: &str,
    ) -> impl Future<Output = Result<Option<Invoice>, StoreError>> + Send;

    /// A tenant's final invoices, newest first.
    fn list_invoices(
        &self,
        tenant: &str,
    ) -> impl Future<Output = Result<Vec<Invoice>, StoreError>> + Send;
}

#[derive(Default)]
struct Inner {
    by_id: HashMap<String, ProvisionedThroughput>,
    idempotency: HashMap<(String, String), IdempotencyRecord>,
    incidents: Vec<RegionIncident>,
    invoices: HashMap<(String, String), Invoice>,
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

    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<ProvisionedThroughput>, StoreError> {
        Ok(self
            .lock()
            .by_id
            .get(id)
            .filter(|pt| pt.tenant == tenant)
            .cloned())
    }

    async fn list(&self, tenant: &str) -> Result<Vec<ProvisionedThroughput>, StoreError> {
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
        Ok(out)
    }

    async fn list_live(&self) -> Result<Vec<ProvisionedThroughput>, StoreError> {
        Ok(self
            .lock()
            .by_id
            .values()
            .filter(|pt| pt.state.is_live())
            .cloned()
            .collect())
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

    async fn idempotency_get(
        &self,
        tenant: &str,
        key: &str,
    ) -> Result<Option<IdempotencyRecord>, StoreError> {
        Ok(self
            .lock()
            .idempotency
            .get(&(tenant.to_string(), key.to_string()))
            .cloned())
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

    async fn list_incidents(&self) -> Result<Vec<RegionIncident>, StoreError> {
        Ok(self.lock().incidents.clone())
    }

    async fn insert_invoice(&self, invoice: Invoice) -> Result<(), StoreError> {
        let mut inner = self.lock();
        let k = (invoice.tenant.clone(), invoice.period.clone());
        if inner.invoices.contains_key(&k) {
            return Err(StoreError::AlreadyExists(invoice.id));
        }
        inner.invoices.insert(k, invoice);
        Ok(())
    }

    async fn get_invoice(&self, tenant: &str, period: &str) -> Result<Option<Invoice>, StoreError> {
        Ok(self
            .lock()
            .invoices
            .get(&(tenant.to_string(), period.to_string()))
            .cloned())
    }

    async fn list_invoices(&self, tenant: &str) -> Result<Vec<Invoice>, StoreError> {
        let mut out: Vec<_> = self
            .lock()
            .invoices
            .values()
            .filter(|i| i.tenant == tenant)
            .cloned()
            .collect();
        out.sort_by(|a, b| b.period.cmp(&a.period));
        Ok(out)
    }
}
