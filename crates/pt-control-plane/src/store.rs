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

use jiff::{SignedDuration, Timestamp};

use crate::billing::Invoice;
use crate::failover::{next_heartbeat, GatewayHeartbeat};
use crate::model::{Heartbeat, ProvisionedThroughput, RegionIncident};

/// Who holds a named lease, and until when (ADR-023).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Lease {
    pub name: String,
    pub holder: String,
    pub expires_at: Timestamp,
}

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

    // Shared state for several control-plane instances (ADR-023).

    /// Raise the entitlement version to `max(current + 1, at_least)` and return it. Every
    /// instance's changes share this counter, so versions only increase.
    fn bump_version(&self, at_least: u64) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// The current entitlement version (0 before the first bump).
    fn current_version(&self) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// Record a gateway heartbeat (see [`next_heartbeat`]).
    fn record_heartbeat(
        &self,
        region: &str,
        hb: &Heartbeat,
        now: Timestamp,
        timeout: SignedDuration,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Every gateway's latest heartbeat record.
    fn gateway_heartbeats(
        &self,
    ) -> impl Future<Output = Result<Vec<GatewayHeartbeat>, StoreError>> + Send;

    /// Forget gateways last seen before `before`. Returns how many were removed.
    fn prune_heartbeats(
        &self,
        before: Timestamp,
    ) -> impl Future<Output = Result<usize, StoreError>> + Send;

    /// Take or renew lease `name` for `holder` until `now + ttl`. Succeeds if the lease is
    /// free, expired, or already held by `holder`.
    fn try_lease(
        &self,
        name: &str,
        holder: &str,
        now: Timestamp,
        ttl: SignedDuration,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    fn lease(&self, name: &str) -> impl Future<Output = Result<Option<Lease>, StoreError>> + Send;
}

#[derive(Default)]
struct Inner {
    by_id: HashMap<String, ProvisionedThroughput>,
    idempotency: HashMap<(String, String), IdempotencyRecord>,
    incidents: Vec<RegionIncident>,
    invoices: HashMap<(String, String), Invoice>,
    version: u64,
    heartbeats: HashMap<(String, String), GatewayHeartbeat>,
    leases: HashMap<String, Lease>,
}

#[derive(Default)]
pub struct MemoryStore {
    inner: Mutex<Inner>,
}

impl MemoryStore {
    /// A store whose entitlement version starts at `version` (the service uses the clock in
    /// milliseconds, so versions keep increasing across restarts).
    pub fn starting_at(version: u64) -> Self {
        let s = Self::default();
        s.lock().version = version;
        s
    }
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

    async fn bump_version(&self, at_least: u64) -> Result<u64, StoreError> {
        let mut inner = self.lock();
        inner.version = (inner.version + 1).max(at_least);
        Ok(inner.version)
    }

    async fn current_version(&self) -> Result<u64, StoreError> {
        Ok(self.lock().version)
    }

    async fn record_heartbeat(
        &self,
        region: &str,
        hb: &Heartbeat,
        now: Timestamp,
        timeout: SignedDuration,
    ) -> Result<(), StoreError> {
        let mut inner = self.lock();
        let key = (region.to_string(), hb.gateway_id.clone());
        let next = next_heartbeat(inner.heartbeats.get(&key), region, hb, now, timeout);
        inner.heartbeats.insert(key, next);
        Ok(())
    }

    async fn gateway_heartbeats(&self) -> Result<Vec<GatewayHeartbeat>, StoreError> {
        Ok(self.lock().heartbeats.values().cloned().collect())
    }

    async fn prune_heartbeats(&self, before: Timestamp) -> Result<usize, StoreError> {
        let mut inner = self.lock();
        let n = inner.heartbeats.len();
        inner.heartbeats.retain(|_, h| h.last_seen >= before);
        Ok(n - inner.heartbeats.len())
    }

    async fn try_lease(
        &self,
        name: &str,
        holder: &str,
        now: Timestamp,
        ttl: SignedDuration,
    ) -> Result<bool, StoreError> {
        let mut inner = self.lock();
        let free = inner
            .leases
            .get(name)
            .is_none_or(|l| l.holder == holder || l.expires_at <= now);
        if free {
            inner.leases.insert(
                name.to_string(),
                Lease {
                    name: name.to_string(),
                    holder: holder.to_string(),
                    expires_at: now + ttl,
                },
            );
        }
        Ok(free)
    }

    async fn lease(&self, name: &str) -> Result<Option<Lease>, StoreError> {
        Ok(self.lock().leases.get(name).cloned())
    }
}

/// A shared store, so several service instances can use one (for example, a control plane
/// restarted with a new signing key in tests).
impl<T: Store> Store for std::sync::Arc<T> {
    async fn insert(&self, pt: ProvisionedThroughput) -> Result<(), StoreError> {
        (**self).insert(pt).await
    }
    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<ProvisionedThroughput>, StoreError> {
        (**self).get(tenant, id).await
    }
    async fn list(&self, tenant: &str) -> Result<Vec<ProvisionedThroughput>, StoreError> {
        (**self).list(tenant).await
    }
    async fn list_live(&self) -> Result<Vec<ProvisionedThroughput>, StoreError> {
        (**self).list_live().await
    }
    async fn update(
        &self,
        pt: ProvisionedThroughput,
        expected_version: u64,
    ) -> Result<(), StoreError> {
        (**self).update(pt, expected_version).await
    }
    async fn idempotency_get(
        &self,
        tenant: &str,
        key: &str,
    ) -> Result<Option<IdempotencyRecord>, StoreError> {
        (**self).idempotency_get(tenant, key).await
    }
    async fn idempotency_put(
        &self,
        tenant: &str,
        key: &str,
        record: IdempotencyRecord,
    ) -> Result<(), StoreError> {
        (**self).idempotency_put(tenant, key, record).await
    }
    async fn insert_incident(&self, incident: RegionIncident) -> Result<(), StoreError> {
        (**self).insert_incident(incident).await
    }
    async fn update_incident(&self, incident: RegionIncident) -> Result<(), StoreError> {
        (**self).update_incident(incident).await
    }
    async fn list_incidents(&self) -> Result<Vec<RegionIncident>, StoreError> {
        (**self).list_incidents().await
    }
    async fn insert_invoice(&self, invoice: Invoice) -> Result<(), StoreError> {
        (**self).insert_invoice(invoice).await
    }
    async fn get_invoice(&self, tenant: &str, period: &str) -> Result<Option<Invoice>, StoreError> {
        (**self).get_invoice(tenant, period).await
    }
    async fn list_invoices(&self, tenant: &str) -> Result<Vec<Invoice>, StoreError> {
        (**self).list_invoices(tenant).await
    }
    async fn bump_version(&self, at_least: u64) -> Result<u64, StoreError> {
        (**self).bump_version(at_least).await
    }
    async fn current_version(&self) -> Result<u64, StoreError> {
        (**self).current_version().await
    }
    async fn record_heartbeat(
        &self,
        region: &str,
        hb: &Heartbeat,
        now: Timestamp,
        timeout: SignedDuration,
    ) -> Result<(), StoreError> {
        (**self).record_heartbeat(region, hb, now, timeout).await
    }
    async fn gateway_heartbeats(&self) -> Result<Vec<GatewayHeartbeat>, StoreError> {
        (**self).gateway_heartbeats().await
    }
    async fn prune_heartbeats(&self, before: Timestamp) -> Result<usize, StoreError> {
        (**self).prune_heartbeats(before).await
    }
    async fn try_lease(
        &self,
        name: &str,
        holder: &str,
        now: Timestamp,
        ttl: SignedDuration,
    ) -> Result<bool, StoreError> {
        (**self).try_lease(name, holder, now, ttl).await
    }
    async fn lease(&self, name: &str) -> Result<Option<Lease>, StoreError> {
        (**self).lease(name).await
    }
}
