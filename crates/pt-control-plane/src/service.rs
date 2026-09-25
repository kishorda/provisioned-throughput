//! Business rules for Provisioned Throughput (docs/12 §3).
//!
//! - Create reserves capacity for every region up front, or fails with nothing reserved.
//! - CU increases in existing regions apply immediately after a capacity check, and are
//!   charged pro rata for the rest of the term.
//! - Decreases, region changes, and tier changes are scheduled for the next term, unless
//!   the term hasn't started, in which case they apply immediately.
//! - DELETE mid-term turns off renewal; the reservation keeps serving until `term_end`.
//!   Before the term starts, DELETE cancels immediately.
//! - The lifecycle loop activates, renews (applying scheduled changes), and ends reservations.

use jiff::{SignedDuration, Span, Timestamp};
use pt_core::Shape;
pub use pt_entitlement::sha256_hex;
use pt_entitlement::{
    DeploymentEntitlement, FailoverShare, PreviousKey, RegionFailover, ReservationEntitlement,
    Snapshot, SnapshotSigner,
};
use tokio::sync::watch;

use crate::clock::Clock;
use crate::config::ControlPlaneConfig;
use crate::failover::{self, footprint, growth, Health, RegionStatus, Steering};
use crate::model::{
    total_cus, ApiKey, CreateDeploymentRequest, CreateRequest, DeclareIncident, Deployment,
    Endpoint, Event, EventKind, Heartbeat, IncidentSource, PendingChanges, ProvisionedThroughput,
    RegionIncident, RegionShare, ResolveIncident, RotateKeyRequest, Sku, State,
    UpdateDeploymentRequest, UpdateRequest,
};
use crate::planner::{CapacityPlanner, PlanError};
use crate::pricing;
use crate::store::{IdempotencyRecord, Store, StoreError};
use crate::validate;

/// The lease whose holder runs the background loops (ADR-023).
pub const LEADER_LEASE: &str = "background";

/// How far in the past `start_at` may be (clock skew), and how far ahead.
const START_SKEW: SignedDuration = SignedDuration::from_mins(5);
const MAX_START_AHEAD: SignedDuration = SignedDuration::from_hours(24 * 90);
/// Rotation grace period: default and maximum.
const DEFAULT_KEY_GRACE_MINUTES: u64 = 60;
const MAX_KEY_GRACE_MINUTES: u64 = 7 * 24 * 60;
/// Rotated-out keys kept in their grace period. Older ones are revoked on rotation.
const MAX_PREVIOUS_KEYS: usize = 2;

/// A new inference key: the secret (returned once) and its stored metadata.
fn issue_key(now: Timestamp) -> (String, ApiKey) {
    let secret = format!(
        "ptk_{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let key = ApiKey {
        id: format!("key-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]),
        prefix: secret[..12].to_string(),
        sha256: sha256_hex(secret.as_bytes()),
        created_at: now,
        expires_at: None,
    };
    (secret, key)
}

/// Remove keys whose grace period has ended, recording a `KeyExpired` event for each.
/// Returns whether anything was removed.
fn prune_expired_keys(pt: &mut ProvisionedThroughput, now: Timestamp) -> bool {
    let mut expired = Vec::new();
    for d in &mut pt.deployments {
        d.api_keys.retain(|k| {
            let live = k.expires_at.is_none_or(|e| e > now);
            if !live {
                expired.push((d.id.clone(), k.id.clone()));
            }
            live
        });
    }
    let any = !expired.is_empty();
    for (deployment, key) in expired {
        pt.events.push(Event {
            at: now,
            kind: EventKind::KeyExpired { deployment, key },
        });
    }
    any
}

/// Deployments per reservation.
const MAX_DEPLOYMENTS: usize = 10;

fn validate_max_share(share: Option<f64>) -> Result<(), ServiceError> {
    match share {
        Some(s) if !(s > 0.0 && s <= 1.0) => Err(ServiceError::Validation {
            field: "max_share".into(),
            message: "max_share must be greater than 0 and at most 1.".into(),
        }),
        _ => Ok(()),
    }
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ServiceError {
    #[error("not found")]
    NotFound,
    #[error("{field}: {message}")]
    Validation { field: String, message: String },
    #[error(transparent)]
    Capacity(#[from] PlanError),
    #[error("{message}")]
    Conflict { code: &'static str, message: String },
    #[error("If-Match version {expected} doesn't match current version {current}")]
    PreconditionFailed { expected: u64, current: u64 },
    /// The store is unreachable. Nothing was changed; retry.
    #[error("{0}")]
    Unavailable(String),
}

impl From<StoreError> for ServiceError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::NotFound(_) => ServiceError::NotFound,
            StoreError::VersionConflict(_) => conflict(
                "concurrent_modification",
                "The reservation changed while this request ran. Fetch it and retry.",
            ),
            StoreError::AlreadyExists(what) => {
                conflict("already_exists", format!("{what} already exists."))
            }
            StoreError::Unavailable(m) => ServiceError::Unavailable(m),
        }
    }
}

fn conflict(code: &'static str, message: impl Into<String>) -> ServiceError {
    ServiceError::Conflict {
        code,
        message: message.into(),
    }
}

#[derive(Debug, Clone)]
pub struct CreateOutcome {
    pub resource: ProvisionedThroughput,
    /// Only on first creation. Replays of the same idempotency key don't return the key.
    pub api_key: Option<String>,
    pub replayed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteEffect {
    /// The term hadn't started; capacity is released and nothing is billed.
    CancelledNow,
    /// Renewal is off; the reservation serves until `term_end`.
    EndsAtTermEnd,
    /// Already ended or cancelled.
    AlreadyInactive,
}

/// What one failover check changed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FailoverReport {
    /// Incidents declared, as (incident id, region).
    pub declared: Vec<(String, String)>,
    pub resolved: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LifecycleReport {
    pub activated: u32,
    pub renewed: u32,
    pub ended: u32,
    pub conflicts: u32,
}

/// Capacity operations done during one request, so they can be undone if the write fails.
enum PlanOp {
    Reserved(Vec<RegionShare>),
    Released(Vec<RegionShare>),
}

pub struct Service<S, P, C> {
    pub store: S,
    pub planner: P,
    pub clock: C,
    pub config: ControlPlaneConfig,
    signer: SnapshotSigner,
    /// The latest entitlement version this instance knows of. The shared counter is in the
    /// store; this copy wakes snapshot long-polls, and [`Self::sync_version`] keeps it
    /// current with other instances' changes (ADR-023).
    changes: watch::Sender<u64>,
}

impl<S: Store, P: CapacityPlanner, C: Clock> Service<S, P, C> {
    /// `config` must have passed [`ControlPlaneConfig::validate`].
    pub fn new(store: S, planner: P, clock: C, config: ControlPlaneConfig) -> Self {
        let signer = SnapshotSigner::from_hex(&config.entitlements.signing_key)
            .expect("validated signing key");
        // Start from the clock in milliseconds, so versions keep increasing across restarts.
        let (changes, _) = watch::channel(clock.now().as_millisecond().max(1) as u64);
        Self {
            store,
            planner,
            clock,
            config,
            signer,
            changes,
        }
    }

    /// Bump the shared entitlement version once at startup, so a restarted instance never
    /// labels content with an old version and every verifier refetches (ADR-020, ADR-023).
    pub async fn init_version(&self) -> Result<u64, StoreError> {
        let now_ms = self.clock.now().as_millisecond().max(1) as u64;
        let v = self.store.bump_version(now_ms).await?;
        self.changes.send_modify(|cur| *cur = (*cur).max(v));
        Ok(v)
    }

    /// Pick up the shared version, including other instances' changes. Wakes long-polls
    /// when it moved. Returns the current version.
    pub async fn sync_version(&self) -> Result<u64, StoreError> {
        let v = self.store.current_version().await?;
        self.changes.send_if_modified(|cur| {
            let newer = v > *cur;
            if newer {
                *cur = v;
            }
            newer
        });
        Ok(*self.changes.borrow())
    }

    /// Correct the planner's reserved counts from live reservations (see
    /// [`CapacityPlanner::reconcile`]). Run by the leader.
    pub async fn reconcile_capacity(&self) -> Result<Vec<crate::planner::Drift>, ServiceError> {
        let mut expected: std::collections::HashMap<(String, String), u32> = Default::default();
        for pt in self.store.list_live().await? {
            for share in held(&pt) {
                *expected
                    .entry((share.region, pt.model.clone()))
                    .or_default() += share.cus;
            }
        }
        Ok(self.planner.reconcile(&expected).await?)
    }

    pub fn signer(&self) -> &SnapshotSigner {
        &self.signer
    }

    /// Rebuild the planner's reserved capacity from live reservations. Run once at startup,
    /// before serving. Returns how many reservations were restored.
    pub async fn restore_capacity(&self) -> Result<usize, StoreError> {
        let live = self.store.list_live().await?;
        for pt in &live {
            self.planner.restore(&pt.model, &held(pt)).await;
        }
        if !live.is_empty() {
            tracing::info!(reservations = live.len(), "restored reserved capacity");
        }
        Ok(live.len())
    }

    /// Current entitlement version.
    pub fn entitlement_version(&self) -> u64 {
        *self.changes.borrow()
    }

    /// Watch for entitlement changes.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    /// Publish a committed change: bump the shared version and wake local long-polls. If
    /// the store can't be reached, log it; the leader's next bump publishes the change.
    async fn bump(&self) {
        let now_ms = self.clock.now().as_millisecond().max(0) as u64;
        match self.store.bump_version(now_ms).await {
            Ok(v) => self.changes.send_modify(|cur| *cur = (*cur).max(v)),
            Err(e) => {
                tracing::error!(error = %e, "entitlement version not bumped; gateways see this change on the next one")
            }
        }
    }

    /// Entitlements for `region`: every reservation serving there (active or pending
    /// cancellation), with the region's CU share. `Ok(None)` for an unknown region. A store
    /// failure is an error, never an empty snapshot: gateways keep what they have.
    pub async fn snapshot(&self, region: &str) -> Result<Option<Snapshot>, ServiceError> {
        if !self.config.regions.iter().any(|r| r.name == region) {
            return Ok(None);
        }
        // Read the shared version before the data. A change in between (from any instance)
        // is labelled with the older version, so the gateway fetches again and never misses
        // it.
        let version = self.sync_version().await?;
        let mut live: Vec<_> = self
            .store
            .list_live()
            .await?
            .into_iter()
            .filter(|pt| matches!(pt.state, State::Active | State::PendingCancellation))
            .collect();
        live.sort_by(|a, b| a.id.cmp(&b.id));
        let failovers = self.failovers(self.clock.now()).await?;

        let now = self.clock.now();
        let mut reservations = Vec::new();
        let mut deployments = Vec::new();
        for pt in live {
            let Some(share) = pt.regions.iter().find(|r| r.region == region) else {
                continue;
            };
            let Some(capacity) = self.config.capacity_for(region, &pt.model) else {
                tracing::error!(id = %pt.id, %region, "no capacity entry for a live reservation");
                continue;
            };
            reservations.push(ReservationEntitlement {
                id: pt.id.clone(),
                tenant: pt.tenant.clone(),
                model: pt.model.clone(),
                cus: share.cus,
                tier: pt.tier,
                profile: capacity.profile.clone(),
                shape: pt.shape,
                failover: self.failover_shares(&pt, region),
            });
            for d in &pt.deployments {
                let Some(current) = d.current_key() else {
                    continue;
                };
                deployments.push(DeploymentEntitlement {
                    id: d.id.clone(),
                    reservation: pt.id.clone(),
                    api_key_sha256: current.sha256.clone(),
                    previous_keys: d
                        .api_keys
                        .iter()
                        .filter_map(|k| {
                            let expires = k.expires_at.filter(|e| *e > now)?;
                            Some(PreviousKey {
                                api_key_sha256: k.sha256.clone(),
                                expires_at_ms: expires.as_millisecond().max(0) as u64,
                            })
                        })
                        .collect(),
                    max_share: d.max_share,
                    boundary_policy: pt.boundary_policy.clone(),
                });
            }
        }
        Ok(Some(Snapshot {
            region: region.to_string(),
            version,
            generated_at: now.to_string(),
            reservations,
            deployments,
            failovers,
        }))
    }

    /// Dormant failover entitlements that `region` holds for a Multi-region reservation.
    fn failover_shares(&self, pt: &ProvisionedThroughput, region: &str) -> Vec<FailoverShare> {
        if pt.sku != Sku::MultiRegion {
            return vec![];
        }
        failover::failover_targets(&self.config, &pt.regions)
            .into_iter()
            .filter(|(_, to)| to == region)
            .filter_map(|(from, _)| {
                let cus = pt.regions.iter().find(|r| r.region == from)?.cus;
                Some(FailoverShare {
                    from_region: from,
                    cus,
                })
            })
            .collect()
    }

    fn return_ramp(&self) -> SignedDuration {
        SignedDuration::from_mins(self.config.failover.return_ramp_minutes as i64)
    }

    fn heartbeat_timeout(&self) -> SignedDuration {
        SignedDuration::from_secs(self.config.failover.heartbeat_timeout_seconds as i64)
    }

    /// Region failures that activate failover entitlements now: open incidents, and
    /// resolved ones still in their return ramp.
    async fn failovers(&self, now: Timestamp) -> Result<Vec<RegionFailover>, StoreError> {
        let ramp = self.return_ramp();
        let ms = |t: Timestamp| t.as_millisecond().max(0) as u64;
        let mut out: Vec<_> = self
            .store
            .list_incidents()
            .await?
            .into_iter()
            .filter(|i| i.ended_at.is_none_or(|e| e + ramp > now))
            .map(|i| RegionFailover {
                started_at_ms: ms(i.started_at),
                ended_at_ms: i.ended_at.map(ms),
                return_ramp_ms: ramp.as_millis().max(0) as u64,
                region: i.region,
                incident: i.id,
            })
            .collect();
        out.sort_by(|a, b| a.incident.cmp(&b.incident));
        Ok(out)
    }

    pub async fn create(
        &self,
        tenant: &str,
        idempotency_key: Option<&str>,
        req: CreateRequest,
    ) -> Result<CreateOutcome, ServiceError> {
        let fingerprint = sha256_hex(&serde_json::to_vec(&req).expect("request serialises"));
        if let Some(key) = idempotency_key {
            if let Some(rec) = self.store.idempotency_get(tenant, key).await? {
                if rec.fingerprint != fingerprint {
                    return Err(conflict(
                        "idempotency_key_reused",
                        "This Idempotency-Key was used for a different request.",
                    ));
                }
                let resource = self
                    .store
                    .get(tenant, &rec.resource_id)
                    .await?
                    .ok_or(ServiceError::NotFound)?;
                return Ok(CreateOutcome {
                    resource,
                    api_key: None,
                    replayed: true,
                });
            }
        }

        validate::create(&self.config, &req)?;
        let now = self.clock.now();
        let start = match req.start_at {
            None => now,
            Some(t) if t < now - START_SKEW => {
                return Err(ServiceError::Validation {
                    field: "start_at".into(),
                    message: "start_at is in the past.".into(),
                })
            }
            Some(t) if t > now + MAX_START_AHEAD => {
                return Err(ServiceError::Validation {
                    field: "start_at".into(),
                    message: "start_at can be at most 90 days ahead.".into(),
                })
            }
            Some(t) => t.max(now),
        };
        self.ensure_name_free(tenant, &req.name, None).await?;

        let headroom = failover::headroom(&self.config, req.sku, &req.regions);
        self.planner
            .reserve(&req.model, &footprint(&req.regions, &headroom), &req.shape)
            .await?;

        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let (api_key, key_record) = issue_key(now);
        let cus = total_cus(&req.regions);
        let price = self.price(&req.tier, req.isolation, req.sku, cus);
        let state = if start <= now {
            State::Active
        } else {
            State::Scheduled
        };
        let mut events = vec![Event {
            at: now,
            kind: EventKind::Created {
                cus,
                monthly: price.monthly,
            },
        }];
        if state == State::Active {
            events.push(Event {
                at: now,
                kind: EventKind::Activated {
                    cus: Some(cus),
                    tier: Some(req.tier),
                    monthly: Some(price.monthly),
                },
            });
        }
        let pt = ProvisionedThroughput {
            id: format!("pt-{}", &suffix[..16]),
            tenant: tenant.to_string(),
            name: req.name,
            model: req.model,
            tier: req.tier,
            endpoints: self.endpoints(&req.regions),
            regions: req.regions,
            cus,
            sku: req.sku,
            isolation: req.isolation,
            shape: req.shape,
            boundary_policy: req.boundary_policy,
            term_months: req.term_months,
            term_start: start,
            term_end: add_months(start, req.term_months.months()),
            auto_renew: req.auto_renew,
            state,
            pending_changes: None,
            price,
            failover_headroom: headroom,
            deployments: vec![Deployment {
                id: format!("dep-{}", &suffix[16..]),
                name: "default".into(),
                max_share: None,
                api_keys: vec![key_record],
                created_at: now,
            }],
            version: 1,
            created_at: now,
            updated_at: now,
            events,
        };

        if let Err(e) = self.store.insert(pt.clone()).await {
            self.planner.release(&pt.model, &held(&pt)).await;
            return Err(match e {
                // A concurrent create took the name (the store's live-name index).
                StoreError::AlreadyExists(_) => conflict(
                    "name_taken",
                    format!("You already have a reservation named {}.", pt.name),
                ),
                other => other.into(),
            });
        }
        if let Some(key) = idempotency_key {
            let rec = IdempotencyRecord {
                resource_id: pt.id.clone(),
                fingerprint,
            };
            if let Err(e) = self.store.idempotency_put(tenant, key, rec).await {
                tracing::warn!(error = %e, id = %pt.id, "idempotency key raced with another request");
            }
        }
        self.bump().await;
        tracing::info!(id = %pt.id, %tenant, model = %pt.model, cus, "provisioned throughput created");
        Ok(CreateOutcome {
            resource: pt,
            api_key: Some(api_key),
            replayed: false,
        })
    }

    pub async fn get(&self, tenant: &str, id: &str) -> Result<ProvisionedThroughput, ServiceError> {
        self.store
            .get(tenant, id)
            .await?
            .ok_or(ServiceError::NotFound)
    }

    pub async fn list(
        &self,
        tenant: &str,
        model: Option<&str>,
        include_inactive: bool,
    ) -> Result<Vec<ProvisionedThroughput>, ServiceError> {
        Ok(self
            .store
            .list(tenant)
            .await?
            .into_iter()
            .filter(|pt| include_inactive || pt.state.is_live())
            .filter(|pt| model.is_none_or(|m| pt.model == m))
            .collect())
    }

    pub async fn update(
        &self,
        tenant: &str,
        id: &str,
        if_match: Option<u64>,
        req: UpdateRequest,
    ) -> Result<ProvisionedThroughput, ServiceError> {
        let mut pt = self.get(tenant, id).await?;
        check_version(&pt, if_match)?;
        if !pt.state.is_live() {
            return Err(conflict(
                "inactive",
                "This reservation has ended and can't be changed.",
            ));
        }
        let expected = pt.version;
        let now = self.clock.now();
        let model = validate::model(&self.config, &pt.model)?.clone();

        // Validate everything before touching capacity.
        if let Some(n) = &req.name {
            validate::name(n)?;
        }
        if let Some(t) = req.tier {
            validate::tier(&model, t)?;
        }
        if let Some(r) = &req.regions {
            validate::regions(&self.config, &pt.model, pt.sku, r)?;
        }
        if let Some(s) = &req.shape {
            validate::shape(&model, s)?;
        }
        if let Some(p) = &req.boundary_policy {
            validate::boundary_policy(p)?;
        }
        if let Some(n) = &req.name {
            if *n != pt.name {
                self.ensure_name_free(tenant, n, Some(&pt.id)).await?;
            }
        }

        let before = pt.clone();
        let mut ops = Vec::new();
        let result = self.apply_update(&mut pt, &req, now, &mut ops).await;
        if let Err(e) = result {
            self.undo(&pt.model, &pt.shape, ops).await;
            return Err(e);
        }
        if pt == before {
            return Ok(pt); // nothing changed; don't bump the version
        }
        pt.version += 1;
        pt.updated_at = now;
        match self.store.update(pt.clone(), expected).await {
            Ok(()) => {
                self.bump().await;
                Ok(pt)
            }
            Err(e) => {
                self.undo(&pt.model, &pt.shape, ops).await;
                Err(e.into())
            }
        }
    }

    async fn apply_update(
        &self,
        pt: &mut ProvisionedThroughput,
        req: &UpdateRequest,
        now: Timestamp,
        ops: &mut Vec<PlanOp>,
    ) -> Result<(), ServiceError> {
        let not_started = pt.state == State::Scheduled;
        let push = |pt: &mut ProvisionedThroughput, kind: EventKind| {
            pt.events.push(Event { at: now, kind })
        };

        if let Some(n) = &req.name {
            if *n != pt.name {
                pt.name = n.clone();
                push(pt, EventKind::Renamed { name: n.clone() });
            }
        }
        if let Some(p) = &req.boundary_policy {
            if *p != pt.boundary_policy {
                pt.boundary_policy = p.clone();
                push(pt, EventKind::BoundaryPolicyUpdated);
            }
        }
        match req.auto_renew {
            Some(true) if pt.state == State::PendingCancellation => {
                pt.state = State::Active;
                pt.auto_renew = true;
                push(pt, EventKind::CancellationWithdrawn);
            }
            Some(v) if v != pt.auto_renew => {
                pt.auto_renew = v;
                push(pt, EventKind::AutoRenewChanged { auto_renew: v });
            }
            _ => {}
        }

        let shape = req.shape.unwrap_or(pt.shape);
        let mut schedule_tier = None;
        let mut schedule_regions = None;

        if let Some(tier) = req.tier {
            if not_started {
                pt.tier = tier;
            } else if tier != pt.tier {
                schedule_tier = Some(Some(tier));
            } else {
                schedule_tier = Some(None); // reverting a scheduled change
            }
        }

        if let Some(new) = &req.regions {
            if not_started {
                if *new != pt.regions {
                    let before = held(pt);
                    let headroom = failover::headroom(&self.config, pt.sku, new);
                    let after = footprint(new, &headroom);
                    self.planner.release(&pt.model, &before).await;
                    ops.push(PlanOp::Released(before));
                    self.planner.reserve(&pt.model, &after, &shape).await?;
                    ops.push(PlanOp::Reserved(after));
                    pt.regions = new.clone();
                    pt.failover_headroom = headroom;
                }
            } else if let Some(deltas) = increases_only(&pt.regions, new) {
                if !deltas.is_empty() {
                    // Growing a share can grow the headroom it needs in its failover target.
                    let headroom = failover::headroom(&self.config, pt.sku, new);
                    let extra = growth(&held(pt), &footprint(new, &headroom));
                    self.planner.reserve(&pt.model, &extra, &shape).await?;
                    ops.push(PlanOp::Reserved(extra));
                    pt.failover_headroom = headroom;
                    let per_cu = self.price(&pt.tier, pt.isolation, pt.sku, 1).per_cu_monthly;
                    let mut running = pt.cus;
                    for d in deltas {
                        running += d.cus;
                        let from = pt
                            .regions
                            .iter()
                            .find(|r| r.region == d.region)
                            .map_or(0, |r| r.cus);
                        let charge = pricing::prorated_increase(
                            per_cu,
                            d.cus,
                            pt.term_months.months(),
                            pt.term_start,
                            pt.term_end,
                            now,
                        );
                        push(
                            pt,
                            EventKind::CapacityIncreased {
                                region: d.region.clone(),
                                from,
                                to: from + d.cus,
                                prorated_charge: charge,
                                cus: Some(running),
                                monthly: Some(per_cu * u64::from(running)),
                            },
                        );
                    }
                    pt.regions = new.clone();
                }
                // An increase cancels any scheduled region change that is now stale.
                if pt
                    .pending_changes
                    .as_ref()
                    .is_some_and(|p| p.regions.is_some())
                {
                    schedule_regions = Some(None);
                }
            } else {
                schedule_regions = Some(Some(new.clone()));
            }
        }

        if let Some(s) = req.shape {
            if s != pt.shape {
                self.planner.check_shape(&pt.model, &pt.regions, &s).await?;
                pt.shape = s;
                push(pt, EventKind::ShapeUpdated);
            }
        }

        if schedule_tier.is_some() || schedule_regions.is_some() {
            let wants_change =
                matches!(schedule_tier, Some(Some(_))) || matches!(schedule_regions, Some(Some(_)));
            if wants_change && pt.state == State::PendingCancellation {
                return Err(conflict(
                    "no_next_term",
                    "This reservation ends at term_end, so there's no next term to schedule changes for. Set auto_renew to true first.",
                ));
            }
            let mut pending = pt.pending_changes.take().unwrap_or(PendingChanges {
                tier: None,
                regions: None,
                effective_at: pt.term_end,
            });
            if let Some(t) = schedule_tier {
                pending.tier = t;
            }
            if let Some(r) = schedule_regions {
                pending.regions = r.filter(|r| *r != pt.regions);
            }
            pending.effective_at = pt.term_end;
            if pending.tier.is_some() || pending.regions.is_some() {
                push(
                    pt,
                    EventKind::ChangeScheduled {
                        tier: pending.tier,
                        regions: pending.regions.clone(),
                    },
                );
                pt.pending_changes = Some(pending);
            }
        }

        pt.cus = total_cus(&pt.regions);
        pt.endpoints = self.endpoints(&pt.regions);
        pt.price = self.price(&pt.tier, pt.isolation, pt.sku, pt.cus);
        Ok(())
    }

    pub async fn delete(
        &self,
        tenant: &str,
        id: &str,
        if_match: Option<u64>,
    ) -> Result<(ProvisionedThroughput, DeleteEffect), ServiceError> {
        let mut pt = self.get(tenant, id).await?;
        check_version(&pt, if_match)?;
        let now = self.clock.now();
        let expected = pt.version;
        let effect = match pt.state {
            State::Ended | State::Cancelled => return Ok((pt, DeleteEffect::AlreadyInactive)),
            State::PendingCancellation => return Ok((pt, DeleteEffect::EndsAtTermEnd)),
            State::Scheduled => {
                pt.state = State::Cancelled;
                pt.pending_changes = None;
                pt.events.push(Event {
                    at: now,
                    kind: EventKind::Cancelled,
                });
                DeleteEffect::CancelledNow
            }
            State::Active => {
                pt.state = State::PendingCancellation;
                pt.auto_renew = false;
                pt.pending_changes = None;
                pt.events.push(Event {
                    at: now,
                    kind: EventKind::CancellationRequested {
                        effective_at: pt.term_end,
                    },
                });
                DeleteEffect::EndsAtTermEnd
            }
        };
        pt.version += 1;
        pt.updated_at = now;
        self.store.update(pt.clone(), expected).await?;
        if effect == DeleteEffect::CancelledNow {
            self.planner.release(&pt.model, &held(&pt)).await;
        }
        self.bump().await;
        tracing::info!(id = %pt.id, %tenant, ?effect, "provisioned throughput deleted");
        Ok((pt, effect))
    }

    /// Save `pt` if it's still at `expected`, then publish the change to gateways.
    async fn persist(
        &self,
        mut pt: ProvisionedThroughput,
        expected: u64,
    ) -> Result<ProvisionedThroughput, ServiceError> {
        pt.version += 1;
        pt.updated_at = self.clock.now();
        self.store.update(pt.clone(), expected).await?;
        self.bump().await;
        Ok(pt)
    }

    /// Load a live reservation for a change, checking `If-Match`.
    async fn load_live(
        &self,
        tenant: &str,
        id: &str,
        if_match: Option<u64>,
    ) -> Result<ProvisionedThroughput, ServiceError> {
        let pt = self.get(tenant, id).await?;
        check_version(&pt, if_match)?;
        if !pt.state.is_live() {
            return Err(conflict(
                "inactive",
                "This reservation has ended and can't be changed.",
            ));
        }
        Ok(pt)
    }

    /// Index of a deployment by id, or of the primary deployment when `None`.
    fn deployment_index(
        pt: &ProvisionedThroughput,
        deployment: Option<&str>,
    ) -> Result<usize, ServiceError> {
        match deployment {
            None => Ok(0),
            Some(d) => pt
                .deployments
                .iter()
                .position(|x| x.id == d)
                .ok_or(ServiceError::NotFound),
        }
    }

    fn ensure_deployment_name_free(
        pt: &ProvisionedThroughput,
        name: &str,
        except: Option<&str>,
    ) -> Result<(), ServiceError> {
        validate::name(name)?;
        if pt
            .deployments
            .iter()
            .any(|d| d.name == name && Some(d.id.as_str()) != except)
        {
            return Err(conflict(
                "name_taken",
                format!("This reservation already has a deployment named {name}."),
            ));
        }
        Ok(())
    }

    /// Add a deployment with its own key. Returns the resource, the deployment, and its key.
    pub async fn create_deployment(
        &self,
        tenant: &str,
        id: &str,
        if_match: Option<u64>,
        req: CreateDeploymentRequest,
    ) -> Result<(ProvisionedThroughput, Deployment, String), ServiceError> {
        let mut pt = self.load_live(tenant, id, if_match).await?;
        Self::ensure_deployment_name_free(&pt, &req.name, None)?;
        validate_max_share(req.max_share)?;
        if pt.deployments.len() >= MAX_DEPLOYMENTS {
            return Err(conflict(
                "too_many_deployments",
                format!("A reservation can have at most {MAX_DEPLOYMENTS} deployments."),
            ));
        }
        let expected = pt.version;
        let now = self.clock.now();
        let (secret, key) = issue_key(now);
        let deployment = Deployment {
            id: format!("dep-{}", &uuid::Uuid::new_v4().simple().to_string()[..16]),
            name: req.name,
            max_share: req.max_share,
            api_keys: vec![key],
            created_at: now,
        };
        pt.deployments.push(deployment.clone());
        pt.events.push(Event {
            at: now,
            kind: EventKind::DeploymentCreated {
                deployment: deployment.id.clone(),
                name: deployment.name.clone(),
                max_share: deployment.max_share,
            },
        });
        let pt = self.persist(pt, expected).await?;
        tracing::info!(id = %pt.id, %tenant, deployment = %deployment.id, "deployment created");
        Ok((pt, deployment, secret))
    }

    /// Rename a deployment or change its cap. The cap applies at gateways with the next
    /// snapshot.
    pub async fn update_deployment(
        &self,
        tenant: &str,
        id: &str,
        deployment: &str,
        if_match: Option<u64>,
        req: UpdateDeploymentRequest,
    ) -> Result<ProvisionedThroughput, ServiceError> {
        let mut pt = self.load_live(tenant, id, if_match).await?;
        let i = Self::deployment_index(&pt, Some(deployment))?;
        if let Some(name) = &req.name {
            Self::ensure_deployment_name_free(&pt, name, Some(deployment))?;
        }
        if let Some(share) = req.max_share {
            validate_max_share(share)?;
        }
        let expected = pt.version;
        let d = &mut pt.deployments[i];
        let before = (d.name.clone(), d.max_share);
        if let Some(name) = req.name {
            d.name = name;
        }
        if let Some(share) = req.max_share {
            d.max_share = share;
        }
        if (d.name.clone(), d.max_share) == before {
            return Ok(pt);
        }
        let kind = EventKind::DeploymentUpdated {
            deployment: d.id.clone(),
            name: d.name.clone(),
            max_share: d.max_share,
        };
        pt.events.push(Event {
            at: self.clock.now(),
            kind,
        });
        self.persist(pt, expected).await
    }

    /// Remove a deployment. Its keys stop working with the next snapshot. The last
    /// deployment can't be deleted; delete the reservation instead.
    pub async fn delete_deployment(
        &self,
        tenant: &str,
        id: &str,
        deployment: &str,
        if_match: Option<u64>,
    ) -> Result<ProvisionedThroughput, ServiceError> {
        let mut pt = self.load_live(tenant, id, if_match).await?;
        let i = Self::deployment_index(&pt, Some(deployment))?;
        if pt.deployments.len() == 1 {
            return Err(conflict(
                "last_deployment",
                "A reservation needs at least one deployment. Delete the reservation instead.",
            ));
        }
        let expected = pt.version;
        let removed = pt.deployments.remove(i);
        pt.events.push(Event {
            at: self.clock.now(),
            kind: EventKind::DeploymentDeleted {
                deployment: removed.id.clone(),
            },
        });
        let pt = self.persist(pt, expected).await?;
        tracing::info!(id = %pt.id, %tenant, deployment = %removed.id, "deployment deleted");
        Ok(pt)
    }

    /// Issue a new inference key for a deployment (the primary when `None`). The current key
    /// keeps working for the grace period (0 revokes it now). At most two rotated-out keys
    /// stay live; older ones are revoked. Returns the resource and the new secret, which
    /// isn't stored.
    pub async fn rotate_key(
        &self,
        tenant: &str,
        id: &str,
        deployment: Option<&str>,
        if_match: Option<u64>,
        req: RotateKeyRequest,
    ) -> Result<(ProvisionedThroughput, String), ServiceError> {
        let grace = req.grace_minutes.unwrap_or(DEFAULT_KEY_GRACE_MINUTES);
        if grace > MAX_KEY_GRACE_MINUTES {
            return Err(ServiceError::Validation {
                field: "grace_minutes".into(),
                message: format!("grace_minutes can be at most {MAX_KEY_GRACE_MINUTES} (7 days)."),
            });
        }
        let mut pt = self.load_live(tenant, id, if_match).await?;
        let i = Self::deployment_index(&pt, deployment)?;
        let expected = pt.version;
        let now = self.clock.now();
        prune_expired_keys(&mut pt, now);

        let (secret, new_key) = issue_key(now);
        let previous_expires_at =
            (grace > 0).then(|| now + SignedDuration::from_mins(grace as i64));
        let d = &mut pt.deployments[i];
        let deployment_id = d.id.clone();
        let previous_id = d.current_key().map(|k| k.id.clone()).unwrap_or_default();
        match previous_expires_at {
            Some(at) => {
                if let Some(k) = d.api_keys.iter_mut().find(|k| k.is_current()) {
                    k.expires_at = Some(at);
                }
            }
            None => d.api_keys.retain(|k| k.id != previous_id),
        }
        // Keep the newest rotated-out keys only.
        let mut previous: Vec<ApiKey> = d.api_keys.drain(..).collect();
        previous.sort_by_key(|k| std::cmp::Reverse(k.created_at));
        let revoked: Vec<ApiKey> = previous.split_off(MAX_PREVIOUS_KEYS.min(previous.len()));
        d.api_keys = previous;
        d.api_keys.push(new_key.clone());
        d.api_keys.sort_by_key(|k| k.created_at);

        pt.events.push(Event {
            at: now,
            kind: EventKind::KeyRotated {
                deployment: deployment_id.clone(),
                new_key: new_key.id.clone(),
                previous_key: previous_id,
                previous_expires_at,
            },
        });
        for k in revoked {
            pt.events.push(Event {
                at: now,
                kind: EventKind::KeyRevoked {
                    deployment: deployment_id.clone(),
                    key: k.id,
                },
            });
        }
        let pt = self.persist(pt, expected).await?;
        tracing::info!(id = %pt.id, %tenant, deployment = %deployment_id, key = %new_key.id, grace, "inference key rotated");
        Ok((pt, secret))
    }

    /// Revoke a rotated-out key now. The current key can only be replaced by rotating.
    pub async fn revoke_key(
        &self,
        tenant: &str,
        id: &str,
        deployment: Option<&str>,
        key_id: &str,
        if_match: Option<u64>,
    ) -> Result<ProvisionedThroughput, ServiceError> {
        let mut pt = self.get(tenant, id).await?;
        check_version(&pt, if_match)?;
        let i = Self::deployment_index(&pt, deployment)?;
        let expected = pt.version;
        let d = &mut pt.deployments[i];
        let key =
            d.api_keys
                .iter()
                .find(|k| k.id == key_id)
                .ok_or_else(|| ServiceError::Validation {
                    field: "key_id".into(),
                    message: format!("No live key {key_id} on this deployment."),
                })?;
        if key.is_current() {
            return Err(conflict(
                "current_key",
                "The current key can't be revoked. Rotate with grace_minutes 0 to replace it immediately.",
            ));
        }
        d.api_keys.retain(|k| k.id != key_id);
        let deployment_id = d.id.clone();
        pt.events.push(Event {
            at: self.clock.now(),
            kind: EventKind::KeyRevoked {
                deployment: deployment_id.clone(),
                key: key_id.into(),
            },
        });
        let pt = self.persist(pt, expected).await?;
        tracing::info!(id = %pt.id, %tenant, deployment = %deployment_id, key = %key_id, "inference key revoked");
        Ok(pt)
    }

    /// Declare a region incident. One open incident per region at a time.
    pub async fn declare_incident(
        &self,
        req: DeclareIncident,
    ) -> Result<RegionIncident, ServiceError> {
        let now = self.clock.now();
        if !self.config.regions.iter().any(|r| r.name == req.region) {
            return Err(ServiceError::Validation {
                field: "region".into(),
                message: format!("Unknown region {}.", req.region),
            });
        }
        let description = req.description.trim().to_string();
        if description.is_empty() || description.len() > 500 {
            return Err(ServiceError::Validation {
                field: "description".into(),
                message: "Give a description of 1–500 characters.".into(),
            });
        }
        let started_at = req.started_at.unwrap_or(now);
        if started_at > now + SignedDuration::from_mins(1)
            || started_at < now - SignedDuration::from_hours(24)
        {
            return Err(ServiceError::Validation {
                field: "started_at".into(),
                message: "started_at must be within the last 24 hours.".into(),
            });
        }
        self.open_incident(
            req.region,
            started_at,
            description,
            IncidentSource::Operator,
        )
        .await
    }

    async fn open_incident(
        &self,
        region: String,
        started_at: Timestamp,
        description: String,
        source: IncidentSource,
    ) -> Result<RegionIncident, ServiceError> {
        let open = self.store.list_incidents().await?;
        if let Some(o) = open
            .iter()
            .find(|i| i.region == region && i.ended_at.is_none())
        {
            return Err(conflict(
                "incident_open",
                format!(
                    "Incident {} is already open for {region}. Resolve it first.",
                    o.id
                ),
            ));
        }
        let incident = RegionIncident {
            id: format!("inc-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]),
            region,
            started_at,
            ended_at: None,
            description,
            declared_at: self.clock.now(),
            source,
        };
        self.store
            .insert_incident(incident.clone())
            .await
            .map_err(|e| match e {
                // Another declaration for the region won (one open incident per region).
                StoreError::AlreadyExists(_) => conflict(
                    "incident_open",
                    format!("An incident is already open for {}.", incident.region),
                ),
                other => other.into(),
            })?;
        // Snapshots carry open incidents, which activate failover entitlements.
        self.bump().await;
        tracing::warn!(id = %incident.id, region = %incident.region, ?source, "region incident declared");
        Ok(incident)
    }

    pub async fn resolve_incident(
        &self,
        id: &str,
        req: ResolveIncident,
    ) -> Result<RegionIncident, ServiceError> {
        let now = self.clock.now();
        let mut incident = self
            .store
            .list_incidents()
            .await?
            .into_iter()
            .find(|i| i.id == id)
            .ok_or(ServiceError::NotFound)?;
        if incident.ended_at.is_some() {
            return Err(conflict(
                "already_resolved",
                "This incident is already resolved.",
            ));
        }
        let ended_at = req.ended_at.unwrap_or(now);
        if ended_at < incident.started_at || ended_at > now + SignedDuration::from_mins(1) {
            return Err(ServiceError::Validation {
                field: "ended_at".into(),
                message: "ended_at must be between started_at and now.".into(),
            });
        }
        incident.ended_at = Some(ended_at);
        self.store.update_incident(incident.clone()).await?;
        self.bump().await;
        tracing::info!(id = %incident.id, region = %incident.region, "region incident resolved");
        Ok(incident)
    }

    pub async fn incidents(&self) -> Result<Vec<RegionIncident>, ServiceError> {
        Ok(self.store.list_incidents().await?)
    }

    /// Record a gateway heartbeat for `region`, in the shared store.
    pub async fn heartbeat(&self, region: &str, hb: &Heartbeat) -> Result<(), ServiceError> {
        Ok(self
            .store
            .record_heartbeat(region, hb, self.clock.now(), self.heartbeat_timeout())
            .await?)
    }

    /// Current health of every configured region, from every instance's heartbeats.
    pub async fn region_statuses(&self) -> Result<Vec<RegionStatus>, ServiceError> {
        let now = self.clock.now();
        let gateways = self.store.gateway_heartbeats().await?;
        Ok(self
            .config
            .regions
            .iter()
            .map(|r| failover::region_status(&r.name, &gateways, now, self.heartbeat_timeout()))
            .collect())
    }

    /// Declare incidents for regions that stopped serving, and resolve automatic incidents
    /// for regions that have served continuously for `recovery_seconds`. Run by the leader.
    ///
    /// A region is declared down only while another region has served continuously for at
    /// least the heartbeat timeout. That proves heartbeats are reaching the control plane:
    /// after a control-plane or database outage every region looks stale at once, and the
    /// first region to report must not fail the others over.
    pub async fn run_failover(&self) -> FailoverReport {
        let mut report = FailoverReport::default();
        if !self.config.failover.auto_declare {
            return report;
        }
        let now = self.clock.now();
        let statuses = match self.region_statuses().await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "failover check skipped");
                return report;
            }
        };
        let flowing = |except: &str| {
            statuses.iter().any(|o| {
                o.region != except
                    && o.serving_since
                        .is_some_and(|t| now.duration_since(t) >= self.heartbeat_timeout())
            })
        };
        let recovery = SignedDuration::from_secs(self.config.failover.recovery_seconds as i64);
        let incidents = match self.store.list_incidents().await {
            Ok(i) => i,
            Err(e) => {
                tracing::warn!(error = %e, "failover check skipped");
                return report;
            }
        };
        for s in &statuses {
            let open = incidents
                .iter()
                .find(|i| i.region == s.region && i.ended_at.is_none());
            match (s.health, open) {
                (Health::Down, None) if flowing(&s.region) => {
                    let timeout = self.config.failover.heartbeat_timeout_seconds;
                    let started_at = s
                        .last_serving_at
                        .unwrap_or(now)
                        .max(now - SignedDuration::from_hours(24));
                    let description = format!(
                        "Automatic: no gateway in {} reported serving for {timeout} s.",
                        s.region
                    );
                    match self
                        .open_incident(
                            s.region.clone(),
                            started_at,
                            description,
                            IncidentSource::Automatic,
                        )
                        .await
                    {
                        Ok(i) => report.declared.push((i.id, i.region)),
                        Err(e) => {
                            tracing::warn!(error = %e, region = %s.region, "automatic incident not declared")
                        }
                    }
                }
                (Health::Serving, Some(i))
                    if i.source == IncidentSource::Automatic
                        && s.serving_since
                            .is_some_and(|t| now.duration_since(t) >= recovery) =>
                {
                    match self
                        .resolve_incident(
                            &i.id,
                            ResolveIncident {
                                ended_at: Some(now),
                            },
                        )
                        .await
                    {
                        Ok(i) => report.resolved.push((i.id, i.region)),
                        Err(e) => {
                            tracing::warn!(error = %e, id = %i.id, "automatic incident not resolved")
                        }
                    }
                }
                _ => {}
            }
        }
        report
    }

    /// DNS steering for every region and live reservation (docs/07 §3).
    pub async fn steering(&self) -> Result<Steering, ServiceError> {
        let now = self.clock.now();
        let ramp = self.return_ramp();
        let incidents = self.store.list_incidents().await?;
        let weights: std::collections::HashMap<String, (f64, Option<String>)> = self
            .config
            .regions
            .iter()
            .map(|r| {
                (
                    r.name.clone(),
                    failover::region_weight(&r.name, &incidents, now, ramp),
                )
            })
            .collect();
        let weight_of = |r: &str| weights.get(r).map_or(1.0, |w| w.0);
        let regions = self
            .region_statuses()
            .await?
            .into_iter()
            .map(|status| {
                let (weight, incident) =
                    weights.get(&status.region).cloned().unwrap_or((1.0, None));
                failover::RegionSteering {
                    status,
                    incident,
                    weight: (weight * 1e4).round() / 1e4,
                }
            })
            .collect();
        let mut live: Vec<_> = self
            .store
            .list_live()
            .await?
            .into_iter()
            .filter(|pt| matches!(pt.state, State::Active | State::PendingCancellation))
            .collect();
        live.sort_by(|a, b| a.id.cmp(&b.id));
        let reservations = live
            .iter()
            .map(|pt| failover::ReservationSteering {
                id: pt.id.clone(),
                sku: pt.sku,
                targets: failover::reservation_targets(&self.config, pt, weight_of),
            })
            .collect();
        Ok(Steering {
            generated_at: now,
            regions,
            reservations,
        })
    }

    /// Activate, renew, and end reservations that are due. Run periodically.
    pub async fn run_lifecycle(&self) -> LifecycleReport {
        let mut report = LifecycleReport::default();
        let live = match self.store.list_live().await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(error = %e, "lifecycle run skipped");
                return report;
            }
        };
        for mut pt in live {
            let now = self.clock.now();
            let expected = pt.version;
            let mut ops = Vec::new();
            let mut changed = false;

            if prune_expired_keys(&mut pt, now) {
                changed = true;
            }
            if pt.state == State::Scheduled && now >= pt.term_start {
                pt.state = State::Active;
                // Lifecycle events carry the moment they took effect, not when this loop
                // noticed, so invoices and SLA grace windows line up with the term.
                pt.events.push(Event {
                    at: pt.term_start,
                    kind: EventKind::Activated {
                        cus: Some(pt.cus),
                        tier: Some(pt.tier),
                        monthly: Some(pt.price.monthly),
                    },
                });
                report.activated += 1;
                changed = true;
            }
            while matches!(pt.state, State::Active | State::PendingCancellation)
                && now >= pt.term_end
            {
                changed = true;
                if pt.state == State::Active && pt.auto_renew {
                    self.renew(&mut pt, now, &mut ops).await;
                    report.renewed += 1;
                } else {
                    let before = held(&pt);
                    self.planner.release(&pt.model, &before).await;
                    ops.push(PlanOp::Released(before));
                    pt.state = State::Ended;
                    pt.pending_changes = None;
                    pt.events.push(Event {
                        at: pt.term_end,
                        kind: EventKind::Ended,
                    });
                    report.ended += 1;
                }
            }
            if !changed {
                continue;
            }
            pt.version += 1;
            pt.updated_at = now;
            let (model, shape) = (pt.model.clone(), pt.shape);
            if let Err(e) = self.store.update(pt, expected).await {
                // Usually a customer update won the race. Undo and retry on the next run.
                if matches!(e, StoreError::Unavailable(_)) {
                    tracing::warn!(error = %e, "lifecycle update failed");
                }
                self.undo(&model, &shape, ops).await;
                report.conflicts += 1;
            } else {
                self.bump().await;
            }
        }
        report
    }

    async fn renew(&self, pt: &mut ProvisionedThroughput, now: Timestamp, ops: &mut Vec<PlanOp>) {
        if let Some(pending) = pt.pending_changes.take() {
            if let Some(new) = pending.regions.filter(|r| *r != pt.regions) {
                let before = held(pt);
                let headroom = failover::headroom(&self.config, pt.sku, &new);
                let after = footprint(&new, &headroom);
                self.planner.release(&pt.model, &before).await;
                match self.planner.reserve(&pt.model, &after, &pt.shape).await {
                    Ok(()) => {
                        ops.push(PlanOp::Released(before));
                        ops.push(PlanOp::Reserved(after));
                        pt.regions = new;
                        pt.failover_headroom = headroom;
                    }
                    Err(e) => {
                        // Put the current capacity back and renew unchanged.
                        let _ = self.planner.reserve(&pt.model, &before, &pt.shape).await;
                        pt.events.push(Event {
                            at: now,
                            kind: EventKind::ScheduledChangeFailed {
                                reason: e.to_string(),
                            },
                        });
                    }
                }
            }
            if let Some(t) = pending.tier {
                pt.tier = t;
            }
            let monthly = self
                .price(&pt.tier, pt.isolation, pt.sku, total_cus(&pt.regions))
                .monthly;
            pt.events.push(Event {
                at: pt.term_end,
                kind: EventKind::ChangeApplied {
                    tier: pt.tier,
                    regions: pt.regions.clone(),
                    monthly: Some(monthly),
                },
            });
        }
        pt.term_start = pt.term_end;
        pt.term_end = add_months(pt.term_start, pt.term_months.months());
        pt.cus = total_cus(&pt.regions);
        pt.endpoints = self.endpoints(&pt.regions);
        pt.price = self.price(&pt.tier, pt.isolation, pt.sku, pt.cus);
        pt.events.push(Event {
            at: pt.term_start,
            kind: EventKind::Renewed {
                term_start: pt.term_start,
                term_end: pt.term_end,
                monthly: pt.price.monthly,
            },
        });
    }

    async fn undo(&self, model: &str, shape: &Shape, ops: Vec<PlanOp>) {
        for op in ops.into_iter().rev() {
            match op {
                PlanOp::Reserved(shares) => self.planner.release(model, &shares).await,
                PlanOp::Released(shares) => {
                    if let Err(e) = self.planner.reserve(model, &shares, shape).await {
                        tracing::error!(error = %e, "failed to restore capacity during rollback");
                    }
                }
            }
        }
    }

    async fn ensure_name_free(
        &self,
        tenant: &str,
        name: &str,
        except: Option<&str>,
    ) -> Result<(), ServiceError> {
        let taken = self
            .store
            .list(tenant)
            .await?
            .iter()
            .any(|pt| pt.state.is_live() && pt.name == name && Some(pt.id.as_str()) != except);
        if taken {
            Err(conflict(
                "name_taken",
                format!("You already have a reservation named {name}."),
            ))
        } else {
            Ok(())
        }
    }

    fn price(
        &self,
        tier: &pt_core::Tier,
        isolation: pt_core::PoolIsolation,
        sku: Sku,
        cus: u32,
    ) -> crate::model::Price {
        let p = &self.config.pricing;
        pricing::price(
            &p.currency,
            p.base_cu_price_per_month_cents,
            *tier,
            isolation,
            sku,
            cus,
        )
    }

    fn endpoints(&self, regions: &[RegionShare]) -> Vec<Endpoint> {
        regions
            .iter()
            .map(|r| Endpoint {
                region: r.region.clone(),
                url: self
                    .config
                    .server
                    .endpoint_template
                    .replace("{region}", &r.region),
            })
            .collect()
    }
}

/// Everything a reservation holds from the planner: its shares and its failover headroom.
fn held(pt: &ProvisionedThroughput) -> Vec<RegionShare> {
    footprint(&pt.regions, &pt.failover_headroom)
}

/// If `new` only raises CUs in the current regions (same set), return the increases.
fn increases_only(current: &[RegionShare], new: &[RegionShare]) -> Option<Vec<RegionShare>> {
    if current.len() != new.len() {
        return None;
    }
    let mut deltas = Vec::new();
    for n in new {
        let c = current.iter().find(|c| c.region == n.region)?;
        if n.cus < c.cus {
            return None;
        }
        if n.cus > c.cus {
            deltas.push(RegionShare {
                region: n.region.clone(),
                cus: n.cus - c.cus,
            });
        }
    }
    Some(deltas)
}

fn check_version(pt: &ProvisionedThroughput, if_match: Option<u64>) -> Result<(), ServiceError> {
    match if_match {
        Some(v) if v != pt.version => Err(ServiceError::PreconditionFailed {
            expected: v,
            current: pt.version,
        }),
        _ => Ok(()),
    }
}

/// Calendar months in UTC. A term starting on 31 January ends on 28/29 February.
pub fn add_months(t: Timestamp, months: u8) -> Timestamp {
    t.to_zoned(jiff::tz::TimeZone::UTC)
        .checked_add(Span::new().months(i64::from(months)))
        .expect("term end within supported range")
        .timestamp()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shares(v: &[(&str, u32)]) -> Vec<RegionShare> {
        v.iter()
            .map(|(r, c)| RegionShare {
                region: r.to_string(),
                cus: *c,
            })
            .collect()
    }

    #[test]
    fn increases_only_detects_pure_increases() {
        let cur = shares(&[("eu-west", 4), ("eu-central", 2)]);
        assert_eq!(
            increases_only(&cur, &shares(&[("eu-central", 5), ("eu-west", 4)])),
            Some(shares(&[("eu-central", 3)]))
        );
        assert_eq!(increases_only(&cur, &cur), Some(vec![]));
        assert_eq!(
            increases_only(&cur, &shares(&[("eu-west", 3), ("eu-central", 2)])),
            None
        );
        assert_eq!(increases_only(&cur, &shares(&[("eu-west", 4)])), None);
        assert_eq!(
            increases_only(&cur, &shares(&[("eu-west", 4), ("us-east", 2)])),
            None
        );
    }

    #[test]
    fn calendar_month_arithmetic() {
        let jan31: Timestamp = "2027-01-31T10:00:00Z".parse().unwrap();
        assert_eq!(add_months(jan31, 1).to_string(), "2027-02-28T10:00:00Z");
        let oct1: Timestamp = "2026-10-01T00:00:00Z".parse().unwrap();
        assert_eq!(add_months(oct1, 6).to_string(), "2027-04-01T00:00:00Z");
    }
}
