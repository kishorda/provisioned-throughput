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
use pt_entitlement::{DeploymentEntitlement, ReservationEntitlement, Snapshot, SnapshotSigner};
use tokio::sync::watch;

use crate::clock::Clock;
use crate::config::ControlPlaneConfig;
use crate::model::{
    total_cus, CreateRequest, Endpoint, Event, EventKind, PendingChanges, ProvisionedThroughput,
    RegionShare, State, UpdateRequest,
};
use crate::planner::{CapacityPlanner, PlanError};
use crate::pricing;
use crate::store::{IdempotencyRecord, Store, StoreError};
use crate::validate;

/// How far in the past `start_at` may be (clock skew), and how far ahead.
const START_SKEW: SignedDuration = SignedDuration::from_mins(5);
const MAX_START_AHEAD: SignedDuration = SignedDuration::from_hours(24 * 90);

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
    /// Entitlement version: bumped on every change, watched by snapshot long-polls.
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

    pub fn signer(&self) -> &SnapshotSigner {
        &self.signer
    }

    /// Current entitlement version.
    pub fn entitlement_version(&self) -> u64 {
        *self.changes.borrow()
    }

    /// Watch for entitlement changes.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    fn bump(&self) {
        let now_ms = self.clock.now().as_millisecond().max(0) as u64;
        self.changes.send_modify(|v| *v = (*v + 1).max(now_ms));
    }

    /// Entitlements for `region`: every reservation serving there (active or pending
    /// cancellation), with the region's CU share. `None` for an unknown region.
    pub async fn snapshot(&self, region: &str) -> Option<Snapshot> {
        if !self.config.regions.iter().any(|r| r.name == region) {
            return None;
        }
        // Read the version before the data. A change in between is labelled with the older
        // version, so the gateway fetches again and never misses it.
        let version = self.entitlement_version();
        let mut live: Vec<_> = self
            .store
            .list_live()
            .await
            .into_iter()
            .filter(|pt| matches!(pt.state, State::Active | State::PendingCancellation))
            .collect();
        live.sort_by(|a, b| a.id.cmp(&b.id));

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
            });
            deployments.push(DeploymentEntitlement {
                id: pt.deployment_id.clone(),
                reservation: pt.id.clone(),
                api_key_sha256: pt.api_key_sha256.clone(),
                boundary_policy: pt.boundary_policy.clone(),
            });
        }
        Some(Snapshot {
            region: region.to_string(),
            version,
            generated_at: self.clock.now().to_string(),
            reservations,
            deployments,
        })
    }

    pub async fn create(
        &self,
        tenant: &str,
        idempotency_key: Option<&str>,
        req: CreateRequest,
    ) -> Result<CreateOutcome, ServiceError> {
        let fingerprint = sha256_hex(&serde_json::to_vec(&req).expect("request serialises"));
        if let Some(key) = idempotency_key {
            if let Some(rec) = self.store.idempotency_get(tenant, key).await {
                if rec.fingerprint != fingerprint {
                    return Err(conflict(
                        "idempotency_key_reused",
                        "This Idempotency-Key was used for a different request.",
                    ));
                }
                let resource = self
                    .store
                    .get(tenant, &rec.resource_id)
                    .await
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

        self.planner
            .reserve(&req.model, &req.regions, &req.shape)
            .await?;

        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let api_key = format!(
            "ptk_{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let cus = total_cus(&req.regions);
        let price = self.price(&req.tier, req.isolation, cus);
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
                kind: EventKind::Activated,
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
            deployment_id: format!("dep-{}", &suffix[16..]),
            price,
            api_key_sha256: sha256_hex(api_key.as_bytes()),
            version: 1,
            created_at: now,
            updated_at: now,
            events,
        };

        if let Err(e) = self.store.insert(pt.clone()).await {
            self.planner.release(&pt.model, &pt.regions).await;
            return Err(conflict("already_exists", e.to_string()));
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
        self.bump();
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
            .await
            .ok_or(ServiceError::NotFound)
    }

    pub async fn list(
        &self,
        tenant: &str,
        model: Option<&str>,
        include_inactive: bool,
    ) -> Vec<ProvisionedThroughput> {
        self.store
            .list(tenant)
            .await
            .into_iter()
            .filter(|pt| include_inactive || pt.state.is_live())
            .filter(|pt| model.is_none_or(|m| pt.model == m))
            .collect()
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
                self.bump();
                Ok(pt)
            }
            Err(e) => {
                self.undo(&pt.model, &pt.shape, ops).await;
                Err(match e {
                    StoreError::VersionConflict(_) => conflict(
                        "concurrent_modification",
                        "The reservation changed while this update ran. Fetch it and retry.",
                    ),
                    StoreError::NotFound(_) => ServiceError::NotFound,
                    other => conflict("store_error", other.to_string()),
                })
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
                    self.planner.release(&pt.model, &pt.regions).await;
                    ops.push(PlanOp::Released(pt.regions.clone()));
                    self.planner.reserve(&pt.model, new, &shape).await?;
                    ops.push(PlanOp::Reserved(new.clone()));
                    pt.regions = new.clone();
                }
            } else if let Some(deltas) = increases_only(&pt.regions, new) {
                if !deltas.is_empty() {
                    self.planner.reserve(&pt.model, &deltas, &shape).await?;
                    ops.push(PlanOp::Reserved(deltas.clone()));
                    let per_cu = self.price(&pt.tier, pt.isolation, 1).per_cu_monthly;
                    for d in deltas {
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
        pt.price = self.price(&pt.tier, pt.isolation, pt.cus);
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
        self.store.update(pt.clone(), expected).await.map_err(|_| {
            conflict(
                "concurrent_modification",
                "The reservation changed while this request ran. Retry.",
            )
        })?;
        if effect == DeleteEffect::CancelledNow {
            self.planner.release(&pt.model, &pt.regions).await;
        }
        self.bump();
        tracing::info!(id = %pt.id, %tenant, ?effect, "provisioned throughput deleted");
        Ok((pt, effect))
    }

    /// Activate, renew, and end reservations that are due. Run periodically.
    pub async fn run_lifecycle(&self) -> LifecycleReport {
        let mut report = LifecycleReport::default();
        for mut pt in self.store.list_live().await {
            let now = self.clock.now();
            let expected = pt.version;
            let mut ops = Vec::new();
            let mut changed = false;

            if pt.state == State::Scheduled && now >= pt.term_start {
                pt.state = State::Active;
                pt.events.push(Event {
                    at: now,
                    kind: EventKind::Activated,
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
                    self.planner.release(&pt.model, &pt.regions).await;
                    ops.push(PlanOp::Released(pt.regions.clone()));
                    pt.state = State::Ended;
                    pt.pending_changes = None;
                    pt.events.push(Event {
                        at: now,
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
            if self.store.update(pt, expected).await.is_err() {
                // A customer update won the race. Undo and pick it up on the next run.
                self.undo(&model, &shape, ops).await;
                report.conflicts += 1;
            } else {
                self.bump();
            }
        }
        report
    }

    async fn renew(&self, pt: &mut ProvisionedThroughput, now: Timestamp, ops: &mut Vec<PlanOp>) {
        if let Some(pending) = pt.pending_changes.take() {
            if let Some(new) = pending.regions.filter(|r| *r != pt.regions) {
                self.planner.release(&pt.model, &pt.regions).await;
                match self.planner.reserve(&pt.model, &new, &pt.shape).await {
                    Ok(()) => {
                        ops.push(PlanOp::Released(pt.regions.clone()));
                        ops.push(PlanOp::Reserved(new.clone()));
                        pt.regions = new;
                    }
                    Err(e) => {
                        // Put the current capacity back and renew unchanged.
                        let _ = self
                            .planner
                            .reserve(&pt.model, &pt.regions, &pt.shape)
                            .await;
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
            pt.events.push(Event {
                at: now,
                kind: EventKind::ChangeApplied {
                    tier: pt.tier,
                    regions: pt.regions.clone(),
                },
            });
        }
        pt.term_start = pt.term_end;
        pt.term_end = add_months(pt.term_start, pt.term_months.months());
        pt.cus = total_cus(&pt.regions);
        pt.endpoints = self.endpoints(&pt.regions);
        pt.price = self.price(&pt.tier, pt.isolation, pt.cus);
        pt.events.push(Event {
            at: now,
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
            .await
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
        cus: u32,
    ) -> crate::model::Price {
        let p = &self.config.pricing;
        pricing::price(
            &p.currency,
            p.base_cu_price_per_month_cents,
            *tier,
            isolation,
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
