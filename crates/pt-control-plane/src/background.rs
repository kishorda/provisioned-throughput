//! Background work for one control-plane instance (ADR-023).
//!
//! - **Every instance** polls the shared entitlement version every 500 ms, so its snapshot
//!   long-polls wake for changes made through other instances.
//! - **The leader** runs the loops that change shared state: the lifecycle, failover
//!   detection, capacity reconciliation, invoice finalisation, and pruning. Leadership is
//!   a lease in the store (`LEADER_LEASE`), renewed every [`LEASE_RENEW`]. If the leader
//!   stops renewing, another instance takes over once the lease expires. The loops are safe
//!   to overlap briefly during a handover: they rely on version checks and unique
//!   constraints, not on being alone.

use std::sync::Arc;
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use pt_telemetry::{Directory, UsageStore};

use crate::clock::Clock;
use crate::failover::HEARTBEAT_RETENTION;
use crate::planner::CapacityPlanner;
use crate::service::{Service, LEADER_LEASE};
use crate::store::Store;
use crate::telemetry::CpTelemetry;

/// How long a lease lasts without renewal.
pub const LEASE_TTL: SignedDuration = SignedDuration::from_secs(15);
/// How often the holder renews it (and others try to take it).
pub const LEASE_RENEW: Duration = Duration::from_secs(5);
/// How often every instance picks up the shared version.
pub const VERSION_POLL: Duration = Duration::from_millis(500);
/// How often the leader reconciles capacity counters.
pub const RECONCILE_EVERY: Duration = Duration::from_secs(60);
/// How often the leader prunes old heartbeats and usage.
pub const PRUNE_EVERY: Duration = Duration::from_secs(3_600);

/// A leader-only job that runs every `every`.
struct Job {
    every: Duration,
    last: Option<tokio::time::Instant>,
}

impl Job {
    fn new(every: Duration) -> Self {
        Self { every, last: None }
    }

    fn due(&mut self, now: tokio::time::Instant) -> bool {
        let due = self
            .last
            .is_none_or(|l| now.duration_since(l) >= self.every);
        if due {
            self.last = Some(now);
        }
        due
    }
}

/// Take or renew leadership. Returns whether this instance leads now.
pub async fn lead<S: Store, P: CapacityPlanner, C: Clock>(
    svc: &Service<S, P, C>,
    holder: &str,
) -> bool {
    match svc
        .store
        .try_lease(LEADER_LEASE, holder, svc.clock.now(), LEASE_TTL)
        .await
    {
        Ok(leading) => leading,
        Err(e) => {
            // Without the store we can't prove leadership; stop acting as leader.
            tracing::warn!(error = %e, "leader lease check failed");
            false
        }
    }
}

/// Run forever: the version poller on this instance, and the leader loops while this
/// instance holds the lease. `holder` identifies the instance, and must be unique.
pub async fn run<S: Store, P: CapacityPlanner, C: Clock>(
    svc: Arc<Service<S, P, C>>,
    telemetry: Arc<CpTelemetry<S, P, C>>,
    holder: String,
) {
    let poller = svc.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(VERSION_POLL);
        loop {
            tick.tick().await;
            if let Err(e) = poller.sync_version().await {
                tracing::debug!(error = %e, "version poll failed");
            }
        }
    });

    let config = &svc.config;
    let mut lifecycle = Job::new(Duration::from_secs(config.server.lifecycle_interval_secs));
    let mut failover = Job::new(Duration::from_millis(config.failover.check_interval_ms));
    let mut reconcile = Job::new(RECONCILE_EVERY);
    let mut invoices = Job::new(Duration::from_secs(config.billing.finalize_interval_secs));
    let mut prune = Job::new(PRUNE_EVERY);
    let retention_ms = config.telemetry.retention_days * 86_400_000;

    let mut leading = false;
    let mut renewed: Option<tokio::time::Instant> = None;
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    loop {
        tick.tick().await;
        let now = tokio::time::Instant::now();
        if renewed.is_none_or(|r| now.duration_since(r) >= LEASE_RENEW) {
            let was = leading;
            leading = lead(&svc, &holder).await;
            renewed = Some(now);
            if leading != was {
                tracing::info!(%holder, leading, "leadership changed");
            }
        }
        if !leading {
            continue;
        }
        if failover.due(now) {
            let r = svc.run_failover().await;
            for (id, region) in &r.declared {
                tracing::warn!(%id, %region, "region down: failover entitlements active");
            }
            for (id, region) in &r.resolved {
                tracing::info!(%id, %region, "region recovered: returning traffic gradually");
            }
        }
        if lifecycle.due(now) {
            let r = svc.run_lifecycle().await;
            if r != Default::default() {
                tracing::info!(?r, "lifecycle run");
            }
        }
        if reconcile.due(now) {
            match svc.reconcile_capacity().await {
                Ok(drift) if !drift.is_empty() => tracing::warn!(?drift, "capacity drift"),
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "capacity reconcile failed"),
            }
        }
        if invoices.due(now) {
            match crate::billing::finalize_due(&svc, &telemetry).await {
                Ok(done) if !done.is_empty() => {
                    tracing::info!(invoices = done.len(), "finalised invoices")
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "invoice finalisation failed; retrying later"),
            }
        }
        if prune.due(now) {
            let before: Timestamp = svc.clock.now() - HEARTBEAT_RETENTION;
            if let Err(e) = svc.store.prune_heartbeats(before).await {
                tracing::warn!(error = %e, "heartbeat pruning failed");
            }
            let now_ms = telemetry.directory.now_ms();
            match telemetry
                .store
                .prune(now_ms.saturating_sub(retention_ms))
                .await
            {
                Ok(0) => {}
                Ok(removed) => tracing::info!(removed, "pruned old usage records"),
                Err(e) => tracing::warn!(error = %e, "usage pruning failed"),
            }
        }
    }
}
