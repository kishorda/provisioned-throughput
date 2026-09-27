//! The dashboards' data (ADR-043).
//!
//! - **System** (operators): regions and gateways, incidents and paused sales, capacity per
//!   region and model, every pool's replicas as its capacity controller reported them,
//!   every router's workers and queues, and alerts derived from all of that.
//! - **Usage** (operators for any customer, customers for themselves): each reservation's
//!   contract, utilisation and latency over a window, SLA month to date, a right-sizing
//!   recommendation, and invoices.
//!
//! Pools and routers report into the store ([`record`]), so every control-plane instance
//! serves the same view. The pages themselves are static (`dashboard/*.html`) and read
//! these documents.

use std::collections::BTreeMap;

use jiff::{SignedDuration, Timestamp};
use pt_entitlement::report::{PoolReport, RouterReport};
use pt_telemetry::usage::{self, Granularity};
use pt_telemetry::{sla, Directory, UsageStore};
use serde::Serialize;
use serde_json::Value;

use crate::capacity::MICRO;
use crate::clock::Clock;
use crate::failover::{Health, RegionStatus};
use crate::model::{ProvisionedThroughput, RegionIncident, State};
use crate::planner::CapacityPlanner;
use crate::service::{Service, ServiceError};
use crate::store::{ComponentReport, Lease, SalesHold, Store};
use crate::telemetry::CpTelemetry;

pub const POOL: &str = "pool";
pub const ROUTER: &str = "router";

/// A report older than this is shown as stale. Controllers reconcile at least every 5
/// minutes and routers report every 10 s.
pub const POOL_STALE: SignedDuration = SignedDuration::from_mins(10);
pub const ROUTER_STALE: SignedDuration = SignedDuration::from_secs(60);
/// Stale reports are dropped after this long.
pub const REPORT_RETENTION: SignedDuration = SignedDuration::from_hours(24);

/// Keep a pool's report.
pub async fn record_pool<S: Store, P: CapacityPlanner, C: Clock>(
    svc: &Service<S, P, C>,
    region: &str,
    report: &PoolReport,
) -> Result<(), ServiceError> {
    record(
        svc,
        POOL,
        region,
        &report.id(),
        serde_json::to_value(report).expect("serialises"),
    )
    .await
}

/// Keep a router's report.
pub async fn record_router<S: Store, P: CapacityPlanner, C: Clock>(
    svc: &Service<S, P, C>,
    region: &str,
    report: &RouterReport,
) -> Result<(), ServiceError> {
    if report.router_id.is_empty() || report.router_id.len() > 128 {
        return Err(ServiceError::Validation {
            field: "router_id".into(),
            message: "router_id must be 1–128 characters.".into(),
        });
    }
    record(
        svc,
        ROUTER,
        region,
        &report.router_id,
        report.status.clone(),
    )
    .await
}

async fn record<S: Store, P: CapacityPlanner, C: Clock>(
    svc: &Service<S, P, C>,
    kind: &str,
    region: &str,
    id: &str,
    body: Value,
) -> Result<(), ServiceError> {
    svc.store
        .put_report(ComponentReport {
            kind: kind.into(),
            region: region.into(),
            id: id.into(),
            body,
            reported_at: svc.clock.now(),
        })
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------------------
// System view.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Critical,
    Warning,
    Info,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Alert {
    pub severity: Severity,
    /// What it's about, for example `region eu-west` or `pool pt-serving/maverick`.
    pub scope: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ControlPlaneView {
    pub entitlement_version: u64,
    /// The instance running the background loops (ADR-023).
    pub leader: Option<Lease>,
    /// The snapshot signing key id, or `vault` (ADR-038).
    pub signing_key: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RegionView {
    #[serde(flatten)]
    pub status: RegionStatus,
    pub open_incidents: Vec<RegionIncident>,
    pub sales_holds: Vec<SalesHold>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Arrival {
    pub from: Timestamp,
    pub add_replicas: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CapacityView {
    pub region: String,
    pub model: String,
    pub profile: String,
    /// Configured sellable replicas, and what's reserved of them.
    pub replicas: u32,
    pub reserved_replicas: f64,
    pub used_pct: f64,
    /// CUs that could still be sold now, per tier the model is offered at.
    pub free_cus: BTreeMap<String, u32>,
    /// Reservations held here (shares and failover headroom), and their CUs.
    pub reservations: usize,
    pub cus_sold: u32,
    pub scheduled: Vec<Arrival>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportView<T> {
    pub region: String,
    pub id: String,
    pub reported_at: Timestamp,
    pub age_secs: i64,
    pub stale: bool,
    pub report: T,
}

#[derive(Debug, Clone, Serialize)]
pub struct SystemView {
    pub generated_at: Timestamp,
    pub control_plane: ControlPlaneView,
    pub alerts: Vec<Alert>,
    pub regions: Vec<RegionView>,
    pub capacity: Vec<CapacityView>,
    pub pools: Vec<ReportView<PoolReport>>,
    pub routers: Vec<ReportView<Value>>,
}

pub async fn system<S: Store, P: CapacityPlanner, C: Clock>(
    svc: &Service<S, P, C>,
    telemetry: &CpTelemetry<S, P, C>,
) -> Result<SystemView, ServiceError> {
    let now = svc.clock.now();
    let version = svc.sync_version().await?;
    let leader = svc
        .store
        .lease(crate::service::LEADER_LEASE)
        .await?
        .filter(|l| l.expires_at > now);
    let signing_key = match svc.signer() {
        crate::signing::Signer::Local(s) => s.key_id(),
        crate::signing::Signer::Vault(_) => "vault".into(),
    };
    let incidents = svc.incidents().await?;
    let holds = svc.sales_holds().await?;
    let regions: Vec<RegionView> = svc
        .region_statuses()
        .await?
        .into_iter()
        .map(|status| RegionView {
            open_incidents: incidents
                .iter()
                .filter(|i| i.region == status.region && i.ended_at.is_none())
                .cloned()
                .collect(),
            sales_holds: holds
                .iter()
                .filter(|h| h.region == status.region)
                .cloned()
                .collect(),
            status,
        })
        .collect();

    // Everything sold, from every tenant.
    let mut live = Vec::new();
    for t in &svc.config.tenants {
        live.extend(
            svc.store
                .list(&t.id)
                .await?
                .into_iter()
                .filter(|pt| is_live(pt.state)),
        );
    }
    let mut capacity = Vec::new();
    for c in &svc.config.capacity {
        let (cap, reserved) = svc
            .planner
            .pool_micro(&c.region, &c.model)
            .await
            .unwrap_or((u64::from(c.replicas) * MICRO, 0));
        let offered = svc
            .config
            .model(&c.model)
            .map(|m| m.tiers.clone())
            .unwrap_or_default();
        let free_cus = offered
            .iter()
            .map(|t| {
                let n = svc
                    .costs()
                    .cus_in(&c.region, &c.model, *t, cap.saturating_sub(reserved))
                    .unwrap_or(0);
                (t.as_str().to_string(), n)
            })
            .collect();
        let held_here: Vec<(&ProvisionedThroughput, u32)> = live
            .iter()
            .filter(|pt| pt.model == c.model)
            .filter_map(|pt| {
                let cus: u32 = crate::service::held(pt)
                    .iter()
                    .filter(|s| s.region == c.region)
                    .map(|s| s.cus)
                    .sum();
                (cus > 0).then_some((pt, cus))
            })
            .collect();
        let scheduled = svc
            .config
            .capacity_changes
            .iter()
            .filter(|a| a.region == c.region && a.model == c.model && a.from > now)
            .map(|a| Arrival {
                from: a.from,
                add_replicas: a.add_replicas,
            })
            .collect();
        capacity.push(CapacityView {
            region: c.region.clone(),
            model: c.model.clone(),
            profile: c.profile.clone(),
            replicas: c.replicas,
            reserved_replicas: reserved as f64 / MICRO as f64,
            used_pct: if cap > 0 {
                100.0 * reserved as f64 / cap as f64
            } else {
                0.0
            },
            free_cus,
            reservations: held_here.len(),
            cus_sold: held_here.iter().map(|(_, c)| c).sum(),
            scheduled,
        });
    }

    let age = |at: Timestamp| now.duration_since(at).as_secs();
    let pools = svc
        .store
        .reports(POOL)
        .await?
        .into_iter()
        .filter_map(|r| {
            let report: PoolReport = serde_json::from_value(r.body).ok()?;
            Some(ReportView {
                age_secs: age(r.reported_at),
                stale: now.duration_since(r.reported_at) > POOL_STALE,
                region: r.region,
                id: r.id,
                reported_at: r.reported_at,
                report,
            })
        })
        .collect::<Vec<_>>();
    let routers = svc
        .store
        .reports(ROUTER)
        .await?
        .into_iter()
        .map(|r| ReportView {
            age_secs: age(r.reported_at),
            stale: now.duration_since(r.reported_at) > ROUTER_STALE,
            region: r.region,
            id: r.id,
            reported_at: r.reported_at,
            report: r.body,
        })
        .collect::<Vec<_>>();

    // SLA month to date for every live reservation, for the at-risk alert.
    let mut at_risk = Vec::new();
    for pt in live.iter().filter(|pt| pt.state == State::Active) {
        if let Some((att, target)) = sla_attainment(svc, telemetry, pt).await? {
            if att < target {
                at_risk.push((pt.tenant.clone(), pt.id.clone(), att, target));
            }
        }
    }

    let mut view = SystemView {
        generated_at: now,
        control_plane: ControlPlaneView {
            entitlement_version: version,
            leader,
            signing_key,
        },
        alerts: vec![],
        regions,
        capacity,
        pools,
        routers,
    };
    view.alerts = alerts(&view, &at_risk);
    Ok(view)
}

fn is_live(s: State) -> bool {
    matches!(
        s,
        State::Scheduled | State::Active | State::PendingCancellation
    )
}

/// SLA attainment this month so far and its target, if any window counted.
async fn sla_attainment<S: Store, P: CapacityPlanner, C: Clock>(
    svc: &Service<S, P, C>,
    telemetry: &CpTelemetry<S, P, C>,
    pt: &ProvisionedThroughput,
) -> Result<Option<(f64, f64)>, ServiceError> {
    let now = telemetry.directory.now_ms();
    let month = sla::month_of(now);
    let (start, end) = sla::month_bounds(&month).expect("valid month");
    let Some(info) = telemetry
        .directory
        .reservation(&pt.tenant, &pt.id)
        .await
        .map_err(|e| ServiceError::Unavailable(e.to_string()))?
    else {
        return Ok(None);
    };
    let records = telemetry
        .store
        .range(&pt.tenant, &pt.id, start, end)
        .await
        .map_err(|e| ServiceError::Unavailable(e.to_string()))?;
    let _ = svc;
    let report = sla::report(
        &info,
        &records,
        &month,
        (start, end.min(now.max(start))),
        false,
    );
    Ok((report.windows > 0).then_some((report.attainment_pct, report.target_attainment_pct)))
}

/// A condition's message, else its reason.
fn why(c: &pt_entitlement::report::ReportCondition) -> &str {
    let m = c.message.trim_end_matches('.');
    if !m.is_empty() {
        m
    } else if !c.reason.is_empty() {
        &c.reason
    } else {
        "no reason given"
    }
}

/// Everything worth a look, most severe first.
pub fn alerts(v: &SystemView, sla_at_risk: &[(String, String, f64, f64)]) -> Vec<Alert> {
    let mut out = Vec::new();
    let mut push = |severity, scope: String, message: String| {
        out.push(Alert {
            severity,
            scope,
            message,
        })
    };
    if v.control_plane.leader.is_none() {
        push(
            Severity::Warning,
            "control plane".into(),
            "No instance holds the leader lease: lifecycle, failover, and invoices are paused."
                .into(),
        );
    }
    for r in &v.regions {
        let scope = format!("region {}", r.status.region);
        match r.status.health {
            Health::Down => push(
                Severity::Critical,
                scope.clone(),
                "No gateway is serving.".into(),
            ),
            Health::Unknown => push(
                Severity::Warning,
                scope.clone(),
                "No gateway has reported yet.".into(),
            ),
            Health::Serving if r.status.serving_gateways < r.status.gateways => push(
                Severity::Warning,
                scope.clone(),
                format!(
                    "{} of {} gateways aren't serving.",
                    r.status.gateways - r.status.serving_gateways,
                    r.status.gateways
                ),
            ),
            Health::Serving => {}
        }
        for i in &r.open_incidents {
            push(
                Severity::Warning,
                scope.clone(),
                format!("Open incident since {}: {}", i.started_at, i.description),
            );
        }
        for h in &r.sales_holds {
            push(
                Severity::Info,
                scope.clone(),
                format!(
                    "Sales of {} paused until {} ({}).",
                    h.model, h.expires_at, h.reason
                ),
            );
        }
    }
    for c in &v.capacity {
        let scope = format!("capacity {} {}", c.region, c.model);
        if c.used_pct > 100.0 + 1e-9 {
            push(
                Severity::Critical,
                scope,
                format!(
                    "Oversold: {:.1}% of configured replicas reserved.",
                    c.used_pct
                ),
            );
        } else if c.used_pct >= 90.0 {
            push(
                Severity::Warning,
                scope,
                format!("{:.0}% of configured replicas reserved.", c.used_pct),
            );
        }
    }
    for p in &v.pools {
        let scope = format!("pool {} ({})", p.id, p.region);
        let r = &p.report;
        if p.stale {
            push(
                Severity::Warning,
                scope.clone(),
                format!("Stopped reporting {} min ago.", p.age_secs / 60),
            );
        }
        if r.condition("Ready").is_some_and(|c| c.status != "True") {
            let c = r.condition("Ready").expect("checked");
            push(
                Severity::Critical,
                scope.clone(),
                format!("Not ready: {}.", why(c)),
            );
        }
        if r.is("CapacityShortfall") {
            let c = r.condition("CapacityShortfall").expect("checked");
            push(
                Severity::Critical,
                scope.clone(),
                format!("Capacity shortfall: {}.", why(c)),
            );
        }
        if r.ready.total < r.min_available.total {
            push(
                Severity::Critical,
                scope.clone(),
                format!(
                    "{} ready replicas, below the {} that must stay available.",
                    r.ready.total, r.min_available.total
                ),
            );
        } else if r.ready.total < r.desired.total {
            push(
                Severity::Info,
                scope.clone(),
                format!("{} of {} replicas ready.", r.ready.total, r.desired.total),
            );
        }
        if r.is("Draining") {
            let c = r.condition("Draining").expect("checked");
            let severity = if c.reason == "SurgeCapped" {
                Severity::Warning
            } else {
                Severity::Info
            };
            push(severity, scope.clone(), format!("Draining: {}.", why(c)));
        }
        if r.is("FailoverActive") {
            push(
                Severity::Info,
                scope,
                format!(
                    "Serving {:.0} WU/s of failover demand.",
                    r.failover_wu_per_sec
                ),
            );
        }
    }
    for r in &v.routers {
        if r.stale {
            push(
                Severity::Warning,
                format!("router {} ({})", r.id, r.region),
                format!("Stopped reporting {} s ago.", r.age_secs),
            );
        }
    }
    for (tenant, id, att, target) in sla_at_risk {
        push(
            Severity::Warning,
            format!("reservation {id} ({tenant})"),
            format!("SLA attainment this month is {att:.2}%, below the {target:.1}% commitment."),
        );
    }
    out.sort_by_key(|a| a.severity);
    out
}

// ---------------------------------------------------------------------------------------
// Customers and usage.

#[derive(Debug, Clone, Serialize)]
pub struct CustomerSummary {
    pub tenant: String,
    pub reservations: usize,
    pub cus: u32,
    pub monthly: u64,
    pub currency: String,
    pub models: Vec<String>,
}

/// Every customer, for the operator's picker.
pub async fn customers<S: Store, P: CapacityPlanner, C: Clock>(
    svc: &Service<S, P, C>,
) -> Result<Vec<CustomerSummary>, ServiceError> {
    let mut out = Vec::new();
    for t in &svc.config.tenants {
        let live: Vec<ProvisionedThroughput> = svc
            .store
            .list(&t.id)
            .await?
            .into_iter()
            .filter(|pt| is_live(pt.state))
            .collect();
        let mut models: Vec<String> = live.iter().map(|pt| pt.model.clone()).collect();
        models.sort();
        models.dedup();
        out.push(CustomerSummary {
            tenant: t.id.clone(),
            reservations: live.len(),
            cus: live.iter().map(|pt| pt.cus).sum(),
            monthly: live.iter().map(|pt| pt.price.monthly).sum(),
            currency: svc.config.pricing.currency.clone(),
            models,
        });
    }
    Ok(out)
}

#[derive(Debug, Clone, Serialize)]
pub struct ReservationUsage {
    pub reservation: ProvisionedThroughput,
    pub usage: usage::UsageReport,
    pub sla: sla::SlaReport,
    /// From the reservation's recent traffic, when there's enough of it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recommendation: Option<crate::quote::Recommendation>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UsageView {
    pub tenant: String,
    pub generated_at: Timestamp,
    pub from: Timestamp,
    pub to: Timestamp,
    pub reservations: Vec<ReservationUsage>,
    pub invoices: Vec<crate::billing::Invoice>,
}

/// A customer's reservations and their usage over the last `hours`.
pub async fn usage<S: Store, P: CapacityPlanner, C: Clock>(
    svc: &Service<S, P, C>,
    telemetry: &CpTelemetry<S, P, C>,
    tenant: &str,
    hours: u64,
) -> Result<UsageView, ServiceError> {
    let now = svc.clock.now();
    let hours = hours.clamp(1, 24 * 35);
    let to_ms = telemetry.directory.now_ms();
    let from_ms = to_ms.saturating_sub(hours * 3_600_000);
    let granularity = [
        Granularity::FiveMinutes,
        Granularity::Hour,
        Granularity::Day,
    ]
    .into_iter()
    .find(|g| (to_ms - from_ms) / g.millis() <= 300)
    .unwrap_or(Granularity::Day);
    let unavailable = |e: pt_telemetry::store::UsageError| ServiceError::Unavailable(e.to_string());

    let mut out = Vec::new();
    let mut list = svc.store.list(tenant).await?;
    list.retain(|pt| is_live(pt.state));
    for pt in list {
        let Some(info) = telemetry
            .directory
            .reservation(tenant, &pt.id)
            .await
            .map_err(|e| ServiceError::Unavailable(e.to_string()))?
        else {
            continue;
        };
        let records = telemetry
            .store
            .range(tenant, &pt.id, from_ms, to_ms)
            .await
            .map_err(unavailable)?;
        let usage = usage::report(&info, &records, from_ms, to_ms, granularity);
        let month = sla::month_of(to_ms);
        let (start, end) = sla::month_bounds(&month).expect("valid month");
        let month_records = telemetry
            .store
            .range(tenant, &pt.id, start, end)
            .await
            .map_err(unavailable)?;
        let sla = sla::report(
            &info,
            &month_records,
            &month,
            (start, end.min(to_ms.max(start))),
            false,
        );
        let recommendation = if records.is_empty() {
            None
        } else {
            let req = crate::quote::QuoteRequest {
                from_reservation: Some(pt.id.clone()),
                ..Default::default()
            };
            crate::quote::quote(svc, &telemetry.store, tenant, req)
                .await
                .ok()
                .and_then(|q| q.recommendation)
        };
        out.push(ReservationUsage {
            reservation: pt,
            usage,
            sla,
            recommendation,
        });
    }
    let invoices = crate::billing::list(svc, telemetry, tenant).await?;
    Ok(UsageView {
        tenant: tenant.into(),
        generated_at: now,
        from: Timestamp::from_millisecond(from_ms as i64).unwrap_or(now),
        to: Timestamp::from_millisecond(to_ms as i64).unwrap_or(now),
        reservations: out,
        invoices,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pt_entitlement::report::{ReportCondition, Roles};

    fn view(capacity: Vec<CapacityView>, pools: Vec<ReportView<PoolReport>>) -> SystemView {
        let now: Timestamp = "2026-10-01T00:00:00Z".parse().unwrap();
        SystemView {
            generated_at: now,
            control_plane: ControlPlaneView {
                entitlement_version: 1,
                leader: Some(Lease {
                    name: "background".into(),
                    holder: "cp-1".into(),
                    expires_at: now,
                }),
                signing_key: "k".into(),
            },
            alerts: vec![],
            regions: vec![],
            capacity,
            pools,
            routers: vec![],
        }
    }

    fn capacity(used_pct: f64) -> CapacityView {
        CapacityView {
            region: "eu-west".into(),
            model: "m".into(),
            profile: "p".into(),
            replicas: 8,
            reserved_replicas: 8.0 * used_pct / 100.0,
            used_pct,
            free_cus: BTreeMap::new(),
            reservations: 1,
            cus_sold: 10,
            scheduled: vec![],
        }
    }

    fn pool(reason: &str) -> ReportView<PoolReport> {
        let r = |n| Roles {
            aggregated: n,
            total: n,
            ..Default::default()
        };
        ReportView {
            region: "eu-west".into(),
            id: "pt/p".into(),
            reported_at: "2026-10-01T00:00:00Z".parse().unwrap(),
            age_secs: 5,
            stale: false,
            report: PoolReport {
                namespace: "pt".into(),
                name: "p".into(),
                model: "m".into(),
                catalog_model: None,
                profile: "p".into(),
                engine: "vllm 0.11".into(),
                desired: r(8),
                ready: r(8),
                floor: r(6),
                min_available: r(6),
                hot_spares: 1,
                warm_spares_loaded: Roles::default(),
                drain_surge: Roles::default(),
                draining_nodes: vec!["n1".into()],
                allocations: 1,
                allocated_wu_per_sec: 1.0,
                failover_wu_per_sec: 0.0,
                conditions: vec![ReportCondition {
                    type_: "Draining".into(),
                    status: "True".into(),
                    reason: reason.into(),
                    message: String::new(),
                }],
            },
        }
    }

    fn severities(v: &SystemView) -> Vec<(Severity, String)> {
        alerts(v, &[])
            .into_iter()
            .map(|a| (a.severity, a.scope))
            .collect()
    }

    #[test]
    fn capacity_alerts_by_how_full_the_pool_is() {
        let scope = "capacity eu-west m".to_string();
        assert!(severities(&view(vec![capacity(50.0)], vec![])).is_empty());
        assert_eq!(
            severities(&view(vec![capacity(95.0)], vec![])),
            [(Severity::Warning, scope.clone())]
        );
        assert_eq!(
            severities(&view(vec![capacity(101.0)], vec![])),
            [(Severity::Critical, scope)]
        );
    }

    #[test]
    fn a_capped_drain_surge_is_a_warning_and_a_normal_drain_is_info() {
        let scope = "pool pt/p (eu-west)".to_string();
        assert_eq!(
            severities(&view(vec![], vec![pool("Surge")])),
            [(Severity::Info, scope.clone())]
        );
        assert_eq!(
            severities(&view(vec![], vec![pool("SurgeCapped")])),
            [(Severity::Warning, scope)]
        );
    }

    #[test]
    fn sla_at_risk_is_a_warning_sorted_after_critical() {
        let v = view(vec![capacity(101.0)], vec![]);
        let a = alerts(&v, &[("acme".into(), "pt-1".into(), 99.5, 99.8)]);
        assert_eq!(a[0].severity, Severity::Critical);
        assert_eq!(a[1].severity, Severity::Warning);
        assert!(a[1].message.contains("99.50%"), "{}", a[1].message);
    }
}
