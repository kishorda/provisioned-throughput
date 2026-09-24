//! Usage and SLA telemetry (docs/09), served by the control plane.
//!
//! [`CpDirectory`] gives `pt-telemetry` the tenant keys, region tokens, and reservation
//! details it needs, so the telemetry crate doesn't depend on control-plane storage.

use std::sync::Arc;

use axum::Router;
use pt_telemetry::{
    Directory, DirectoryError, ExclusionWindow, ReservationInfo, Telemetry, UsageBackend,
};

use crate::clock::Clock;
use crate::config::TelemetryConfig;
use crate::model::{EventKind, ProvisionedThroughput, RegionIncident, Sku};
use crate::planner::CapacityPlanner;
use crate::service::{Service, ServiceError};
use crate::store::Store;

pub struct CpDirectory<S, P, C>(pub Arc<Service<S, P, C>>);

impl<S: Store, P: CapacityPlanner, C: Clock> Directory for CpDirectory<S, P, C> {
    fn tenant_for_key(&self, key: &str) -> Option<String> {
        self.0.config.tenant_for_key(key).map(str::to_owned)
    }

    fn region_for_token(&self, token: &str) -> Option<String> {
        self.0.config.region_for_token(token).map(str::to_owned)
    }

    async fn reservation(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<ReservationInfo>, DirectoryError> {
        let pt = match self.0.get(tenant, id).await {
            Ok(pt) => pt,
            Err(ServiceError::NotFound) => return Ok(None),
            Err(e) => return Err(DirectoryError(e.to_string())),
        };
        let incidents = self
            .0
            .incidents()
            .await
            .map_err(|e| DirectoryError(e.to_string()))?;
        let exclusions =
            exclusion_windows(&pt, &incidents, &self.0.config.telemetry, self.now_ms());
        Ok(Some(ReservationInfo {
            exclusions,
            entitlement_wu_s: f64::from(pt.cus) * self.0.config.telemetry.wu_per_cu,
            id: pt.id,
            tenant: pt.tenant,
            tier: pt.tier,
            cus: pt.cus,
            monthly_price: pt.price.monthly,
            currency: pt.price.currency,
            shape: pt.shape,
        }))
    }

    fn now_ms(&self) -> u64 {
        self.0.clock.now().as_millisecond().max(0) as u64
    }
}

pub type CpTelemetry<S, P, C> = Telemetry<UsageBackend, CpDirectory<S, P, C>>;

/// Telemetry for `svc` over `usage`, with its routes.
pub fn telemetry<S: Store, P: CapacityPlanner, C: Clock>(
    svc: Arc<Service<S, P, C>>,
    usage: UsageBackend,
) -> (Router, Arc<CpTelemetry<S, P, C>>) {
    let tel = Arc::new(Telemetry {
        store: usage,
        directory: CpDirectory(svc),
    });
    (pt_telemetry::api::router(tel.clone()), tel)
}

fn ms(t: jiff::Timestamp) -> u64 {
    t.as_millisecond().max(0) as u64
}

/// SLA exclusion windows for one reservation (docs/09 §4).
///
/// - **Customer-initiated changes:** activation, CU increases, shape changes, and changes
///   applied at renewal each exclude `change_grace_minutes` from the event, while capacity
///   and placement catch up.
/// - **Region incidents** in a region the reservation uses:
///   - Multi-region SKU: the first `failover_window_minutes` of the incident, every region.
///   - Regional SKU: the whole incident, requests served in that region only.
pub fn exclusion_windows(
    pt: &ProvisionedThroughput,
    incidents: &[RegionIncident],
    config: &TelemetryConfig,
    now_ms: u64,
) -> Vec<ExclusionWindow> {
    let grace = config.change_grace_minutes * 60_000;
    let failover = config.failover_window_minutes * 60_000;
    let mut out: Vec<ExclusionWindow> = pt
        .events
        .iter()
        .filter_map(|e| {
            let reason = match e.kind {
                EventKind::Activated { .. } => "activation",
                EventKind::CapacityIncreased { .. } => "resize",
                EventKind::ShapeUpdated => "shape_change",
                EventKind::ChangeApplied { .. } => "scheduled_change",
                _ => return None,
            };
            let start = ms(e.at);
            Some(ExclusionWindow {
                start_ms: start,
                end_ms: start + grace,
                reason: reason.into(),
                region: None,
            })
        })
        .filter(|w| w.end_ms > w.start_ms)
        .collect();

    for inc in incidents {
        if !pt.regions.iter().any(|r| r.region == inc.region) {
            continue;
        }
        let start = ms(inc.started_at);
        // An open incident runs until now.
        let end = inc.ended_at.map_or(now_ms.max(start + 1), ms);
        out.push(match pt.sku {
            Sku::MultiRegion => ExclusionWindow {
                start_ms: start,
                end_ms: end.min(start + failover),
                reason: "failover".into(),
                region: None,
            },
            Sku::Regional => ExclusionWindow {
                start_ms: start,
                end_ms: end,
                reason: "region_outage".into(),
                region: Some(inc.region.clone()),
            },
        });
    }
    out.sort_by_key(|w| w.start_ms);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Event, Price, RegionShare, State};
    use jiff::Timestamp;
    use pt_core::{PoolIsolation, Shape, TermMonths, Tier};

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    fn pt(sku: Sku, events: Vec<(&str, EventKind)>) -> ProvisionedThroughput {
        ProvisionedThroughput {
            id: "pt-1".into(),
            tenant: "acme".into(),
            name: "n".into(),
            model: "m".into(),
            tier: Tier::Agentic,
            regions: vec![
                RegionShare {
                    region: "eu-west".into(),
                    cus: 2,
                },
                RegionShare {
                    region: "eu-central".into(),
                    cus: 2,
                },
            ],
            cus: 4,
            sku,
            isolation: PoolIsolation::Shared,
            shape: Shape {
                input_p95: 1,
                input_max: 1,
                output_p95: 1,
                context_ceiling: 1,
                cache_hit_ratio: 0.0,
                burst_factor: 1.0,
            },
            boundary_policy: Default::default(),
            term_months: TermMonths::One,
            term_start: at("2026-10-01T00:00:00Z"),
            term_end: at("2026-11-01T00:00:00Z"),
            auto_renew: true,
            state: State::Active,
            pending_changes: None,
            endpoints: vec![],
            price: Price {
                currency: "USD".into(),
                per_cu_monthly: 0,
                monthly: 0,
            },
            failover_headroom: vec![],
            deployments: vec![],
            version: 1,
            created_at: at("2026-10-01T00:00:00Z"),
            updated_at: at("2026-10-01T00:00:00Z"),
            events: events
                .into_iter()
                .map(|(t, kind)| Event { at: at(t), kind })
                .collect(),
        }
    }

    fn incident(region: &str, start: &str, end: Option<&str>) -> RegionIncident {
        RegionIncident {
            id: "inc".into(),
            region: region.into(),
            started_at: at(start),
            ended_at: end.map(at),
            description: "d".into(),
            declared_at: at(start),
            source: Default::default(),
        }
    }

    #[test]
    fn customer_changes_open_grace_windows() {
        let p = pt(
            Sku::Regional,
            vec![
                (
                    "2026-10-01T00:00:00Z",
                    EventKind::Created { cus: 4, monthly: 0 },
                ),
                (
                    "2026-10-01T00:00:00Z",
                    EventKind::Activated {
                        cus: None,
                        tier: None,
                        monthly: None,
                    },
                ),
                ("2026-10-05T12:00:00Z", EventKind::BoundaryPolicyUpdated),
                (
                    "2026-10-07T09:00:00Z",
                    EventKind::CapacityIncreased {
                        region: "eu-west".into(),
                        from: 2,
                        to: 4,
                        prorated_charge: 0,
                        cus: None,
                        monthly: None,
                    },
                ),
            ],
        );
        let w = exclusion_windows(&p, &[], &TelemetryConfig::default(), 0);
        let reasons: Vec<_> = w.iter().map(|w| w.reason.as_str()).collect();
        assert_eq!(
            reasons,
            ["activation", "resize"],
            "policy changes don't affect capacity"
        );
        assert_eq!(w[1].end_ms - w[1].start_ms, 10 * 60_000);
        assert!(w.iter().all(|w| w.region.is_none()));
    }

    #[test]
    fn incidents_depend_on_sku() {
        let incidents = [
            incident(
                "eu-west",
                "2026-10-10T08:00:00Z",
                Some("2026-10-10T09:00:00Z"),
            ),
            incident("us-east", "2026-10-11T08:00:00Z", None), // not one of this reservation's regions
        ];
        let cfg = TelemetryConfig::default();

        let multi = exclusion_windows(&pt(Sku::MultiRegion, vec![]), &incidents, &cfg, 0);
        assert_eq!(multi.len(), 1);
        assert_eq!(multi[0].reason, "failover");
        assert_eq!(
            multi[0].end_ms - multi[0].start_ms,
            5 * 60_000,
            "failover window only"
        );
        assert_eq!(multi[0].region, None);

        let regional = exclusion_windows(&pt(Sku::Regional, vec![]), &incidents, &cfg, 0);
        assert_eq!(regional[0].reason, "region_outage");
        assert_eq!(
            regional[0].end_ms - regional[0].start_ms,
            3_600_000,
            "the whole incident"
        );
        assert_eq!(regional[0].region.as_deref(), Some("eu-west"));

        // An open incident runs until now.
        let open = [incident("eu-central", "2026-10-12T08:00:00Z", None)];
        let now = ms(at("2026-10-12T08:30:00Z"));
        let w = exclusion_windows(&pt(Sku::Regional, vec![]), &open, &cfg, now);
        assert_eq!(w[0].end_ms, now);
        // A failover shorter than the window ends with the incident.
        let short = [incident(
            "eu-west",
            "2026-10-12T08:00:00Z",
            Some("2026-10-12T08:02:00Z"),
        )];
        let w = exclusion_windows(&pt(Sku::MultiRegion, vec![]), &short, &cfg, 0);
        assert_eq!(w[0].end_ms - w[0].start_ms, 2 * 60_000);
    }
}
