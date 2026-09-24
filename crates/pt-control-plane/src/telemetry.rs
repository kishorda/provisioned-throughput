//! Usage and SLA telemetry (docs/09), served by the control plane.
//!
//! [`CpDirectory`] gives `pt-telemetry` the tenant keys, region tokens, and reservation
//! details it needs, so the telemetry crate doesn't depend on control-plane storage.

use std::sync::Arc;

use axum::Router;
use pt_telemetry::{Directory, MemoryUsageStore, ReservationInfo, Telemetry};

use crate::clock::Clock;
use crate::planner::CapacityPlanner;
use crate::service::Service;
use crate::store::Store;

pub struct CpDirectory<S, P, C>(pub Arc<Service<S, P, C>>);

impl<S: Store, P: CapacityPlanner, C: Clock> Directory for CpDirectory<S, P, C> {
    fn tenant_for_key(&self, key: &str) -> Option<String> {
        self.0.config.tenant_for_key(key).map(str::to_owned)
    }

    fn region_for_token(&self, token: &str) -> Option<String> {
        self.0.config.region_for_token(token).map(str::to_owned)
    }

    async fn reservation(&self, tenant: &str, id: &str) -> Option<ReservationInfo> {
        let pt = self.0.get(tenant, id).await.ok()?;
        Some(ReservationInfo {
            entitlement_wu_s: f64::from(pt.cus) * self.0.config.telemetry.wu_per_cu,
            id: pt.id,
            tenant: pt.tenant,
            tier: pt.tier,
            cus: pt.cus,
            monthly_price: pt.price.monthly,
            currency: pt.price.currency,
            shape: pt.shape,
        })
    }

    fn now_ms(&self) -> u64 {
        self.0.clock.now().as_millisecond().max(0) as u64
    }
}

pub type CpTelemetry<S, P, C> = Telemetry<MemoryUsageStore, CpDirectory<S, P, C>>;

/// Telemetry for `svc`, with its routes.
pub fn telemetry<S: Store, P: CapacityPlanner, C: Clock>(
    svc: Arc<Service<S, P, C>>,
) -> (Router, Arc<CpTelemetry<S, P, C>>) {
    let tel = Arc::new(Telemetry {
        store: MemoryUsageStore::default(),
        directory: CpDirectory(svc),
    });
    (pt_telemetry::api::router(tel.clone()), tel)
}
