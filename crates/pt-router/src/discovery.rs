//! Find workers and hot spares through the pool's headless Services (ADR-044).
//!
//! The capacity controller labels each Ready worker pod `floor` or `spare` and renders two
//! headless Services on that label. Every `refresh_secs` the router resolves both names;
//! each address is a worker `http://ip:port`, and the ones from the spares Service are hot
//! spares. The dispatcher retires workers that disappear, so running requests finish.
//!
//! DNS can fail, and an empty answer can mean a Service with no Ready pods or a lookup
//! problem. So:
//! - a floor lookup that fails or returns nothing keeps the last known floor workers;
//! - a failed spares lookup means "no spares" only when the floor lookup worked in the same
//!   refresh (DNS is fine, the Service just has no endpoints); otherwise the last known
//!   spares stay;
//! - an address in both answers (a pod changing label between lookups) counts as floor.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::config::DiscoveryConfig;
use crate::dispatch::WorkerSpec;
use crate::http::Shared;

pub struct Discovery {
    config: DiscoveryConfig,
    floor: BTreeSet<SocketAddr>,
    spares: BTreeSet<SocketAddr>,
}

impl Discovery {
    pub fn new(config: DiscoveryConfig) -> Self {
        Self {
            config,
            floor: BTreeSet::new(),
            spares: BTreeSet::new(),
        }
    }

    /// Fold one refresh's lookups into the known workers (`None` = the lookup failed) and
    /// return the workers to have. Pure.
    pub fn update(
        &mut self,
        floor: Option<Vec<SocketAddr>>,
        spares: Option<Vec<SocketAddr>>,
    ) -> Vec<WorkerSpec> {
        let floor_ok = floor.is_some();
        if let Some(f) = floor.filter(|f| !f.is_empty()) {
            self.floor = f.into_iter().collect();
        }
        match spares {
            Some(s) => self.spares = s.into_iter().collect(),
            None if floor_ok => self.spares.clear(),
            None => {}
        }
        let spec = |addr: &SocketAddr, hot_spare: bool| WorkerSpec {
            id: addr.to_string(),
            url: format!("http://{addr}"),
            slots: self.config.slots,
            kv_blocks: self.config.kv_blocks,
            hot_spare,
        };
        self.floor
            .iter()
            .map(|a| spec(a, false))
            .chain(
                self.spares
                    .iter()
                    .filter(|a| !self.floor.contains(a))
                    .map(|a| spec(a, true)),
            )
            .collect()
    }

    /// Resolve both Services and apply the result.
    pub async fn refresh(&mut self, shared: &Shared) {
        let floor = resolve(&self.config.workers_dns).await;
        let spares = match self.config.spares_dns.clone() {
            Some(name) => resolve(&name).await,
            None => Some(vec![]),
        };
        let want = self.update(floor, spares);
        let changes = shared.set_workers(&want);
        if !changes.is_empty() {
            tracing::info!(
                added = ?changes.added,
                retired = ?changes.retired,
                spare_changed = ?changes.spare_changed,
                floor = self.floor.len(),
                spares = want.iter().filter(|w| w.hot_spare).count(),
                "workers changed"
            );
        }
    }

    /// Refresh every `refresh_secs`, forever.
    pub async fn run(mut self, shared: Arc<Shared>) {
        let mut tick = tokio::time::interval(Duration::from_secs(self.config.refresh_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            self.refresh(&shared).await;
        }
    }
}

async fn resolve(name: &str) -> Option<Vec<SocketAddr>> {
    match tokio::net::lookup_host(name).await {
        Ok(addrs) => Some(addrs.collect()),
        Err(e) => {
            tracing::debug!(name, error = %e, "worker lookup failed");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d() -> Discovery {
        Discovery::new(DiscoveryConfig {
            workers_dns: "w:8000".into(),
            spares_dns: Some("s:8000".into()),
            slots: 8,
            kv_blocks: 1000,
            refresh_secs: 5,
        })
    }

    fn a(last: u8) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, last], 8000))
    }

    fn view(w: &[WorkerSpec]) -> Vec<(String, bool)> {
        w.iter().map(|w| (w.id.clone(), w.hot_spare)).collect()
    }

    #[test]
    fn floor_and_spares_become_workers() {
        let mut d = d();
        let w = d.update(Some(vec![a(2), a(1)]), Some(vec![a(3)]));
        assert_eq!(
            view(&w),
            [
                ("10.0.0.1:8000".into(), false),
                ("10.0.0.2:8000".into(), false),
                ("10.0.0.3:8000".into(), true)
            ]
        );
        assert_eq!(w[0].url, "http://10.0.0.1:8000");
        assert_eq!((w[0].slots, w[0].kv_blocks), (8, 1000));
    }

    #[test]
    fn failed_or_empty_floor_lookups_keep_the_last_workers() {
        let mut d = d();
        d.update(Some(vec![a(1)]), Some(vec![a(3)]));
        assert_eq!(
            view(&d.update(None, None)).len(),
            2,
            "DNS down: keep everything"
        );
        assert_eq!(view(&d.update(Some(vec![]), Some(vec![a(3)]))).len(), 2);
    }

    #[test]
    fn a_failed_spares_lookup_means_none_only_when_dns_works() {
        let mut d = d();
        d.update(Some(vec![a(1)]), Some(vec![a(3)]));
        // The spares Service has no endpoints (NXDOMAIN) while the floor resolves.
        assert_eq!(
            view(&d.update(Some(vec![a(1)]), None)),
            [("10.0.0.1:8000".into(), false)]
        );
    }

    #[test]
    fn a_pod_in_both_answers_is_floor() {
        let mut d = d();
        let w = d.update(Some(vec![a(1), a(3)]), Some(vec![a(3)]));
        assert_eq!(
            view(&w),
            [
                ("10.0.0.1:8000".into(), false),
                ("10.0.0.3:8000".into(), false)
            ]
        );
    }
}
