//! Automatic region-failure handling (docs/07 §4, ADR-014).
//!
//! - **Headroom.** A Multi-region reservation holds, in each region's failover target,
//!   enough extra capacity to absorb that region's share. One region fails at a time, so a
//!   target holds the largest share that fails over to it, not the sum.
//! - **Detection.** Gateways heartbeat every few seconds. A region is down when none of its
//!   gateways has reported serving for `heartbeat_timeout_seconds`. Health is soft state:
//!   after a control-plane restart, regions are `unknown` until their gateways report.
//! - **Steering.** DNS weights per region and per reservation, from incidents rather than
//!   raw health, so operator-declared incidents drain regions too.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use jiff::{SignedDuration, Timestamp};
use serde::Serialize;

use crate::config::ControlPlaneConfig;
use crate::model::{Heartbeat, ProvisionedThroughput, RegionIncident, RegionShare, Sku};

/// Where each region's share goes if it fails: `(from, to)` pairs.
///
/// The target is the region's configured `pair` if the reservation has a share there.
/// Otherwise it's the reservation's largest other region (ties by name).
pub fn failover_targets(
    config: &ControlPlaneConfig,
    regions: &[RegionShare],
) -> Vec<(String, String)> {
    regions
        .iter()
        .filter_map(|from| {
            let paired = config
                .region(&from.region)
                .and_then(|c| c.pair.as_deref())
                .filter(|p| regions.iter().any(|r| r.region == *p));
            let to = paired.map(str::to_owned).or_else(|| {
                regions
                    .iter()
                    .filter(|r| r.region != from.region)
                    .max_by(|a, b| a.cus.cmp(&b.cus).then_with(|| b.region.cmp(&a.region)))
                    .map(|r| r.region.clone())
            })?;
            Some((from.region.clone(), to))
        })
        .collect()
}

/// Failover headroom per region for a reservation. Empty for the Regional SKU.
pub fn headroom(
    config: &ControlPlaneConfig,
    sku: Sku,
    regions: &[RegionShare],
) -> Vec<RegionShare> {
    if sku != Sku::MultiRegion {
        return vec![];
    }
    let mut out: Vec<RegionShare> = Vec::new();
    for (from, to) in failover_targets(config, regions) {
        let cus = regions
            .iter()
            .find(|r| r.region == from)
            .map_or(0, |r| r.cus);
        match out.iter_mut().find(|r| r.region == to) {
            Some(r) => r.cus = r.cus.max(cus),
            None => out.push(RegionShare { region: to, cus }),
        }
    }
    out.sort_by(|a, b| a.region.cmp(&b.region));
    out
}

/// Everything a reservation holds from the planner: its shares plus its headroom.
pub fn footprint(regions: &[RegionShare], headroom: &[RegionShare]) -> Vec<RegionShare> {
    let mut out: Vec<RegionShare> = regions.to_vec();
    for h in headroom {
        match out.iter_mut().find(|r| r.region == h.region) {
            Some(r) => r.cus += h.cus,
            None => out.push(h.clone()),
        }
    }
    out
}

/// What `new` holds beyond `old`, per region. Decreases are ignored.
pub fn growth(old: &[RegionShare], new: &[RegionShare]) -> Vec<RegionShare> {
    new.iter()
        .filter_map(|n| {
            let before = old
                .iter()
                .find(|o| o.region == n.region)
                .map_or(0, |o| o.cus);
            (n.cus > before).then(|| RegionShare {
                region: n.region.clone(),
                cus: n.cus - before,
            })
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Health {
    /// No gateway has reported since the control plane started.
    Unknown,
    Serving,
    Down,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RegionStatus {
    pub region: String,
    pub health: Health,
    /// Gateways that reported within the heartbeat timeout, and how many of them serve.
    pub gateways: usize,
    pub serving_gateways: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_serving_at: Option<Timestamp>,
    /// Start of the current unbroken run of serving heartbeats.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serving_since: Option<Timestamp>,
    /// Gateways reporting within the heartbeat timeout, by the key that signed their
    /// current snapshot (`unknown` if they didn't say). A rotation's old key can be removed
    /// once no gateway reports it.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub snapshot_key_ids: BTreeMap<String, usize>,
}

/// A gateway's last heartbeat: when, whether it was serving, and its snapshot's key.
type GatewaySeen = (Timestamp, bool, Option<String>);

#[derive(Debug, Default)]
struct Seen {
    gateways: HashMap<String, GatewaySeen>,
    last_serving: Option<Timestamp>,
    serving_since: Option<Timestamp>,
}

/// Heartbeats per region. Soft state, rebuilt from heartbeats after a restart.
#[derive(Debug, Default)]
pub struct RegionHealth {
    regions: Mutex<HashMap<String, Seen>>,
}

impl RegionHealth {
    pub fn record(&self, region: &str, hb: &Heartbeat, now: Timestamp, timeout: SignedDuration) {
        let mut all = self.regions.lock().unwrap_or_else(|e| e.into_inner());
        let seen = all.entry(region.to_string()).or_default();
        seen.gateways
            .insert(hb.gateway_id.clone(), (now, hb.serving, hb.key_id.clone()));
        // Forget gateways long gone, so the map stays small as pods churn.
        seen.gateways
            .retain(|_, (at, _, _)| now.duration_since(*at) <= timeout * 10);
        if hb.serving {
            let broken = seen
                .last_serving
                .is_none_or(|t| now.duration_since(t) > timeout);
            if broken {
                seen.serving_since = Some(now);
            }
            seen.last_serving = Some(now);
        }
    }

    pub fn status(&self, region: &str, now: Timestamp, timeout: SignedDuration) -> RegionStatus {
        let all = self.regions.lock().unwrap_or_else(|e| e.into_inner());
        let Some(seen) = all.get(region) else {
            return RegionStatus {
                region: region.into(),
                health: Health::Unknown,
                gateways: 0,
                serving_gateways: 0,
                last_serving_at: None,
                serving_since: None,
                snapshot_key_ids: BTreeMap::new(),
            };
        };
        let recent: Vec<&GatewaySeen> = seen
            .gateways
            .values()
            .filter(|(at, _, _)| now.duration_since(*at) <= timeout)
            .collect();
        let mut snapshot_key_ids = BTreeMap::new();
        for (_, _, key) in &recent {
            *snapshot_key_ids
                .entry(key.clone().unwrap_or_else(|| "unknown".into()))
                .or_default() += 1;
        }
        let serving = seen
            .last_serving
            .is_some_and(|t| now.duration_since(t) <= timeout);
        RegionStatus {
            region: region.into(),
            health: if serving {
                Health::Serving
            } else {
                Health::Down
            },
            gateways: recent.len(),
            serving_gateways: recent.iter().filter(|(_, s, _)| *s).count(),
            last_serving_at: seen.last_serving,
            serving_since: seen.serving_since.filter(|_| serving),
            snapshot_key_ids,
        }
    }
}

/// DNS steering: what a GeoDNS or global load balancer controller should publish.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Steering {
    pub generated_at: Timestamp,
    pub regions: Vec<RegionSteering>,
    pub reservations: Vec<ReservationSteering>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RegionSteering {
    #[serde(flatten)]
    pub status: RegionStatus,
    /// Open incident, or the resolved one whose return ramp is still running.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incident: Option<String>,
    /// 0 while an incident is open, ramping back to 1 after it's resolved.
    pub weight: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReservationSteering {
    pub id: String,
    pub sku: Sku,
    /// Where the reservation's global name should resolve, with weights summing to 1.
    /// Empty when none of its regions can serve.
    pub targets: Vec<Target>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Target {
    pub region: String,
    pub endpoint: String,
    pub weight: f64,
}

/// A region's DNS weight at `now`, and the incident setting it.
pub fn region_weight(
    region: &str,
    incidents: &[RegionIncident],
    now: Timestamp,
    ramp: SignedDuration,
) -> (f64, Option<String>) {
    let mut weight: f64 = 1.0;
    let mut cause = None;
    for i in incidents
        .iter()
        .filter(|i| i.region == region && i.started_at <= now)
    {
        let w = match i.ended_at {
            None => 0.0,
            Some(end) if now <= end => 0.0,
            Some(_) if ramp.is_zero() => 1.0,
            Some(end) => (now.duration_since(end).as_secs_f64() / ramp.as_secs_f64()).min(1.0),
        };
        if w < weight {
            weight = w;
            cause = Some(i.id.clone());
        }
    }
    (weight, cause)
}

/// Target weights for one reservation. A Multi-region reservation's failed share moves to
/// its failover target. A Regional reservation keeps only what its healthy regions hold.
pub fn reservation_targets(
    config: &ControlPlaneConfig,
    pt: &ProvisionedThroughput,
    weight_of: impl Fn(&str) -> f64,
) -> Vec<Target> {
    let mut w: Vec<(String, f64)> = pt
        .regions
        .iter()
        .map(|r| (r.region.clone(), f64::from(r.cus) * weight_of(&r.region)))
        .collect();
    if pt.sku == Sku::MultiRegion {
        for (from, to) in failover_targets(config, &pt.regions) {
            let cus = pt
                .regions
                .iter()
                .find(|r| r.region == from)
                .map_or(0, |r| r.cus);
            let moved = f64::from(cus) * (1.0 - weight_of(&from));
            if weight_of(&to) > 0.0 {
                if let Some(t) = w.iter_mut().find(|(r, _)| *r == to) {
                    t.1 += moved;
                }
            }
        }
    }
    let total: f64 = w.iter().map(|(_, x)| x).sum();
    if total <= 0.0 {
        return vec![];
    }
    w.into_iter()
        .filter(|(_, x)| *x > 0.0)
        .map(|(region, x)| Target {
            endpoint: config.server.endpoint_template.replace("{region}", &region),
            region,
            weight: (x / total * 1e4).round() / 1e4,
        })
        .collect()
}
