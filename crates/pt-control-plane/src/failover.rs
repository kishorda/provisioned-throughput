//! Automatic region-failure handling (docs/07 §4, ADR-014).
//!
//! - **Headroom.** A Multi-region reservation holds, in each region's failover target,
//!   enough extra capacity to absorb that region's share. One region fails at a time, so a
//!   target holds the largest share that fails over to it, not the sum.
//! - **Detection.** Gateways heartbeat every few seconds. A region is down when none of its
//!   gateways has reported serving for `heartbeat_timeout_seconds`. Heartbeats are stored
//!   in the database, so every control-plane instance judges health the same (ADR-023).
//! - **Steering.** DNS weights per region and per reservation, from incidents rather than
//!   raw health, so operator-declared incidents drain regions too.

use std::collections::BTreeMap;

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

/// What the control plane remembers about one gateway, from its heartbeats. Stored in the
/// database, so every control-plane instance sees every gateway (ADR-023).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct GatewayHeartbeat {
    pub region: String,
    pub gateway_id: String,
    pub last_seen: Timestamp,
    pub serving: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
    /// Last heartbeat that reported serving.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_serving_at: Option<Timestamp>,
    /// Start of this gateway's current unbroken run of serving heartbeats.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serving_since: Option<Timestamp>,
}

/// Heartbeat rows are kept this long after a gateway's last heartbeat, so a long-dead
/// region still reads as down rather than unknown.
pub const HEARTBEAT_RETENTION: SignedDuration = SignedDuration::from_hours(24);

/// A gateway's record after heartbeat `hb` at `now`. A serving run is unbroken while
/// serving heartbeats are at most `timeout` apart.
pub fn next_heartbeat(
    prev: Option<&GatewayHeartbeat>,
    region: &str,
    hb: &Heartbeat,
    now: Timestamp,
    timeout: SignedDuration,
) -> GatewayHeartbeat {
    let last_serving = prev.and_then(|p| p.last_serving_at);
    let (last_serving_at, serving_since) = if hb.serving {
        let unbroken = last_serving.is_some_and(|t| now.duration_since(t) <= timeout);
        let since = if unbroken {
            prev.and_then(|p| p.serving_since).unwrap_or(now)
        } else {
            now
        };
        (Some(now), Some(since))
    } else {
        (last_serving, prev.and_then(|p| p.serving_since))
    };
    GatewayHeartbeat {
        region: region.to_string(),
        gateway_id: hb.gateway_id.clone(),
        last_seen: now,
        serving: hb.serving,
        key_id: hb.key_id.clone(),
        last_serving_at,
        serving_since,
    }
}

/// A region's health from its gateways' records.
///
/// - **Serving** if some gateway reported serving within `timeout`. The region has been
///   serving continuously at least since the earliest `serving_since` among those gateways.
/// - **Down** if gateways have reported, but none has been serving within `timeout`.
/// - **Unknown** if no gateway has ever reported (for example, before the first heartbeat).
pub fn region_status(
    region: &str,
    gateways: &[GatewayHeartbeat],
    now: Timestamp,
    timeout: SignedDuration,
) -> RegionStatus {
    let mine: Vec<&GatewayHeartbeat> = gateways.iter().filter(|g| g.region == region).collect();
    let fresh = |t: Timestamp| now.duration_since(t) <= timeout;
    let recent: Vec<&&GatewayHeartbeat> = mine.iter().filter(|g| fresh(g.last_seen)).collect();
    let mut snapshot_key_ids = BTreeMap::new();
    for g in &recent {
        *snapshot_key_ids
            .entry(g.key_id.clone().unwrap_or_else(|| "unknown".into()))
            .or_default() += 1;
    }
    let serving_now: Vec<&&GatewayHeartbeat> = mine
        .iter()
        .filter(|g| g.last_serving_at.is_some_and(fresh))
        .collect();
    let health = if mine.is_empty() {
        Health::Unknown
    } else if serving_now.is_empty() {
        Health::Down
    } else {
        Health::Serving
    };
    RegionStatus {
        region: region.into(),
        health,
        gateways: recent.len(),
        serving_gateways: recent.iter().filter(|g| g.serving).count(),
        last_serving_at: mine.iter().filter_map(|g| g.last_serving_at).max(),
        serving_since: serving_now.iter().filter_map(|g| g.serving_since).min(),
        snapshot_key_ids,
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
