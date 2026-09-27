//! Which gateway replica serves a request (docs/04 §6, ADR-040).
//!
//! Replicas share entitlements through the Quota Coordinator, but each keeps its own token
//! counts (ADR-028) and prefix history (ADR-030). An agent whose turns land on different
//! replicas is counted from scratch each time, and gets no expected cache hits. And a small
//! reservation spread over every replica has its lease split into slivers.
//!
//! So, with `[affinity]`, requests have an owner among the replicas, chosen by rendezvous
//! hashing (the replica with the highest hash of key and URL), which moves only the keys of
//! a replica that joins or leaves:
//! - **Sessions.** A request with `x-pt-session-id` belongs to the session's owner.
//! - **Home gateways.** A request for a reservation of at most `home_below_cus` CUs belongs
//!   to one of the deployment's `home_gateways` highest-ranked replicas, spread by
//!   request.
//! - Other requests stay where they landed.
//!
//! A replica that isn't the owner forwards the request to it once, marked
//! `x-pt-forwarded`, which the owner always serves itself. If the owner can't be reached,
//! the request is served where it is and the owner is skipped for a while.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Deserialize;

/// Marks a forwarded request: the receiver serves it, whoever owns it.
pub const FORWARDED_HEADER: &str = "x-pt-forwarded";

/// How long an unreachable peer is skipped.
const DOWN_FOR: Duration = Duration::from_secs(10);

/// `[affinity]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AffinityConfig {
    /// This replica's URL, as it appears in `peers`.
    pub self_url: String,
    /// Every replica in the region, including this one, for example the pods of a
    /// StatefulSet behind a headless Service.
    pub peers: Vec<String>,
    /// Reservations of at most this many CUs are served by home gateways. 0 turns it off.
    #[serde(default)]
    pub home_below_cus: u32,
    /// How many home gateways a small reservation's deployment has.
    #[serde(default = "default_home_gateways")]
    pub home_gateways: usize,
}

fn default_home_gateways() -> usize {
    2
}

/// Where a request should be served.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Route {
    Here,
    Forward(String),
}

pub struct Affinity {
    self_url: String,
    peers: Vec<String>,
    home_below_cus: u32,
    home_gateways: usize,
    pub http: reqwest::Client,
    down: Mutex<HashMap<String, Instant>>,
}

impl Affinity {
    pub fn new(config: &AffinityConfig) -> Result<Self, String> {
        let norm = |u: &str| u.trim_end_matches('/').to_string();
        let self_url = norm(&config.self_url);
        let mut peers: Vec<String> = config.peers.iter().map(|p| norm(p)).collect();
        peers.sort();
        peers.dedup();
        if !peers.contains(&self_url) {
            return Err(format!("affinity.peers must include self_url {self_url}"));
        }
        if config.home_gateways == 0 {
            return Err("affinity.home_gateways must be at least 1".into());
        }
        Ok(Self {
            self_url,
            peers,
            home_below_cus: config.home_below_cus,
            home_gateways: config.home_gateways,
            http: reqwest::Client::new(),
            down: Mutex::new(HashMap::new()),
        })
    }

    /// Peers by rendezvous rank for `key`, highest first, skipping ones marked down.
    fn ranked(&self, key: &str, now: Instant) -> Vec<&String> {
        let down = self.down.lock().unwrap_or_else(|e| e.into_inner());
        let mut ranked: Vec<(u64, &String)> = self
            .peers
            .iter()
            .filter(|p| **p == self.self_url || down.get(*p).is_none_or(|until| now >= *until))
            .map(|p| (fnv1a(&[key.as_bytes(), b"\0", p.as_bytes()]), p))
            .collect();
        ranked.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(b.1)));
        ranked.into_iter().map(|(_, p)| p).collect()
    }

    /// Where to serve a request. `spread` picks among home gateways (a request id works).
    pub fn route(
        &self,
        session: Option<&str>,
        deployment: &str,
        reservation_cus: u32,
        spread: u64,
        now: Instant,
    ) -> Route {
        let owner = if let Some(s) = session {
            self.ranked(&format!("session:{s}"), now).first().copied()
        } else if reservation_cus > 0 && reservation_cus <= self.home_below_cus {
            let homes = self.ranked(&format!("home:{deployment}"), now);
            let homes = &homes[..homes.len().min(self.home_gateways)];
            if homes.contains(&&self.self_url) {
                return Route::Here;
            }
            homes.get(spread as usize % homes.len().max(1)).copied()
        } else {
            None
        };
        match owner {
            Some(o) if *o != self.self_url => Route::Forward(o.clone()),
            _ => Route::Here,
        }
    }

    /// The owner couldn't be reached: skip it for a while.
    pub fn mark_down(&self, peer: &str, now: Instant) {
        self.down
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(peer.to_string(), now + DOWN_FOR);
    }
}

/// FNV-1a: the same on every replica, unlike the standard library's random hasher.
fn fnv1a(parts: &[&[u8]]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for part in parts {
        for b in *part {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    fn affinity(me: &str, peers: &[&str], home_below: u32, homes: usize) -> Affinity {
        Affinity::new(&AffinityConfig {
            self_url: me.into(),
            peers: peers.iter().map(|p| p.to_string()).collect(),
            home_below_cus: home_below,
            home_gateways: homes,
        })
        .unwrap()
    }

    const PEERS: [&str; 3] = ["http://a", "http://b", "http://c"];

    #[test]
    fn every_replica_agrees_on_a_sessions_owner() {
        let t = Instant::now();
        let replicas: Vec<_> = PEERS.iter().map(|p| affinity(p, &PEERS, 0, 2)).collect();
        for s in ["s1", "s2", "agent-42", "x"] {
            let owners: Vec<String> = replicas
                .iter()
                .zip(PEERS)
                .map(|(r, me)| match r.route(Some(s), "dep", 100, 0, t) {
                    Route::Here => me.to_string(),
                    Route::Forward(o) => o,
                })
                .collect();
            assert!(owners.windows(2).all(|w| w[0] == w[1]), "{s}: {owners:?}");
        }
        // Big reservations without a session stay where they land.
        assert_eq!(replicas[0].route(None, "dep", 100, 0, t), Route::Here);
    }

    #[test]
    fn small_reservations_go_to_their_home_gateways() {
        let t = Instant::now();
        let a = affinity("http://a", &PEERS, 4, 1);
        let b = affinity("http://b", &PEERS, 4, 1);
        let c = affinity("http://c", &PEERS, 4, 1);
        // Exactly one replica serves a 2-CU reservation's deployment itself.
        let here = [&a, &b, &c]
            .iter()
            .filter(|r| r.route(None, "dep-small", 2, 7, t) == Route::Here)
            .count();
        assert_eq!(here, 1);
        // Above the threshold: served where it lands.
        assert_eq!(a.route(None, "dep-small", 5, 7, t), Route::Here);
        // Two homes: requests spread over both.
        let two = affinity("http://a", &PEERS, 4, 2);
        let homes: std::collections::HashSet<Route> =
            (0..8).map(|n| two.route(None, "dep-x", 1, n, t)).collect();
        assert!(homes.len() <= 2);
    }

    #[test]
    fn an_unreachable_owner_is_skipped_for_a_while() {
        let t = Instant::now();
        let a = affinity("http://a", &PEERS, 0, 2);
        // Find a session b or c owns.
        let (s, owner) = (0..100)
            .map(|n| format!("s{n}"))
            .find_map(|s| match a.route(Some(&s), "d", 1, 0, t) {
                Route::Forward(o) => Some((s, o)),
                Route::Here => None,
            })
            .unwrap();
        a.mark_down(&owner, t);
        assert_ne!(
            a.route(Some(&s), "d", 1, 0, t),
            Route::Forward(owner.clone())
        );
        assert_eq!(
            a.route(Some(&s), "d", 1, 0, t + DOWN_FOR),
            Route::Forward(owner)
        );
    }

    #[test]
    fn self_must_be_a_peer() {
        assert!(Affinity::new(&AffinityConfig {
            self_url: "http://z".into(),
            peers: vec!["http://a".into()],
            home_below_cus: 0,
            home_gateways: 2,
        })
        .is_err());
    }
}
