//! Expected prefix-cache hits, for the admission estimate (docs/04 §3, ADR-030).
//!
//! Cached prefill costs a fraction of uncached prefill (`b` against `a` in the WU
//! formula). An agent resends its whole conversation every turn, and the engine has most
//! of it cached, so estimating every input token as uncached overstates the request's cost
//! several times over. Settlement refunds the difference, but admission has already queued,
//! throttled, or spilled the request on the inflated estimate.
//!
//! The gateway remembers which message prefixes it sent recently. Each prefix is a hash
//! over the scope (the reservation) and every message up to that point, so it matches only
//! the same conversation from the same reservation. A new request's longest remembered
//! prefix is *probably* cached. The expected cached tokens are that prefix's tokens times
//! a hit rate learned per model from the cached-token counts the engine reports, because
//! engines evict, cache only whole blocks, and may be behind another gateway replica.
//!
//! Keying by reservation means one tenant's traffic never changes another's estimate, even
//! when the engine shares a system prompt's cache between them.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasher, Hash, Hasher, RandomState};
use std::time::{Duration, Instant};

use serde::Serialize;

/// Weight of each new observation in the hit rate.
const ALPHA: f64 = 0.1;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PrefixCacheConfig {
    /// Prefixes remembered, across all reservations.
    pub capacity: usize,
    /// A prefix not sent for this long is assumed evicted.
    pub ttl: Duration,
    /// Hit rate assumed for a model before the engine has reported any cached tokens.
    pub initial_hit_rate: f64,
}

impl Default for PrefixCacheConfig {
    fn default() -> Self {
        Self {
            capacity: 200_000,
            ttl: Duration::from_secs(600),
            // Placeholder until calibration: conservative, since overestimating cache hits
            // under-prices the request.
            initial_hit_rate: 0.5,
        }
    }
}

/// What a request is expected to find cached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Prediction {
    /// Tokens of the longest remembered prefix.
    pub matched_tokens: u64,
    /// `matched_tokens` × the model's hit rate: what the estimate treats as cached.
    pub expected_cached: u64,
}

#[derive(Debug, Clone, Copy)]
struct HitRate {
    rate: f64,
    observations: u64,
    matched_tokens: u64,
    engine_cached_tokens: u64,
    unpredicted_cached_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ModelHitRate {
    pub model: String,
    /// Engine-reported cached tokens ÷ matched prefix tokens, as a moving average.
    pub hit_rate: f64,
    pub observations: u64,
    /// Totals since start: prefix tokens the gateway matched, and cached tokens the engine
    /// reported for those requests.
    pub matched_tokens: u64,
    pub engine_cached_tokens: u64,
    /// Cached tokens the engine reported where the gateway matched nothing, for example
    /// prefixes sent through another gateway replica.
    pub unpredicted_cached_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PrefixCacheStatus {
    pub prefixes: usize,
    pub capacity: usize,
    pub ttl_secs: u64,
    pub models: Vec<ModelHitRate>,
}

/// Builds prefix keys. Cheap to clone, so callers can hash outside the cache's lock.
#[derive(Clone)]
pub struct PrefixKeys(RandomState);

impl PrefixKeys {
    /// One hash per message: the scope and every message up to and including it.
    pub fn keys(&self, scope: &str, messages: &[(String, String)]) -> Vec<u64> {
        let mut h: DefaultHasher = self.0.build_hasher();
        scope.hash(&mut h);
        messages
            .iter()
            .map(|(role, content)| {
                role.hash(&mut h);
                content.hash(&mut h);
                h.finish()
            })
            .collect()
    }
}

pub struct PrefixCache {
    config: PrefixCacheConfig,
    /// Prefix hash → when it was last sent, and the generation of that write.
    seen: HashMap<u64, (Instant, u64)>,
    /// Writes in order, for eviction. Entries whose generation is stale are skipped.
    order: VecDeque<(u64, u64)>,
    generation: u64,
    /// Keys are random per process, so clients can't aim collisions at the index.
    hasher: PrefixKeys,
    rates: HashMap<String, HitRate>,
}

impl PrefixCache {
    pub fn new(config: PrefixCacheConfig) -> Self {
        Self {
            config,
            seen: HashMap::new(),
            order: VecDeque::new(),
            generation: 0,
            hasher: PrefixKeys(RandomState::new()),
            rates: HashMap::new(),
        }
    }

    /// The key builder this cache matches against.
    pub fn key_builder(&self) -> PrefixKeys {
        self.hasher.clone()
    }

    /// One hash per message: the scope and every message up to and including it.
    pub fn keys(&self, scope: &str, messages: &[(String, String)]) -> Vec<u64> {
        self.hasher.keys(scope, messages)
    }

    fn fresh(&self, key: u64, now: Instant) -> bool {
        self.seen
            .get(&key)
            .is_some_and(|(at, _)| now.saturating_duration_since(*at) < self.config.ttl)
    }

    fn rate(&self, model: &str) -> f64 {
        self.rates
            .get(model)
            .map_or(self.config.initial_hit_rate, |r| r.rate)
    }

    /// Expected cached tokens for a request with these prefix `keys` and per-message token
    /// counts.
    pub fn predict(
        &self,
        model: &str,
        keys: &[u64],
        per_message: &[u64],
        now: Instant,
    ) -> Prediction {
        let Some(last) = keys.iter().rposition(|k| self.fresh(*k, now)) else {
            return Prediction::default();
        };
        let total: u64 = per_message.iter().sum();
        let matched: u64 = per_message.iter().take(last + 1).sum();
        // The engine always computes at least the last prompt token.
        let matched = matched.min(total.saturating_sub(1));
        Prediction {
            matched_tokens: matched,
            expected_cached: (matched as f64 * self.rate(model)).floor() as u64,
        }
    }

    /// The engine accepted a request with these prefixes: it will hold them in its cache.
    pub fn record(&mut self, keys: &[u64], now: Instant) {
        if self.config.capacity == 0 {
            return;
        }
        for &k in keys {
            self.generation += 1;
            self.seen.insert(k, (now, self.generation));
            self.order.push_back((k, self.generation));
        }
        // Evict the oldest writes, and drop stale ones so `order` stays bounded.
        while self.seen.len() > self.config.capacity || self.order.len() > 2 * self.config.capacity
        {
            let Some((k, generation)) = self.order.pop_front() else {
                break;
            };
            if self.seen.get(&k).is_some_and(|(_, g)| *g == generation) {
                self.seen.remove(&k);
            }
        }
    }

    /// Learn from a settled request: what the gateway matched, and what the engine
    /// reported as cached.
    pub fn observe(&mut self, model: &str, matched_tokens: u64, engine_cached: u64) {
        let initial = self.config.initial_hit_rate;
        let r = self.rates.entry(model.to_string()).or_insert(HitRate {
            rate: initial,
            observations: 0,
            matched_tokens: 0,
            engine_cached_tokens: 0,
            unpredicted_cached_tokens: 0,
        });
        if matched_tokens == 0 {
            r.unpredicted_cached_tokens += engine_cached;
            return;
        }
        let seen = (engine_cached as f64 / matched_tokens as f64).min(1.0);
        r.rate = if r.observations == 0 {
            seen
        } else {
            r.rate + ALPHA * (seen - r.rate)
        };
        r.observations += 1;
        r.matched_tokens += matched_tokens;
        r.engine_cached_tokens += engine_cached;
    }

    pub fn status(&self) -> PrefixCacheStatus {
        let mut models: Vec<ModelHitRate> = self
            .rates
            .iter()
            .map(|(m, r)| ModelHitRate {
                model: m.clone(),
                hit_rate: r.rate,
                observations: r.observations,
                matched_tokens: r.matched_tokens,
                engine_cached_tokens: r.engine_cached_tokens,
                unpredicted_cached_tokens: r.unpredicted_cached_tokens,
            })
            .collect();
        models.sort_by(|a, b| a.model.cmp(&b.model));
        PrefixCacheStatus {
            prefixes: self.seen.len(),
            capacity: self.config.capacity,
            ttl_secs: self.config.ttl.as_secs(),
            models,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msgs(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter()
            .map(|(r, c)| (r.to_string(), c.to_string()))
            .collect()
    }

    #[test]
    fn a_conversation_matches_its_previous_turn() {
        let mut c = PrefixCache::new(PrefixCacheConfig::default());
        let t = Instant::now();
        let turn1 = msgs(&[("system", "be brief"), ("user", "hi")]);
        let k1 = c.keys("res-a", &turn1);
        assert_eq!(c.predict("m", &k1, &[1_000, 50], t), Prediction::default());
        c.record(&k1, t);

        let mut turn2 = turn1.clone();
        turn2.extend(msgs(&[("assistant", "hello"), ("user", "more")]));
        let k2 = c.keys("res-a", &turn2);
        assert_eq!(&k2[..2], &k1[..], "shared prefix, same keys");
        let p = c.predict("m", &k2, &[1_000, 50, 20, 30], t);
        // The first two messages were sent; half is expected cached before learning.
        assert_eq!(p.matched_tokens, 1_050);
        assert_eq!(p.expected_cached, 525);
    }

    #[test]
    fn reservations_and_edits_do_not_match() {
        let mut c = PrefixCache::new(PrefixCacheConfig::default());
        let t = Instant::now();
        let m = msgs(&[("system", "shared prompt"), ("user", "hi")]);
        c.record(&c.keys("res-a", &m), t);
        assert_eq!(
            c.predict("m", &c.keys("res-b", &m), &[100, 10], t),
            Prediction::default(),
            "another reservation's history doesn't count"
        );
        // An edited first message changes every later prefix.
        let edited = msgs(&[("system", "shared prompt!"), ("user", "hi")]);
        assert_eq!(
            c.predict("m", &c.keys("res-a", &edited), &[100, 10], t)
                .matched_tokens,
            0
        );
        // An identical resend matches all but the last token.
        assert_eq!(
            c.predict("m", &c.keys("res-a", &m), &[100, 10], t)
                .matched_tokens,
            109
        );
    }

    #[test]
    fn prefixes_expire_and_are_bounded() {
        let mut c = PrefixCache::new(PrefixCacheConfig {
            capacity: 3,
            ttl: Duration::from_secs(60),
            initial_hit_rate: 1.0,
        });
        let t = Instant::now();
        let a = c.keys("r", &msgs(&[("user", "a"), ("user", "b")]));
        c.record(&a, t);
        assert!(
            c.predict("m", &a, &[5, 5], t + Duration::from_secs(59))
                .matched_tokens
                > 0
        );
        assert_eq!(
            c.predict("m", &a, &[5, 5], t + Duration::from_secs(60)),
            Prediction::default(),
            "expired"
        );
        // Refreshing and adding more stays within capacity, evicting the oldest.
        c.record(&a, t);
        let b = c.keys("r", &msgs(&[("user", "x"), ("user", "y")]));
        c.record(&b, t);
        assert_eq!(c.status().prefixes, 3);
        assert!(c.order.len() <= 6);
        assert!(!c.fresh(a[0], t), "oldest evicted");
        assert!(c.fresh(b[1], t));
    }

    #[test]
    fn hit_rate_is_learned_per_model() {
        let mut c = PrefixCache::new(PrefixCacheConfig::default());
        // The engine caches whole blocks: about 90% of what the gateway matches.
        c.observe("m", 1_000, 900);
        assert!(
            (c.rate("m") - 0.9).abs() < 1e-9,
            "first observation replaces the guess"
        );
        for _ in 0..50 {
            c.observe("m", 1_000, 800);
        }
        assert!((c.rate("m") - 0.8).abs() < 0.01);
        assert_eq!(c.rate("other"), 0.5, "other models keep the initial guess");
        // Cached tokens the gateway didn't predict are counted, not learned.
        c.observe("m", 0, 300);
        let s = c.status();
        assert_eq!(s.models[0].unpredicted_cached_tokens, 300);
        assert_eq!(s.models[0].observations, 51);
        // Never more than all of it.
        c.observe("n", 100, 150);
        assert_eq!(c.rate("n"), 1.0);
    }
}
