//! Active/standby leadership for the Quota Coordinator (ADR-027).
//!
//! Two or more coordinator replicas compete for one lease record (a Kubernetes
//! `coordination.k8s.io/v1` Lease in production, [`MemoryLease`] in tests). Only the holder
//! answers renewals. The rules follow client-go's leader election, and add one for leases
//! gateways still hold:
//!
//! - **Serving.** The leader serves until `renew_deadline` after its last successful
//!   renewal, measured from *before* the write.
//! - **Taking over a dead leader.** A candidate takes an unchanged record only after it has
//!   watched it for `lease_duration`, by its own clock. The old leader stopped serving at
//!   most `renew_deadline` after its last renewal, so its last grants were issued at least
//!   `lease_duration − renew_deadline` before the takeover. Config validation keeps that gap
//!   longer than a grant lives, so no old grant is outstanding and the new leader starts
//!   cold.
//! - **Taking over a released lease.** A leader that shuts down stops serving, then clears
//!   the holder. A candidate may take it at once, but its grants from the old leader are
//!   still live, so the new leader *warms up* for one grant hold: it never grants a gateway
//!   more than the lease the gateway reports holding ([`crate::Coordinator::begin_term`]).
//!
//! Every write is a compare-and-swap on the record's version, so two candidates can't both
//! take it.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A lease record as the election sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseRecord {
    /// `None` when released.
    pub holder: Option<String>,
    /// Changes on every write (Kubernetes `resourceVersion`).
    pub version: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ElectionError {
    #[error("lease backend: {0}")]
    Backend(String),
}

/// Where the lease record lives.
pub trait LeaseBackend: Send + Sync + 'static {
    fn get(&self) -> impl Future<Output = Result<Option<LeaseRecord>, ElectionError>> + Send;
    /// Write `holder`. With `expected: None` the record must not exist yet; otherwise its
    /// version must still be `expected`. Returns false if another writer got there first.
    fn put(
        &self,
        holder: Option<&str>,
        expected: Option<&str>,
    ) -> impl Future<Output = Result<bool, ElectionError>> + Send;
}

/// Holder and version.
type Record = (Option<String>, u64);

/// An in-process lease, shared by clones. For tests and single-machine runs.
#[derive(Debug, Clone, Default)]
pub struct MemoryLease {
    record: Arc<Mutex<Option<Record>>>,
}

impl LeaseBackend for MemoryLease {
    async fn get(&self) -> Result<Option<LeaseRecord>, ElectionError> {
        let r = self.record.lock().unwrap_or_else(|e| e.into_inner());
        Ok(r.as_ref().map(|(holder, v)| LeaseRecord {
            holder: holder.clone(),
            version: v.to_string(),
        }))
    }

    async fn put(
        &self,
        holder: Option<&str>,
        expected: Option<&str>,
    ) -> Result<bool, ElectionError> {
        let mut r = self.record.lock().unwrap_or_else(|e| e.into_inner());
        let next = match (&*r, expected) {
            (None, None) => 1,
            (Some((_, v)), Some(e)) if v.to_string() == e => v + 1,
            _ => return Ok(false),
        };
        *r = Some((holder.map(str::to_string), next));
        Ok(true)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectionConfig {
    /// This replica's name in the lease (the pod name in Kubernetes).
    pub identity: String,
    /// How long a candidate watches an unchanged record before taking it over.
    pub lease_duration: Duration,
    /// How long the leader serves after its last successful renewal.
    pub renew_deadline: Duration,
    /// How often to renew or try to acquire.
    pub retry_period: Duration,
}

impl ElectionConfig {
    /// The timing rules that make takeover safe for grants held for `grant_hold`.
    pub fn validate(&self, grant_hold: Duration) -> Result<(), String> {
        if self.identity.is_empty() {
            return Err("election identity must not be empty".into());
        }
        if self.retry_period.is_zero() || self.retry_period >= self.renew_deadline {
            return Err("retry_period must be positive and shorter than renew_deadline".into());
        }
        if self.renew_deadline >= self.lease_duration {
            return Err("renew_deadline must be shorter than lease_duration".into());
        }
        if self.lease_duration - self.renew_deadline < grant_hold {
            return Err(format!(
                "lease_duration − renew_deadline must be at least the grant hold ({} ms, 1.5 × lease_ttl_ms), so a dead leader's grants expire before a standby takes over",
                grant_hold.as_millis()
            ));
        }
        Ok(())
    }
}

/// What a [`Elector::tick`] changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change {
    None,
    /// This replica took the lease from another holder or created it. With `warm_up`, the
    /// previous leader released it and its grants may still be live.
    Acquired {
        warm_up: bool,
    },
    /// This replica had stopped serving but still held the record, and renewed it. Nothing
    /// else can have granted in between, so its state is still valid.
    Resumed,
    /// This replica stopped serving: it couldn't renew within the deadline.
    Lost,
}

pub struct Elector<B> {
    backend: B,
    config: ElectionConfig,
    /// The other holder's record version, and when this replica first saw it.
    observed: Option<(String, Instant)>,
    /// Start of the last successful renewal.
    last_renew: Option<Instant>,
}

impl<B: LeaseBackend> Elector<B> {
    pub fn new(backend: B, config: ElectionConfig) -> Self {
        Self {
            backend,
            config,
            observed: None,
            last_renew: None,
        }
    }

    pub fn config(&self) -> &ElectionConfig {
        &self.config
    }

    /// Serve until this instant, if leading.
    pub fn leader_until(&self) -> Option<Instant> {
        self.last_renew.map(|t| t + self.config.renew_deadline)
    }

    pub fn is_leader(&self, now: Instant) -> bool {
        self.leader_until().is_some_and(|u| now < u)
    }

    /// Renew, or try to acquire. `now` must be taken before the call, so a slow write
    /// shortens leadership rather than extending it.
    pub async fn tick(&mut self, now: Instant) -> Result<Change, ElectionError> {
        let was = self.is_leader(now);
        let result = self.step(now).await;
        let change = match result {
            Ok(c) => c,
            Err(e) => {
                if was && !self.is_leader(now) {
                    return Ok(Change::Lost);
                }
                return Err(e);
            }
        };
        if change == Change::None && was && !self.is_leader(now) {
            return Ok(Change::Lost);
        }
        Ok(change)
    }

    async fn step(&mut self, now: Instant) -> Result<Change, ElectionError> {
        let me = self.config.identity.clone();
        let was = self.is_leader(now);
        let Some(rec) = self.backend.get().await? else {
            // No record yet: nobody has led, so nobody holds grants.
            if self.backend.put(Some(&me), None).await? {
                self.acquired(now);
                return Ok(Change::Acquired { warm_up: false });
            }
            return Ok(Change::None);
        };
        match rec.holder.as_deref() {
            Some(h) if h == me => {
                if self.backend.put(Some(&me), Some(&rec.version)).await? {
                    self.last_renew = Some(now);
                    if !was {
                        return Ok(Change::Resumed);
                    }
                }
                Ok(Change::None)
            }
            None => {
                // Released: its grants may still be live, so warm up.
                if self.backend.put(Some(&me), Some(&rec.version)).await? {
                    self.acquired(now);
                    return Ok(Change::Acquired { warm_up: true });
                }
                Ok(Change::None)
            }
            Some(_) => {
                // Someone else leads (or did). Lose any stale claim of our own.
                self.last_renew = None;
                match &self.observed {
                    Some((v, _)) if *v == rec.version => {}
                    _ => self.observed = Some((rec.version.clone(), now)),
                }
                let (_, since) = self.observed.as_ref().expect("just set");
                if now.saturating_duration_since(*since) >= self.config.lease_duration
                    && self.backend.put(Some(&me), Some(&rec.version)).await?
                {
                    self.acquired(now);
                    return Ok(Change::Acquired { warm_up: false });
                }
                if was {
                    return Ok(Change::Lost);
                }
                Ok(Change::None)
            }
        }
    }

    fn acquired(&mut self, now: Instant) {
        self.last_renew = Some(now);
        self.observed = None;
    }

    /// Stop serving, then clear the holder so a standby can take over at once.
    pub async fn release(&mut self) -> Result<(), ElectionError> {
        if self.last_renew.take().is_none() {
            return Ok(());
        }
        if let Some(rec) = self.backend.get().await? {
            if rec.holder.as_deref() == Some(self.config.identity.as_str()) {
                self.backend.put(None, Some(&rec.version)).await?;
            }
        }
        Ok(())
    }
}

/// Whether this replica may answer renewals now. Shared with the HTTP API.
#[derive(Debug)]
pub struct Leadership {
    /// `None`: always serve (a single coordinator without election).
    until: Mutex<Option<Option<Instant>>>,
}

impl Leadership {
    /// A single coordinator that always serves.
    pub fn always() -> Arc<Self> {
        Arc::new(Self {
            until: Mutex::new(None),
        })
    }

    /// Elected: serves only while [`Leadership::set`] says so.
    pub fn elected() -> Arc<Self> {
        Arc::new(Self {
            until: Mutex::new(Some(None)),
        })
    }

    pub fn set(&self, until: Option<Instant>) {
        let mut u = self.until.lock().unwrap_or_else(|e| e.into_inner());
        if u.is_some() {
            *u = Some(until);
        }
    }

    pub fn serving(&self, now: Instant) -> bool {
        match *self.until.lock().unwrap_or_else(|e| e.into_inner()) {
            None => true,
            Some(until) => until.is_some_and(|u| now < u),
        }
    }
}

/// Run the election until `shutdown` resolves, then release the lease. On acquiring,
/// starts a new term on the coordinator.
pub async fn run<B: LeaseBackend>(
    mut elector: Elector<B>,
    coordinator: Arc<crate::Coordinator>,
    leadership: Arc<Leadership>,
    shutdown: impl Future<Output = ()>,
) {
    let mut tick = tokio::time::interval(elector.config().retry_period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = &mut shutdown => break,
        }
        let now = Instant::now();
        match elector.tick(now).await {
            Ok(Change::Acquired { warm_up }) => {
                coordinator.begin_term(now, warm_up);
                tracing::info!(identity = %elector.config().identity, warm_up, "leading the quota coordinator");
            }
            Ok(Change::Resumed) => tracing::info!("resumed leading the quota coordinator"),
            Ok(Change::Lost) => tracing::warn!("lost quota coordinator leadership; standing by"),
            Ok(Change::None) => {}
            Err(e) => tracing::warn!(error = %e, "quota coordinator election failed"),
        }
        leadership.set(elector.leader_until());
    }
    leadership.set(None);
    if let Err(e) = elector.release().await {
        tracing::warn!(error = %e, "couldn't release the quota coordinator lease");
    }
}

#[cfg(feature = "kube")]
pub mod kube_lease {
    //! The lease as a Kubernetes `coordination.k8s.io/v1` Lease.

    use std::time::Duration;

    use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
    use kube::api::{Api, PostParams};

    use super::{ElectionError, LeaseBackend, LeaseRecord};

    pub struct KubeLease {
        api: Api<Lease>,
        name: String,
        lease_duration: Duration,
    }

    impl KubeLease {
        pub fn new(
            client: kube::Client,
            namespace: &str,
            name: &str,
            lease_duration: Duration,
        ) -> Self {
            Self {
                api: Api::namespaced(client, namespace),
                name: name.into(),
                lease_duration,
            }
        }
    }

    fn backend(e: kube::Error) -> ElectionError {
        ElectionError::Backend(e.to_string())
    }

    impl LeaseBackend for KubeLease {
        async fn get(&self) -> Result<Option<LeaseRecord>, ElectionError> {
            let lease = self.api.get_opt(&self.name).await.map_err(backend)?;
            Ok(lease.map(|l| LeaseRecord {
                holder: l
                    .spec
                    .and_then(|s| s.holder_identity)
                    .filter(|h| !h.is_empty()),
                version: l.metadata.resource_version.unwrap_or_default(),
            }))
        }

        async fn put(
            &self,
            holder: Option<&str>,
            expected: Option<&str>,
        ) -> Result<bool, ElectionError> {
            // renewTime is informational: candidates time the record by their own clocks.
            let lease = Lease {
                metadata: ObjectMeta {
                    name: Some(self.name.clone()),
                    resource_version: expected.map(str::to_string),
                    ..Default::default()
                },
                spec: Some(LeaseSpec {
                    holder_identity: holder.map(str::to_string),
                    lease_duration_seconds: Some(self.lease_duration.as_secs().max(1) as i32),
                    renew_time: Some(MicroTime(jiff::Timestamp::now())),
                    ..Default::default()
                }),
            };
            let pp = PostParams::default();
            let result = match expected {
                None => self.api.create(&pp, &lease).await.map(|_| ()),
                Some(_) => self.api.replace(&self.name, &pp, &lease).await.map(|_| ()),
            };
            match result {
                Ok(()) => Ok(true),
                Err(kube::Error::Api(s)) if s.code == 409 => Ok(false),
                Err(e) => Err(backend(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(id: &str) -> ElectionConfig {
        ElectionConfig {
            identity: id.into(),
            lease_duration: Duration::from_secs(5),
            renew_deadline: Duration::from_secs(3),
            retry_period: Duration::from_secs(1),
        }
    }

    fn pair() -> (Elector<MemoryLease>, Elector<MemoryLease>) {
        let lease = MemoryLease::default();
        (
            Elector::new(lease.clone(), config("a")),
            Elector::new(lease, config("b")),
        )
    }

    #[test]
    fn validation_keeps_takeover_after_grants_expire() {
        let hold = Duration::from_millis(1_500);
        assert!(config("a").validate(hold).is_ok());
        let tight = ElectionConfig {
            renew_deadline: Duration::from_millis(4_000),
            ..config("a")
        };
        assert!(tight.validate(hold).unwrap_err().contains("grant hold"));
        let slow = ElectionConfig {
            retry_period: Duration::from_secs(3),
            ..config("a")
        };
        assert!(slow.validate(hold).is_err());
    }

    #[tokio::test]
    async fn one_leader_and_takeover_after_the_lease_duration() {
        let (mut a, mut b) = pair();
        let t0 = Instant::now();
        let s = |n: u64| t0 + Duration::from_millis(n);
        assert_eq!(
            a.tick(s(0)).await.unwrap(),
            Change::Acquired { warm_up: false }
        );
        assert_eq!(b.tick(s(0)).await.unwrap(), Change::None);
        // a renews every second; b never takes over.
        for n in 1..=10 {
            a.tick(s(n * 1_000)).await.unwrap();
            assert_eq!(b.tick(s(n * 1_000 + 10)).await.unwrap(), Change::None);
            assert!(a.is_leader(s(n * 1_000 + 10)) && !b.is_leader(s(n * 1_000 + 10)));
        }
        // a dies after its renewal at 10 s. It would stop serving at 13 s...
        assert!(a.is_leader(s(12_999)) && !a.is_leader(s(13_000)));
        // ... and b, which last saw the record change at 10.01 s, takes over 5 s later.
        for n in 11..=14 {
            assert_eq!(b.tick(s(n * 1_000 + 10)).await.unwrap(), Change::None);
        }
        assert_eq!(
            b.tick(s(15_010)).await.unwrap(),
            Change::Acquired { warm_up: false }
        );
        assert!(b.is_leader(s(15_010)));
        // Never two at once: a stopped 2 s before b started.
        assert!(!a.is_leader(s(15_010)));
        // a comes back: it sees b's record and stands by.
        assert_eq!(a.tick(s(16_000)).await.unwrap(), Change::None);
        assert!(!a.is_leader(s(16_000)));
    }

    #[tokio::test]
    async fn released_lease_is_taken_at_once_with_warm_up() {
        let (mut a, mut b) = pair();
        let t0 = Instant::now();
        a.tick(t0).await.unwrap();
        b.tick(t0).await.unwrap();
        a.release().await.unwrap();
        assert!(!a.is_leader(t0), "stops serving before releasing");
        assert_eq!(
            b.tick(t0 + Duration::from_millis(100)).await.unwrap(),
            Change::Acquired { warm_up: true }
        );
    }

    #[tokio::test]
    async fn a_leader_that_cannot_renew_stops_serving_and_may_resume() {
        let (mut a, _) = pair();
        let t0 = Instant::now();
        a.tick(t0).await.unwrap();
        // Renewals fail (say, the API server is unreachable) until the deadline passes.
        assert!(a.is_leader(t0 + Duration::from_millis(2_999)));
        assert!(!a.is_leader(t0 + Duration::from_secs(3)));
        // Nobody else took the record, so a renews and resumes with its state intact.
        assert_eq!(
            a.tick(t0 + Duration::from_secs(4)).await.unwrap(),
            Change::Resumed
        );
    }

    #[tokio::test]
    async fn racing_candidates_cannot_both_win() {
        let (mut a, mut b) = pair();
        let t0 = Instant::now();
        let (x, y) = tokio::join!(a.tick(t0), b.tick(t0));
        let won = [x.unwrap(), y.unwrap()]
            .iter()
            .filter(|c| matches!(c, Change::Acquired { .. }))
            .count();
        assert_eq!(won, 1);
        assert_ne!(a.is_leader(t0), b.is_leader(t0));
    }

    #[test]
    fn leadership_gate() {
        let t = Instant::now();
        assert!(Leadership::always().serving(t));
        let l = Leadership::elected();
        assert!(!l.serving(t));
        l.set(Some(t + Duration::from_secs(1)));
        assert!(l.serving(t) && !l.serving(t + Duration::from_secs(1)));
    }
}
