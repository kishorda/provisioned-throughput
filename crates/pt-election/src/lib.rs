//! Active/standby leader election on one lease record (ADR-027, ADR-029).
//!
//! Replicas compete for a lease record: a Kubernetes `coordination.k8s.io/v1` Lease in
//! production ([`kube_lease::KubeLease`], feature `kube`), [`MemoryLease`] in tests. Used by
//! the Quota Coordinator and the Regional Capacity Controller. The rules follow client-go's
//! leader election:
//!
//! - **Serving.** The leader acts until `renew_deadline` after its last successful
//!   renewal, measured from *before* the write.
//! - **Taking over a dead leader.** A candidate takes an unchanged record only after it has
//!   watched it for `lease_duration`, by its own clock. The old leader stopped acting at
//!   most `renew_deadline` after its last renewal, so there's a gap of at least
//!   `lease_duration − renew_deadline` with no leader. [`ElectionConfig::validate`] makes
//!   the gap at least as long as the caller needs, for example longer than the Quota
//!   Coordinator's grants live.
//! - **Taking over a released lease.** A leader that shuts down stops acting, then clears
//!   the holder, and a candidate may take it at once ([`Change::Acquired`] with
//!   `warm_up`). Anything the old leader handed out is still live, so the caller decides
//!   whether to warm up: the Quota Coordinator does.
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
    /// Check the timings. A dead leader is replaced at least `min_gap` after it stopped
    /// acting: for the Quota Coordinator, how long its grants are held.
    pub fn validate(&self, min_gap: Duration) -> Result<(), String> {
        if self.identity.is_empty() {
            return Err("election identity must not be empty".into());
        }
        if self.retry_period.is_zero() || self.retry_period >= self.renew_deadline {
            return Err("retry_period must be positive and shorter than renew_deadline".into());
        }
        if self.renew_deadline >= self.lease_duration {
            return Err("renew_deadline must be shorter than lease_duration".into());
        }
        if self.lease_duration - self.renew_deadline < min_gap {
            return Err(format!(
                "lease_duration − renew_deadline must be at least the grant hold ({} ms, 1.5 × lease_ttl_ms), so a dead leader's grants expire before a standby takes over",
                min_gap.as_millis()
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

/// Run the election until `shutdown` resolves, then release the lease. `on_change` sees
/// every change, with the `now` it was measured from; `leadership` follows the elector.
pub async fn run<B: LeaseBackend>(
    mut elector: Elector<B>,
    leadership: Arc<Leadership>,
    mut on_change: impl FnMut(Change, Instant),
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
            Ok(Change::None) => {}
            Ok(change) => on_change(change, now),
            Err(e) => tracing::warn!(error = %e, "leader election failed"),
        }
        leadership.set(elector.leader_until());
    }
    leadership.set(None);
    if let Err(e) = elector.release().await {
        tracing::warn!(error = %e, "couldn't release the lease");
    }
}

/// Resolves on Ctrl-C, or SIGTERM from Kubernetes, so a leader can release its lease.
pub async fn terminated() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// How [`lead`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ended {
    /// `work` returned. The lease was released.
    Finished,
    /// Leadership lapsed while `work` ran: `work` was dropped. The caller should exit, so
    /// it restarts as a standby with fresh state.
    Lost,
    /// `shutdown` resolved. `work` was dropped (if it had started) and the lease released.
    Shutdown,
}

/// Wait to lead, then run `work` while renewing the lease. `work` is dropped the moment
/// leadership lapses (`renew_deadline` after the last renewal), before any standby can
/// take over.
pub async fn lead<B: LeaseBackend, F: Future<Output = ()>>(
    mut elector: Elector<B>,
    shutdown: impl Future<Output = ()>,
    work: impl FnOnce() -> F,
) -> Ended {
    let mut tick = tokio::time::interval(elector.config().retry_period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tokio::pin!(shutdown);
    let id = elector.config().identity.clone();

    // Stand by until this replica holds the lease.
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = &mut shutdown => return Ended::Shutdown,
        }
        let now = Instant::now();
        match elector.tick(now).await {
            Ok(_) if elector.is_leader(now) => break,
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "leader election failed"),
        }
    }
    tracing::info!(identity = %id, "leading");

    let work = work();
    tokio::pin!(work);
    let ended = loop {
        let until = elector.leader_until().unwrap_or_else(Instant::now);
        tokio::select! {
            _ = &mut work => break Ended::Finished,
            _ = &mut shutdown => break Ended::Shutdown,
            _ = tick.tick() => {
                if let Err(e) = elector.tick(Instant::now()).await {
                    tracing::warn!(error = %e, "couldn't renew the lease");
                }
            }
            _ = tokio::time::sleep_until(until.into()) => {}
        }
        if !elector.is_leader(Instant::now()) {
            tracing::warn!(identity = %id, "lost leadership");
            return Ended::Lost;
        }
    };
    if let Err(e) = elector.release().await {
        tracing::warn!(error = %e, "couldn't release the lease");
    }
    ended
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
        /// Connect with the in-cluster service account or the current kubeconfig.
        pub async fn connect(
            namespace: &str,
            name: &str,
            lease_duration: Duration,
        ) -> Result<Self, ElectionError> {
            let client = kube::Client::try_default().await.map_err(backend)?;
            Ok(Self::new(client, namespace, name, lease_duration))
        }

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

    /// A lease this replica can be cut off from, like a partition from the API server.
    #[derive(Clone)]
    struct Partitioned {
        inner: MemoryLease,
        cut: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Partitioned {
        fn check(&self) -> Result<(), ElectionError> {
            if self.cut.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(ElectionError::Backend("unreachable".into()));
            }
            Ok(())
        }
    }

    impl LeaseBackend for Partitioned {
        async fn get(&self) -> Result<Option<LeaseRecord>, ElectionError> {
            self.check()?;
            self.inner.get().await
        }
        async fn put(
            &self,
            holder: Option<&str>,
            expected: Option<&str>,
        ) -> Result<bool, ElectionError> {
            self.check()?;
            self.inner.put(holder, expected).await
        }
    }

    fn fast(id: &str) -> ElectionConfig {
        ElectionConfig {
            identity: id.into(),
            lease_duration: Duration::from_millis(400),
            renew_deadline: Duration::from_millis(200),
            retry_period: Duration::from_millis(40),
        }
    }

    /// Which replica worked, when it started, and when it stopped.
    type Span = (&'static str, Instant, Option<Instant>);

    /// Records when each replica's work started and stopped.
    #[derive(Clone, Default)]
    struct Spans(Arc<Mutex<Vec<Span>>>);

    impl Spans {
        async fn work(self, id: &'static str, run_for: Option<Duration>) {
            let i = {
                let mut v = self.0.lock().unwrap();
                v.push((id, Instant::now(), None));
                v.len() - 1
            };
            struct Stop(Spans, usize);
            impl Drop for Stop {
                fn drop(&mut self) {
                    self.0 .0.lock().unwrap()[self.1].2 = Some(Instant::now());
                }
            }
            let _stop = Stop(self.clone(), i);
            match run_for {
                Some(d) => tokio::time::sleep(d).await,
                None => std::future::pending().await,
            }
        }

        fn get(&self) -> Vec<Span> {
            self.0.lock().unwrap().clone()
        }
    }

    #[tokio::test]
    async fn lead_runs_work_only_while_leading() {
        let lease = MemoryLease::default();
        let cut = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let a_lease = Partitioned {
            inner: lease.clone(),
            cut: cut.clone(),
        };
        let spans = Spans::default();
        let a = tokio::spawn({
            let spans = spans.clone();
            lead(
                Elector::new(a_lease, fast("a")),
                std::future::pending(),
                move || spans.work("a", None),
            )
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let b = tokio::spawn({
            let spans = spans.clone();
            lead(
                Elector::new(lease, fast("b")),
                std::future::pending(),
                move || spans.work("b", Some(Duration::from_millis(50))),
            )
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(spans.get().len(), 1, "only a works");

        // a is cut off from the lease: its work stops at the renew deadline, and b starts
        // only after the full lease duration.
        cut.store(true, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(a.await.unwrap(), Ended::Lost);
        assert_eq!(b.await.unwrap(), Ended::Finished);
        let s = spans.get();
        let (a_stop, b_start) = (s[0].2.unwrap(), s[1].1);
        assert_eq!((s[0].0, s[1].0), ("a", "b"));
        assert!(
            b_start > a_stop + Duration::from_millis(150),
            "no overlap, with a margin: a stopped {:?} before b started",
            b_start - a_stop
        );
    }

    #[tokio::test]
    async fn finishing_or_shutting_down_releases_at_once() {
        let lease = MemoryLease::default();
        let t0 = Instant::now();
        let ended = lead(
            Elector::new(lease.clone(), fast("a")),
            std::future::pending(),
            || tokio::time::sleep(Duration::from_millis(20)),
        )
        .await;
        assert_eq!(ended, Ended::Finished);
        assert_eq!(lease.get().await.unwrap().unwrap().holder, None, "released");
        // So the next candidate leads at once, not a lease duration later.
        let spans = Spans::default();
        let s2 = spans.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let b = tokio::spawn(lead(
            Elector::new(lease.clone(), fast("b")),
            async move {
                let _ = rx.await;
            },
            move || s2.work("b", None),
        ));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(spans.get()[0].1 < t0 + Duration::from_millis(150));
        tx.send(()).unwrap();
        assert_eq!(b.await.unwrap(), Ended::Shutdown);
        assert!(spans.get()[0].2.is_some(), "work dropped");
        assert_eq!(lease.get().await.unwrap().unwrap().holder, None);
    }
}
