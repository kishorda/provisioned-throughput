//! Worker state and selection (docs/05 §3–4).
//!
//! Dispatch is pull-based: a request only goes to a worker with a free slot and enough KV
//! blocks, so engine queues stay shallow and ordering stays with the scheduler. Among
//! workers that can take it, the router applies placement and per-tenant KV budgets, then
//! scores by prefix-cache overlap, session affinity, and load.
//!
//! Hot spares (docs/06 §3) serve PAYG until provisioned traffic needs them. PAYG prefers
//! them and provisioned traffic prefers the floor. During a region failover the router
//! fences new PAYG off hot spares and may preempt running PAYG ([`preemption_victim`]).

use std::collections::{HashMap, HashSet, VecDeque};

use pt_core::TrafficClass;
use serde::Serialize;

/// What the router knows about one request when placing it.
#[derive(Debug, Clone, PartialEq)]
pub struct Placement {
    /// Reservation id (the WFQ flow and KV-budget owner).
    pub reservation: String,
    pub class: TrafficClass,
    /// KV blocks the request is expected to hold: (prompt + max output) ÷ block size.
    pub kv_blocks: u32,
    /// Cumulative hashes of the message prefixes, with each prefix's token count.
    pub prefixes: Vec<(u64, u64)>,
    pub prompt_tokens: u64,
    pub session: Option<String>,
}

/// A reservation's allocation on this pool, mirroring the `PoolAllocation` CRD (docs/08 §2).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Allocation {
    /// WFQ weight: the reservation's WU/s on the pool.
    pub wu_per_sec: f64,
    /// Share of each worker's KV blocks this reservation may hold (docs/05 §4).
    pub kv_share: Option<f64>,
    /// Workers reserved for this reservation. Others' provisioned traffic can't use them;
    /// PAYG can, as backfill (ADR-006).
    pub dedicated_workers: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Worker {
    pub id: String,
    pub url: String,
    pub slots: u32,
    pub kv_blocks: u32,
    pub slots_used: u32,
    pub kv_used: u32,
    /// KV blocks held per reservation.
    pub kv_by_reservation: HashMap<String, u32>,
    /// Failure-domain headroom kept loaded: serves PAYG until provisioned traffic needs it.
    pub hot_spare: bool,
}

impl Worker {
    pub fn new(id: &str, url: &str, slots: u32, kv_blocks: u32) -> Self {
        Self {
            id: id.into(),
            url: url.into(),
            slots,
            kv_blocks,
            slots_used: 0,
            kv_used: 0,
            kv_by_reservation: HashMap::new(),
            hot_spare: false,
        }
    }

    pub fn with_hot_spare(mut self, hot_spare: bool) -> Self {
        self.hot_spare = hot_spare;
        self
    }

    fn has_credit(&self, blocks: u32) -> bool {
        self.slots_used < self.slots && self.kv_used + blocks <= self.kv_blocks
    }

    fn load(&self) -> f64 {
        let s = f64::from(self.slots_used) / f64::from(self.slots.max(1));
        let k = f64::from(self.kv_used) / f64::from(self.kv_blocks.max(1));
        (s + k) / 2.0
    }
}

/// Which workers recently served which message prefixes: an approximate prefix-cache index,
/// like Dynamo's approximate KV routing mode. Bounded; oldest entries are forgotten.
#[derive(Debug, Default)]
pub struct PrefixIndex {
    by_hash: HashMap<u64, HashSet<usize>>,
    order: VecDeque<u64>,
    capacity: usize,
}

impl PrefixIndex {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            ..Default::default()
        }
    }

    pub fn record(&mut self, prefixes: &[(u64, u64)], worker: usize) {
        for (h, _) in prefixes {
            let set = self.by_hash.entry(*h).or_default();
            if set.is_empty() {
                self.order.push_back(*h);
            }
            set.insert(worker);
        }
        while self.order.len() > self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.by_hash.remove(&old);
            }
        }
    }

    /// Tokens of the longest prefix `worker` has seen.
    pub fn overlap(&self, prefixes: &[(u64, u64)], worker: usize) -> u64 {
        prefixes
            .iter()
            .take_while(|(h, _)| self.by_hash.get(h).is_some_and(|s| s.contains(&worker)))
            .last()
            .map_or(0, |(_, tokens)| *tokens)
    }
}

/// Score weights for worker selection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Weights {
    pub overlap: f64,
    pub load: f64,
    pub session: f64,
    /// KV budget overcommit: a reservation may hold up to share × blocks × this.
    pub kv_overcommit: f64,
    /// Cost of the wrong kind of worker: PAYG on the floor, or provisioned on a hot spare.
    pub spare: f64,
}

impl Default for Weights {
    fn default() -> Self {
        Self {
            overlap: 1.0,
            load: 1.0,
            session: 0.5,
            kv_overcommit: 1.2,
            spare: 0.25,
        }
    }
}

/// Why no worker could take a request right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoWorker {
    /// Every eligible worker is full. Wait for a completion.
    Busy,
    /// The reservation's KV budget is used up on every eligible worker.
    KvBudget,
    /// No worker is eligible at all (for example, a dedicated set that doesn't exist).
    NoEligible,
    /// The request needs more KV blocks than any eligible worker has in total.
    TooLarge,
}

/// Workers that placement rules allow for `p`, ignoring current load.
pub fn eligible(
    workers: &[Worker],
    allocations: &HashMap<String, Allocation>,
    p: &Placement,
) -> Vec<usize> {
    let alloc = allocations.get(&p.reservation);
    let dedicated_to: HashMap<&str, &str> = allocations
        .iter()
        .flat_map(|(res, a)| {
            a.dedicated_workers
                .iter()
                .map(move |w| (w.as_str(), res.as_str()))
        })
        .collect();
    let payg = matches!(p.class, TrafficClass::Payg | TrafficClass::Spillover);
    (0..workers.len())
        .filter(|&i| {
            let w = &workers[i];
            // Placement: dedicated workers serve their reservation, plus PAYG backfill.
            let owner_ok = match dedicated_to.get(w.id.as_str()) {
                Some(owner) => *owner == p.reservation || payg,
                None => true,
            };
            // A reservation with dedicated workers uses only those for provisioned traffic.
            let own_set_ok = match alloc.filter(|a| !a.dedicated_workers.is_empty() && !payg) {
                Some(a) => a.dedicated_workers.contains(&w.id),
                None => true,
            };
            owner_ok && own_set_ok
        })
        .collect()
}

/// Whether `p` could ever be placed: some eligible worker is big enough.
pub fn feasible(
    workers: &[Worker],
    allocations: &HashMap<String, Allocation>,
    p: &Placement,
) -> Result<(), NoWorker> {
    let e = eligible(workers, allocations, p);
    if e.is_empty() {
        return Err(NoWorker::NoEligible);
    }
    if e.iter().all(|&i| p.kv_blocks > workers[i].kv_blocks) {
        return Err(NoWorker::TooLarge);
    }
    Ok(())
}

/// Whether `p`'s reservation may place `p.kv_blocks` more on worker `w`.
fn within_budget(
    w: &Worker,
    allocations: &HashMap<String, Allocation>,
    weights: &Weights,
    p: &Placement,
) -> bool {
    let Some(share) = allocations.get(&p.reservation).and_then(|a| a.kv_share) else {
        return true;
    };
    let budget = (share * f64::from(w.kv_blocks) * weights.kv_overcommit).floor() as u32;
    let held = w
        .kv_by_reservation
        .get(&p.reservation)
        .copied()
        .unwrap_or(0);
    // Nothing held here yet: always allowed, so one request larger than the budget can
    // still run alone.
    held == 0 || held + p.kv_blocks <= budget
}

/// Pick a worker for `p`, or say why none can take it now. With `fence` set (a region
/// failover is active), PAYG isn't placed on hot spares.
pub fn select(
    workers: &[Worker],
    allocations: &HashMap<String, Allocation>,
    index: &PrefixIndex,
    sessions: &HashMap<String, usize>,
    weights: &Weights,
    p: &Placement,
    fence: bool,
) -> Result<usize, NoWorker> {
    feasible(workers, allocations, p)?;
    let eligible = eligible(workers, allocations, p);
    let backfill = matches!(p.class, TrafficClass::Payg | TrafficClass::Spillover);

    let mut best: Option<(usize, f64)> = None;
    let mut blocked_by_budget = false;
    for &i in &eligible {
        let w = &workers[i];
        if fence && p.class == TrafficClass::Payg && w.hot_spare {
            continue;
        }
        if !w.has_credit(p.kv_blocks) {
            continue;
        }
        if !within_budget(w, allocations, weights, p) {
            blocked_by_budget = true;
            continue;
        }
        let overlap = if p.prompt_tokens > 0 {
            index.overlap(&p.prefixes, i) as f64 / p.prompt_tokens as f64
        } else {
            0.0
        };
        let session = match &p.session {
            Some(s) if sessions.get(s) == Some(&i) => 1.0,
            _ => 0.0,
        };
        let misplaced = if backfill != w.hot_spare { 1.0 } else { 0.0 };
        let cost = weights.load * w.load() - weights.overlap * overlap - weights.session * session
            + weights.spare * misplaced;
        if best.is_none_or(|(_, c)| cost < c) {
            best = Some((i, cost));
        }
    }
    match best {
        Some((i, _)) => Ok(i),
        None if blocked_by_budget => Err(NoWorker::KvBudget),
        None => Err(NoWorker::Busy),
    }
}

/// A request running on a worker, as preemption sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct Running {
    pub id: u64,
    pub worker: usize,
    pub class: TrafficClass,
    pub kv_blocks: u32,
    /// Monotonic start order: larger started later.
    pub started: u64,
    /// Already chosen for preemption; its capacity is about to free.
    pub preempting: bool,
}

/// Which running PAYG request to preempt so that `p` can be placed, if any.
///
/// Returns `None` when `p` isn't blocked by load, or when requests already being preempted
/// will free enough room. Otherwise it picks, among workers eligible for `p`, a running
/// PAYG request whose slot and KV blocks let `p` fit: on a hot spare if possible, then the
/// youngest, so the least generated work is lost.
pub fn preemption_victim(
    workers: &[Worker],
    allocations: &HashMap<String, Allocation>,
    weights: &Weights,
    running: &[Running],
    p: &Placement,
) -> Option<u64> {
    let eligible = eligible(workers, allocations, p);
    // Room on worker `i` once the listed requests are gone.
    let fits_without = |i: usize, freed: &[&Running]| {
        let w = &workers[i];
        let slots = w.slots_used.saturating_sub(freed.len() as u32);
        let kv = w
            .kv_used
            .saturating_sub(freed.iter().map(|r| r.kv_blocks).sum::<u32>());
        slots < w.slots
            && kv + p.kv_blocks <= w.kv_blocks
            && within_budget(w, allocations, weights, p)
    };
    let pending = |i: usize| -> Vec<&Running> {
        running
            .iter()
            .filter(|r| r.worker == i && r.preempting)
            .collect()
    };
    if eligible.iter().any(|&i| fits_without(i, &pending(i))) {
        return None;
    }
    eligible
        .iter()
        .flat_map(|&i| {
            running
                .iter()
                .filter(move |r| r.worker == i && r.class == TrafficClass::Payg && !r.preempting)
        })
        .filter(|r| {
            let mut freed = pending(r.worker);
            freed.push(r);
            fits_without(r.worker, &freed)
        })
        .max_by_key(|r| (workers[r.worker].hot_spare, r.started))
        .map(|r| r.id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workers(n: usize) -> Vec<Worker> {
        (0..n)
            .map(|i| Worker::new(&format!("w{i}"), "http://x", 2, 100))
            .collect()
    }

    fn place(res: &str, class: TrafficClass, blocks: u32) -> Placement {
        Placement {
            reservation: res.into(),
            class,
            kv_blocks: blocks,
            prefixes: vec![(11, 100), (22, 200)],
            prompt_tokens: 250,
            session: None,
        }
    }

    fn sel(
        w: &[Worker],
        a: &HashMap<String, Allocation>,
        idx: &PrefixIndex,
        p: &Placement,
    ) -> Result<usize, NoWorker> {
        select(w, a, idx, &HashMap::new(), &Weights::default(), p, false)
    }

    #[test]
    fn prefers_idle_then_prefix_overlap() {
        let mut w = workers(2);
        let none = HashMap::new();
        let mut idx = PrefixIndex::new(100);
        w[0].slots_used = 1;
        w[0].kv_used = 50;
        assert_eq!(
            sel(&w, &none, &idx, &place("r", TrafficClass::Provisioned, 10)),
            Ok(1),
            "less loaded"
        );
        // Worker 0 holds 200 of the 250 prompt tokens: overlap outweighs its load.
        idx.record(&[(11, 100), (22, 200)], 0);
        assert_eq!(
            sel(&w, &none, &idx, &place("r", TrafficClass::Provisioned, 10)),
            Ok(0)
        );
        assert_eq!(
            idx.overlap(&[(11, 100), (99, 200)], 0),
            100,
            "stops at the first miss"
        );
    }

    #[test]
    fn session_affinity_breaks_ties() {
        let w = workers(3);
        let sessions = HashMap::from([("s".to_string(), 2usize)]);
        let mut p = place("r", TrafficClass::Provisioned, 1);
        p.session = Some("s".into());
        let got = select(
            &w,
            &HashMap::new(),
            &PrefixIndex::new(10),
            &sessions,
            &Weights::default(),
            &p,
            false,
        );
        assert_eq!(got, Ok(2));
    }

    #[test]
    fn pull_based_credits() {
        let mut w = workers(1);
        let none = HashMap::new();
        let idx = PrefixIndex::new(10);
        w[0].slots_used = 2;
        assert_eq!(
            sel(&w, &none, &idx, &place("r", TrafficClass::Provisioned, 1)),
            Err(NoWorker::Busy)
        );
        w[0].slots_used = 0;
        w[0].kv_used = 95;
        assert_eq!(
            sel(&w, &none, &idx, &place("r", TrafficClass::Provisioned, 10)),
            Err(NoWorker::Busy)
        );
        assert_eq!(
            sel(&w, &none, &idx, &place("r", TrafficClass::Provisioned, 500)),
            Err(NoWorker::TooLarge)
        );
    }

    #[test]
    fn dedicated_workers_and_payg_backfill() {
        let w = workers(2);
        let allocs = HashMap::from([(
            "big".to_string(),
            Allocation {
                wu_per_sec: 1.0,
                kv_share: None,
                dedicated_workers: vec!["w1".into()],
            },
        )]);
        let idx = PrefixIndex::new(10);
        // big uses only its dedicated worker.
        assert_eq!(
            sel(
                &w,
                &allocs,
                &idx,
                &place("big", TrafficClass::Provisioned, 1)
            ),
            Ok(1)
        );
        // Others' provisioned traffic can't use w1 ...
        let mut w_busy0 = w.clone();
        w_busy0[0].slots_used = 2;
        assert_eq!(
            sel(
                &w_busy0,
                &allocs,
                &idx,
                &place("small", TrafficClass::Provisioned, 1)
            ),
            Err(NoWorker::Busy)
        );
        // ... but PAYG can backfill it.
        assert_eq!(
            sel(
                &w_busy0,
                &allocs,
                &idx,
                &place("small", TrafficClass::Payg, 1)
            ),
            Ok(1)
        );
    }

    #[test]
    fn kv_budget_per_reservation() {
        let mut w = workers(2);
        let allocs = HashMap::from([(
            "r".to_string(),
            Allocation {
                wu_per_sec: 1.0,
                kv_share: Some(0.25),
                dedicated_workers: vec![],
            },
        )]);
        let idx = PrefixIndex::new(10);
        // Budget: 0.25 × 100 × 1.2 = 30 blocks per worker.
        w[0].kv_by_reservation.insert("r".into(), 25);
        w[0].kv_used = 25;
        assert_eq!(
            sel(
                &w,
                &allocs,
                &idx,
                &place("r", TrafficClass::Provisioned, 10)
            ),
            Ok(1)
        );
        w[1].kv_by_reservation.insert("r".into(), 25);
        w[1].kv_used = 25;
        assert_eq!(
            sel(
                &w,
                &allocs,
                &idx,
                &place("r", TrafficClass::Provisioned, 10)
            ),
            Err(NoWorker::KvBudget)
        );
        // Another reservation isn't affected.
        assert!(sel(
            &w,
            &allocs,
            &idx,
            &place("other", TrafficClass::Provisioned, 10)
        )
        .is_ok());
    }

    #[test]
    fn payg_prefers_hot_spares_and_is_fenced_off_them_in_failover() {
        let w = vec![
            Worker::new("floor", "http://x", 2, 100),
            Worker::new("spare", "http://x", 2, 100).with_hot_spare(true),
        ];
        let none = HashMap::new();
        let idx = PrefixIndex::new(10);
        let payg = place("p", TrafficClass::Payg, 1);
        let prov = place("r", TrafficClass::Provisioned, 1);
        assert_eq!(sel(&w, &none, &idx, &payg), Ok(1));
        assert_eq!(sel(&w, &none, &idx, &prov), Ok(0));
        let fenced = |p: &Placement, w: &[Worker]| {
            select(
                w,
                &none,
                &idx,
                &HashMap::new(),
                &Weights::default(),
                p,
                true,
            )
        };
        assert_eq!(fenced(&payg, &w), Ok(0), "fenced off the spare");
        let mut full = w.clone();
        full[0].slots_used = 2;
        assert_eq!(fenced(&payg, &full), Err(NoWorker::Busy));
        assert_eq!(fenced(&prov, &full), Ok(1), "provisioned may use the spare");
        let spill = place("r", TrafficClass::Spillover, 1);
        assert_eq!(fenced(&spill, &full), Ok(1), "only PAYG is fenced");
    }

    fn run(id: u64, worker: usize, class: TrafficClass, started: u64) -> Running {
        Running {
            id,
            worker,
            class,
            kv_blocks: 10,
            started,
            preempting: false,
        }
    }

    #[test]
    fn preemption_picks_youngest_payg_on_a_spare() {
        let mut w = vec![
            Worker::new("floor", "http://x", 2, 100),
            Worker::new("spare", "http://x", 2, 100).with_hot_spare(true),
        ];
        for x in &mut w {
            x.slots_used = 2;
            x.kv_used = 20;
        }
        let none = HashMap::new();
        let wt = Weights::default();
        let prov = place("r", TrafficClass::Provisioned, 10);
        let mut running = vec![
            run(1, 0, TrafficClass::Payg, 1),
            run(2, 0, TrafficClass::Provisioned, 2),
            run(3, 1, TrafficClass::Payg, 3),
            run(4, 1, TrafficClass::Payg, 4),
        ];
        // On the spare first, and the youngest there.
        assert_eq!(preemption_victim(&w, &none, &wt, &running, &prov), Some(4));
        // Once one is being preempted, no more are needed for one waiting request.
        running[3].preempting = true;
        assert_eq!(preemption_victim(&w, &none, &wt, &running, &prov), None);
        // Without spares' PAYG, the floor's PAYG goes.
        let floor_only = vec![
            run(1, 0, TrafficClass::Payg, 1),
            run(2, 0, TrafficClass::Provisioned, 2),
        ];
        assert_eq!(
            preemption_victim(&w, &none, &wt, &floor_only, &prov),
            Some(1)
        );
        // None when a worker already has room.
        w[0].slots_used = 1;
        assert_eq!(preemption_victim(&w, &none, &wt, &running, &prov), None);
        // Provisioned and spillover work is never a victim.
        w[0].slots_used = 2;
        let only_prov = vec![
            run(2, 0, TrafficClass::Provisioned, 2),
            run(6, 1, TrafficClass::Spillover, 3),
        ];
        assert_eq!(preemption_victim(&w, &none, &wt, &only_prov, &prov), None);
    }

    #[test]
    fn prefix_index_is_bounded() {
        let mut idx = PrefixIndex::new(2);
        idx.record(&[(1, 10), (2, 20), (3, 30)], 0);
        assert_eq!(idx.overlap(&[(1, 10)], 0), 0, "oldest forgotten");
        assert_eq!(idx.overlap(&[(2, 20), (3, 30)], 0), 30);
    }
}
