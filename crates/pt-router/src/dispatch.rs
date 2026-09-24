//! The dispatcher: scheduler order plus worker selection, with capacity accounting.
//!
//! Pure and synchronous: the HTTP layer calls [`Dispatcher::dispatch`] whenever a request
//! arrives or a worker frees capacity, and gets back the requests to send now.
//!
//! While a region failover is active ([`Dispatcher::note_failover`]), new PAYG is fenced
//! off hot spares, and [`Dispatcher::preempt`] names running PAYG requests to abort when
//! provisioned work has waited longer than the grace period with every eligible worker
//! busy (docs/06 §3, docs/07 §4).

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use pt_core::TrafficClass;
use serde::Serialize;

use crate::scheduler::{Item, Scheduler, CLASSES};
use crate::workers::{
    feasible, preemption_victim, select, Allocation, NoWorker, Placement, PrefixIndex, Running,
    Weights, Worker,
};

/// Queued heads examined per dispatch round, to bound the work when many flows are blocked.
const SCAN_LIMIT: usize = 64;
/// Sessions remembered for affinity.
const MAX_SESSIONS: usize = 100_000;

/// A request sent to a worker.
#[derive(Debug)]
pub struct Assignment<T> {
    pub id: u64,
    pub worker: usize,
    pub placement: Placement,
    pub payload: T,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ClassCounts {
    pub provisioned: u64,
    pub burst: u64,
    pub spillover: u64,
    pub payg: u64,
}

impl ClassCounts {
    fn bump(&mut self, c: TrafficClass) {
        match c {
            TrafficClass::Provisioned => self.provisioned += 1,
            TrafficClass::Burst => self.burst += 1,
            TrafficClass::Spillover => self.spillover += 1,
            TrafficClass::Payg => self.payg += 1,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Status {
    pub queued: ClassCounts,
    pub dispatched: ClassCounts,
    /// PAYG requests aborted to make room for provisioned work.
    pub preempted: u64,
    /// A region failover is active: PAYG is fenced off hot spares.
    pub failover_active: bool,
    pub workers: Vec<Worker>,
}

/// A dispatched request, until it's released.
struct Active {
    worker: usize,
    placement: Placement,
    seq: u64,
    preempting: bool,
}

pub struct Dispatcher<T> {
    /// Queued requests with the time they were queued.
    scheduler: Scheduler<(Placement, Instant, T)>,
    running: HashMap<u64, Active>,
    seq: u64,
    failover_until: Option<Instant>,
    preempted: u64,
    workers: Vec<Worker>,
    allocations: HashMap<String, Allocation>,
    index: PrefixIndex,
    sessions: HashMap<String, usize>,
    session_order: VecDeque<String>,
    weights: Weights,
    next_id: u64,
    dispatched: ClassCounts,
}

impl<T> Dispatcher<T> {
    pub fn new(
        workers: Vec<Worker>,
        allocations: HashMap<String, Allocation>,
        weights: Weights,
        payg_guard_every: u64,
    ) -> Self {
        Self {
            scheduler: Scheduler::new(payg_guard_every),
            running: HashMap::new(),
            seq: 0,
            failover_until: None,
            preempted: 0,
            workers,
            allocations,
            index: PrefixIndex::new(100_000),
            sessions: HashMap::new(),
            session_order: VecDeque::new(),
            weights,
            next_id: 1,
            dispatched: ClassCounts::default(),
        }
    }

    pub fn worker(&self, i: usize) -> &Worker {
        &self.workers[i]
    }

    /// Queue a request. Fails at once if no worker could ever take it.
    ///
    /// The WFQ weight is the reservation's allocation on this pool if configured, else
    /// `fallback_weight` (for example from the gateway's `x-pt-weight` header).
    pub fn enqueue(
        &mut self,
        placement: Placement,
        wu: f64,
        fallback_weight: Option<f64>,
        now: Instant,
        deadline: Instant,
        payload: T,
    ) -> Result<u64, NoWorker> {
        feasible(&self.workers, &self.allocations, &placement)?;
        let weight = self
            .allocations
            .get(&placement.reservation)
            .map(|a| a.wu_per_sec)
            .filter(|w| *w > 0.0)
            .or(fallback_weight.filter(|w| *w > 0.0))
            .unwrap_or(1.0);
        let id = self.next_id;
        self.next_id += 1;
        let flow = placement.reservation.clone();
        let class = placement.class;
        self.scheduler.enqueue(Item {
            id,
            flow,
            class,
            wu,
            weight,
            deadline,
            payload: (placement, now, payload),
        });
        Ok(id)
    }

    /// A region failover is active until `until`. Called for every request the gateway
    /// marks as using a failover entitlement, so the fence lifts soon after they stop.
    pub fn note_failover(&mut self, until: Instant) {
        self.failover_until = Some(self.failover_until.map_or(until, |u| u.max(until)));
    }

    pub fn failover_active(&self, now: Instant) -> bool {
        self.failover_until.is_some_and(|u| now < u)
    }

    /// Send whatever can go now, in scheduler order. A flow whose head can't be placed is
    /// skipped, so it doesn't block the others.
    pub fn dispatch(&mut self, now: Instant) -> Vec<Assignment<T>> {
        let fence = self.failover_active(now);
        let mut out = Vec::new();
        loop {
            let mut placed = None;
            for h in self.scheduler.candidates().into_iter().take(SCAN_LIMIT) {
                let Some(q) = self.scheduler.peek(&h) else {
                    continue;
                };
                let pick = select(
                    &self.workers,
                    &self.allocations,
                    &self.index,
                    &self.sessions,
                    &self.weights,
                    &q.payload.0,
                    fence,
                );
                if let Ok(w) = pick {
                    placed = Some((h, w));
                    break;
                }
            }
            let Some((h, w)) = placed else { break };
            let q = self.scheduler.take(&h).expect("peeked above");
            let (placement, _, payload) = q.payload;
            self.reserve(w, &placement);
            self.dispatched.bump(placement.class);
            self.seq += 1;
            self.running.insert(
                q.id,
                Active {
                    worker: w,
                    placement: placement.clone(),
                    seq: self.seq,
                    preempting: false,
                },
            );
            out.push(Assignment {
                id: q.id,
                worker: w,
                placement,
                payload,
            });
        }
        out
    }

    fn reserve(&mut self, w: usize, p: &Placement) {
        let worker = &mut self.workers[w];
        worker.slots_used += 1;
        worker.kv_used += p.kv_blocks;
        *worker
            .kv_by_reservation
            .entry(p.reservation.clone())
            .or_default() += p.kv_blocks;
        self.index.record(&p.prefixes, w);
        if let Some(s) = &p.session {
            if self.sessions.insert(s.clone(), w).is_none() {
                self.session_order.push_back(s.clone());
                while self.session_order.len() > MAX_SESSIONS {
                    if let Some(old) = self.session_order.pop_front() {
                        self.sessions.remove(&old);
                    }
                }
            }
        }
    }

    /// Running PAYG requests to abort now so waiting provisioned work can be placed. Only
    /// while a failover is active, and only for provisioned heads that have waited at least
    /// `grace`. Each victim is marked, so it's named once and its capacity counts as freed.
    pub fn preempt(&mut self, now: Instant, grace: Duration) -> Vec<u64> {
        if !self.failover_active(now) {
            return vec![];
        }
        let mut victims = Vec::new();
        for h in self.scheduler.candidates().into_iter().take(SCAN_LIMIT) {
            let Some(q) = self.scheduler.peek(&h) else {
                continue;
            };
            let (placement, queued_at, _) = &q.payload;
            if q.class != TrafficClass::Provisioned || now.duration_since(*queued_at) < grace {
                continue;
            }
            let running: Vec<Running> = self
                .running
                .iter()
                .map(|(id, a)| Running {
                    id: *id,
                    worker: a.worker,
                    class: a.placement.class,
                    kv_blocks: a.placement.kv_blocks,
                    started: a.seq,
                    preempting: a.preempting,
                })
                .collect();
            if let Some(id) = preemption_victim(
                &self.workers,
                &self.allocations,
                &self.weights,
                &running,
                placement,
            ) {
                if let Some(a) = self.running.get_mut(&id) {
                    a.preempting = true;
                }
                self.preempted += 1;
                victims.push(id);
            }
        }
        victims
    }

    /// A dispatched request finished, failed, or was preempted: free its slot and KV
    /// blocks. Returns false if it was already released.
    pub fn release(&mut self, id: u64) -> bool {
        let Some(Active {
            worker, placement, ..
        }) = self.running.remove(&id)
        else {
            return false;
        };
        let p = &placement;
        let w = &mut self.workers[worker];
        w.slots_used = w.slots_used.saturating_sub(1);
        w.kv_used = w.kv_used.saturating_sub(p.kv_blocks);
        if let Some(held) = w.kv_by_reservation.get_mut(&p.reservation) {
            *held = held.saturating_sub(p.kv_blocks);
            if *held == 0 {
                w.kv_by_reservation.remove(&p.reservation);
            }
        }
        true
    }

    /// Drop a queued request (client gone or timed out).
    pub fn cancel(&mut self, id: u64) -> bool {
        self.scheduler.remove(id).is_some()
    }

    pub fn status(&self, now: Instant) -> Status {
        let mut queued = ClassCounts::default();
        for c in CLASSES {
            let n = self.scheduler.len_of(c) as u64;
            match c {
                TrafficClass::Provisioned => queued.provisioned = n,
                TrafficClass::Burst => queued.burst = n,
                TrafficClass::Spillover => queued.spillover = n,
                TrafficClass::Payg => queued.payg = n,
            }
        }
        Status {
            queued,
            dispatched: self.dispatched.clone(),
            preempted: self.preempted,
            failover_active: self.failover_active(now),
            workers: self.workers.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn later() -> Instant {
        Instant::now() + Duration::from_secs(60)
    }

    fn p(res: &str, class: TrafficClass, blocks: u32) -> Placement {
        Placement {
            reservation: res.into(),
            class,
            kv_blocks: blocks,
            prefixes: vec![],
            prompt_tokens: 0,
            session: None,
        }
    }

    fn one_slot() -> Dispatcher<&'static str> {
        Dispatcher::new(
            vec![Worker::new("w0", "http://x", 1, 100)],
            HashMap::new(),
            Weights::default(),
            0,
        )
    }

    #[test]
    fn pull_based_one_at_a_time_in_priority_order() {
        let mut d = one_slot();
        d.enqueue(
            p("a", TrafficClass::Payg, 1),
            1.0,
            None,
            Instant::now(),
            later(),
            "payg",
        )
        .unwrap();
        d.enqueue(
            p("b", TrafficClass::Provisioned, 1),
            1.0,
            None,
            Instant::now(),
            later(),
            "prov",
        )
        .unwrap();
        let first = d.dispatch(Instant::now());
        assert_eq!(first.len(), 1, "one slot");
        assert_eq!(first[0].payload, "prov");
        assert!(
            d.dispatch(Instant::now()).is_empty(),
            "nothing until capacity frees"
        );
        d.release(first[0].id);
        assert_eq!(d.dispatch(Instant::now())[0].payload, "payg");
    }

    #[test]
    fn a_blocked_flow_does_not_block_others() {
        let allocs = HashMap::from([(
            "greedy".to_string(),
            Allocation {
                wu_per_sec: 10.0,
                kv_share: Some(0.1),
                dedicated_workers: vec![],
            },
        )]);
        let mut d: Dispatcher<u32> = Dispatcher::new(
            vec![Worker::new("w0", "http://x", 10, 100)],
            allocs,
            Weights::default(),
            0,
        );
        // greedy's budget is 0.1 × 100 × 1.2 = 12 blocks: its first 10-block request runs,
        // the second can't. Another tenant queued behind it still goes.
        d.enqueue(
            p("greedy", TrafficClass::Provisioned, 10),
            1.0,
            None,
            Instant::now(),
            later(),
            1,
        )
        .unwrap();
        d.enqueue(
            p("greedy", TrafficClass::Provisioned, 10),
            1.0,
            None,
            Instant::now(),
            later(),
            2,
        )
        .unwrap();
        d.enqueue(
            p("other", TrafficClass::Provisioned, 10),
            1.0,
            None,
            Instant::now(),
            later(),
            3,
        )
        .unwrap();
        let sent: Vec<u32> = d
            .dispatch(Instant::now())
            .into_iter()
            .map(|a| a.payload)
            .collect();
        assert_eq!(sent, [1, 3]);
        assert_eq!(d.status(Instant::now()).queued.provisioned, 1);
    }

    #[test]
    fn impossible_requests_fail_fast_and_cancel_works() {
        let mut d = one_slot();
        assert_eq!(
            d.enqueue(
                p("a", TrafficClass::Provisioned, 500),
                1.0,
                None,
                Instant::now(),
                later(),
                "big"
            ),
            Err(NoWorker::TooLarge)
        );
        let _held = d
            .enqueue(
                p("a", TrafficClass::Provisioned, 1),
                1.0,
                None,
                Instant::now(),
                later(),
                "x",
            )
            .unwrap();
        let id = d
            .enqueue(
                p("a", TrafficClass::Provisioned, 1),
                1.0,
                None,
                Instant::now(),
                later(),
                "y",
            )
            .unwrap();
        d.dispatch(Instant::now());
        assert!(d.cancel(id));
        assert!(!d.cancel(id));
        assert_eq!(d.status(Instant::now()).queued.provisioned, 0);
    }

    #[test]
    fn release_returns_capacity() {
        let mut d = one_slot();
        d.enqueue(
            p("a", TrafficClass::Provisioned, 30),
            1.0,
            None,
            Instant::now(),
            later(),
            "x",
        )
        .unwrap();
        let a = d.dispatch(Instant::now()).pop().unwrap();
        assert_eq!(d.worker(0).kv_used, 30);
        assert_eq!(d.worker(0).kv_by_reservation["a"], 30);
        assert!(d.release(a.id));
        assert!(!d.release(a.id), "released once");
        assert_eq!(d.worker(0).kv_used, 0);
        assert!(d.worker(0).kv_by_reservation.is_empty());
        assert_eq!(d.status(Instant::now()).dispatched.provisioned, 1);
    }

    #[test]
    fn failover_fences_spares_and_preempts_payg_for_waiting_provisioned() {
        let mut d: Dispatcher<&str> = Dispatcher::new(
            vec![
                Worker::new("floor", "http://x", 1, 100),
                Worker::new("spare", "http://x", 1, 100).with_hot_spare(true),
            ],
            HashMap::new(),
            Weights::default(),
            0,
        );
        let t0 = Instant::now();
        let grace = Duration::from_millis(250);
        // Normal times: PAYG goes to the spare first, then the floor.
        d.enqueue(
            p("p", TrafficClass::Payg, 1),
            1.0,
            None,
            t0,
            later(),
            "payg1",
        )
        .unwrap();
        let a = d.dispatch(t0).pop().unwrap();
        assert_eq!((a.payload, a.worker), ("payg1", 1));
        d.enqueue(
            p("p", TrafficClass::Payg, 1),
            1.0,
            None,
            t0,
            later(),
            "payg2",
        )
        .unwrap();
        assert_eq!(d.dispatch(t0).pop().unwrap().worker, 0);

        // No failover: provisioned waits, nothing is preempted.
        d.enqueue(
            p("r", TrafficClass::Provisioned, 1),
            1.0,
            None,
            t0,
            later(),
            "prov",
        )
        .unwrap();
        assert!(d.dispatch(t0).is_empty());
        assert!(d.preempt(t0 + grace * 4, grace).is_empty());

        // Failover: within the grace nothing happens, then the spare's PAYG is preempted.
        d.note_failover(t0 + Duration::from_secs(30));
        assert!(
            d.preempt(t0 + grace / 2, grace).is_empty(),
            "within the grace"
        );
        let t1 = t0 + grace;
        let victims = d.preempt(t1, grace);
        assert_eq!(victims, [a.id], "the hot spare's PAYG");
        assert!(d.preempt(t1, grace).is_empty(), "named once");
        assert!(d.release(a.id));
        let sent = d.dispatch(t1);
        assert_eq!((sent[0].payload, sent[0].worker), ("prov", 1));
        let s = d.status(t1);
        assert_eq!(s.preempted, 1);
        assert!(s.failover_active);

        // The fence keeps new PAYG off the spare while the failover lasts.
        d.enqueue(
            p("p", TrafficClass::Payg, 1),
            1.0,
            None,
            t1,
            later(),
            "payg3",
        )
        .unwrap();
        assert!(d.release(sent[0].id));
        assert!(d.dispatch(t1).is_empty(), "spare free, but fenced");
        assert!(!d.status(t0 + Duration::from_secs(31)).failover_active);
        assert_eq!(
            d.dispatch(t0 + Duration::from_secs(31))[0].payload,
            "payg3",
            "the fence lifts"
        );
    }
}
