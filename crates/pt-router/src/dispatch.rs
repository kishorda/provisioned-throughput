//! The dispatcher: scheduler order plus worker selection, with capacity accounting.
//!
//! Pure and synchronous: the HTTP layer calls [`Dispatcher::dispatch`] whenever a request
//! arrives or a worker frees capacity, and gets back the requests to send now.

use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use pt_core::TrafficClass;
use serde::Serialize;

use crate::scheduler::{Item, Scheduler, CLASSES};
use crate::workers::{
    feasible, select, Allocation, NoWorker, Placement, PrefixIndex, Weights, Worker,
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
    pub workers: Vec<Worker>,
}

pub struct Dispatcher<T> {
    scheduler: Scheduler<(Placement, T)>,
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
            payload: (placement, payload),
        });
        Ok(id)
    }

    /// Send whatever can go now, in scheduler order. A flow whose head can't be placed is
    /// skipped, so it doesn't block the others.
    pub fn dispatch(&mut self) -> Vec<Assignment<T>> {
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
                );
                if let Ok(w) = pick {
                    placed = Some((h, w));
                    break;
                }
            }
            let Some((h, w)) = placed else { break };
            let q = self.scheduler.take(&h).expect("peeked above");
            let (placement, payload) = q.payload;
            self.reserve(w, &placement);
            self.dispatched.bump(placement.class);
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

    /// A request finished (or failed) on `worker`: free its slot and KV blocks.
    pub fn release(&mut self, worker: usize, p: &Placement) {
        let w = &mut self.workers[worker];
        w.slots_used = w.slots_used.saturating_sub(1);
        w.kv_used = w.kv_used.saturating_sub(p.kv_blocks);
        if let Some(held) = w.kv_by_reservation.get_mut(&p.reservation) {
            *held = held.saturating_sub(p.kv_blocks);
            if *held == 0 {
                w.kv_by_reservation.remove(&p.reservation);
            }
        }
    }

    /// Drop a queued request (client gone or timed out).
    pub fn cancel(&mut self, id: u64) -> bool {
        self.scheduler.remove(id).is_some()
    }

    pub fn status(&self) -> Status {
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
        d.enqueue(p("a", TrafficClass::Payg, 1), 1.0, None, later(), "payg")
            .unwrap();
        d.enqueue(
            p("b", TrafficClass::Provisioned, 1),
            1.0,
            None,
            later(),
            "prov",
        )
        .unwrap();
        let first = d.dispatch();
        assert_eq!(first.len(), 1, "one slot");
        assert_eq!(first[0].payload, "prov");
        assert!(d.dispatch().is_empty(), "nothing until capacity frees");
        d.release(first[0].worker, &first[0].placement);
        assert_eq!(d.dispatch()[0].payload, "payg");
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
            later(),
            1,
        )
        .unwrap();
        d.enqueue(
            p("greedy", TrafficClass::Provisioned, 10),
            1.0,
            None,
            later(),
            2,
        )
        .unwrap();
        d.enqueue(
            p("other", TrafficClass::Provisioned, 10),
            1.0,
            None,
            later(),
            3,
        )
        .unwrap();
        let sent: Vec<u32> = d.dispatch().into_iter().map(|a| a.payload).collect();
        assert_eq!(sent, [1, 3]);
        assert_eq!(d.status().queued.provisioned, 1);
    }

    #[test]
    fn impossible_requests_fail_fast_and_cancel_works() {
        let mut d = one_slot();
        assert_eq!(
            d.enqueue(
                p("a", TrafficClass::Provisioned, 500),
                1.0,
                None,
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
                later(),
                "x",
            )
            .unwrap();
        let id = d
            .enqueue(
                p("a", TrafficClass::Provisioned, 1),
                1.0,
                None,
                later(),
                "y",
            )
            .unwrap();
        d.dispatch();
        assert!(d.cancel(id));
        assert!(!d.cancel(id));
        assert_eq!(d.status().queued.provisioned, 0);
    }

    #[test]
    fn release_returns_capacity() {
        let mut d = one_slot();
        d.enqueue(
            p("a", TrafficClass::Provisioned, 30),
            1.0,
            None,
            later(),
            "x",
        )
        .unwrap();
        let a = d.dispatch().pop().unwrap();
        assert_eq!(d.worker(0).kv_used, 30);
        assert_eq!(d.worker(0).kv_by_reservation["a"], 30);
        d.release(a.worker, &a.placement);
        assert_eq!(d.worker(0).kv_used, 0);
        assert!(d.worker(0).kv_by_reservation.is_empty());
        assert_eq!(d.status().dispatched.provisioned, 1);
    }
}
