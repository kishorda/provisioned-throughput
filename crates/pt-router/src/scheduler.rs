//! Request ordering (docs/05 §2–3): strict priority classes, then weighted fair queuing by
//! WU across reservations within a class.
//!
//! WFQ uses self-clocked fair queuing. A request of `wu` from a flow with weight `w` gets
//! `finish = max(V, flow's last finish) + wu / w`, and the class's virtual time `V` becomes
//! the finish tag of each request dispatched. So a reservation sending 200K-token prompts
//! gets its weighted share of *work*, not the same number of requests as one sending
//! 500-token prompts.
//!
//! Requests within a flow stay FIFO. The dispatcher may skip a flow whose head can't be
//! placed yet (for example its KV budget is full), so one blocked tenant doesn't hold up
//! the others.

use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use pt_core::TrafficClass;

/// Dispatch order, highest first.
pub const CLASSES: [TrafficClass; 4] = [
    TrafficClass::Provisioned,
    TrafficClass::Burst,
    TrafficClass::Spillover,
    TrafficClass::Payg,
];

fn class_index(c: TrafficClass) -> usize {
    match c {
        TrafficClass::Provisioned => 0,
        TrafficClass::Burst => 1,
        TrafficClass::Spillover => 2,
        TrafficClass::Payg => 3,
    }
}

/// A request to queue. `T` is whatever the caller needs back when it's dispatched.
#[derive(Debug)]
pub struct Item<T> {
    pub id: u64,
    /// The WFQ flow: the reservation id.
    pub flow: String,
    pub class: TrafficClass,
    pub wu: f64,
    /// The flow's share, for example its WU/s allocation.
    pub weight: f64,
    pub deadline: Instant,
    pub payload: T,
}

/// A queued request.
#[derive(Debug)]
pub struct Queued<T> {
    pub id: u64,
    /// The WFQ flow: the reservation id.
    pub flow: String,
    pub class: TrafficClass,
    pub wu: f64,
    pub deadline: Instant,
    pub payload: T,
    finish: f64,
}

#[derive(Debug, Default)]
struct Flow<T> {
    last_finish: f64,
    queue: VecDeque<Queued<T>>,
}

#[derive(Debug)]
struct ClassQueue<T> {
    virtual_time: f64,
    flows: HashMap<String, Flow<T>>,
    len: usize,
}

impl<T> Default for ClassQueue<T> {
    fn default() -> Self {
        Self {
            virtual_time: 0.0,
            flows: HashMap::new(),
            len: 0,
        }
    }
}

/// Where a candidate request sits, to take it once a worker is found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handle {
    pub class: TrafficClass,
    pub flow: String,
    pub id: u64,
}

#[derive(Debug)]
pub struct Scheduler<T> {
    classes: [ClassQueue<T>; 4],
    /// Dispatches since the last PAYG dispatch, for the starvation guard.
    since_payg: u64,
    /// PAYG goes first after this many dispatches without one, if any is waiting. This is
    /// the small minimum share of docs/05 §3. 0 disables it.
    payg_guard_every: u64,
}

impl<T> Scheduler<T> {
    pub fn new(payg_guard_every: u64) -> Self {
        Self {
            classes: Default::default(),
            since_payg: 0,
            payg_guard_every,
        }
    }

    pub fn len(&self) -> usize {
        self.classes.iter().map(|c| c.len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len_of(&self, class: TrafficClass) -> usize {
        self.classes[class_index(class)].len
    }

    /// Add a request.
    pub fn enqueue(&mut self, item: Item<T>) {
        let cq = &mut self.classes[class_index(item.class)];
        let f = cq.flows.entry(item.flow.clone()).or_insert_with(|| Flow {
            last_finish: 0.0,
            queue: VecDeque::new(),
        });
        let start = cq.virtual_time.max(f.last_finish);
        let finish = start + item.wu.max(0.0) / item.weight.max(1e-9);
        f.last_finish = finish;
        f.queue.push_back(Queued {
            id: item.id,
            flow: item.flow,
            class: item.class,
            wu: item.wu,
            deadline: item.deadline,
            payload: item.payload,
            finish,
        });
        cq.len += 1;
    }

    /// Head requests in dispatch order: classes by priority (PAYG first if starved), and
    /// within a class, each flow's head by finish tag.
    pub fn candidates(&self) -> Vec<Handle> {
        let starved = self.payg_guard_every > 0
            && self.since_payg >= self.payg_guard_every
            && self.classes[3].len > 0;
        let mut order: Vec<usize> = vec![0, 1, 2, 3];
        if starved {
            order = vec![3, 0, 1, 2];
        }
        let mut out = Vec::new();
        for ci in order {
            let mut heads: Vec<&Queued<T>> = self.classes[ci]
                .flows
                .values()
                .filter_map(|f| f.queue.front())
                .collect();
            heads.sort_by(|a, b| a.finish.total_cmp(&b.finish).then(a.id.cmp(&b.id)));
            out.extend(heads.into_iter().map(|q| Handle {
                class: q.class,
                flow: q.flow.clone(),
                id: q.id,
            }));
        }
        out
    }

    pub fn peek(&self, h: &Handle) -> Option<&Queued<T>> {
        self.classes[class_index(h.class)]
            .flows
            .get(&h.flow)?
            .queue
            .front()
            .filter(|q| q.id == h.id)
    }

    /// Take the head request at `h` for dispatch.
    pub fn take(&mut self, h: &Handle) -> Option<Queued<T>> {
        let cq = &mut self.classes[class_index(h.class)];
        let flow = cq.flows.get_mut(&h.flow)?;
        if flow.queue.front().map(|q| q.id) != Some(h.id) {
            return None;
        }
        let q = flow.queue.pop_front()?;
        cq.len -= 1;
        cq.virtual_time = cq.virtual_time.max(q.finish);
        if flow.queue.is_empty() && flow.last_finish <= cq.virtual_time {
            cq.flows.remove(&h.flow);
        }
        if h.class == TrafficClass::Payg {
            self.since_payg = 0;
        } else {
            self.since_payg += 1;
        }
        Some(q)
    }

    /// Remove a request wherever it is (for example, the client went away).
    pub fn remove(&mut self, id: u64) -> Option<Queued<T>> {
        for cq in &mut self.classes {
            for f in cq.flows.values_mut() {
                if let Some(pos) = f.queue.iter().position(|q| q.id == id) {
                    cq.len -= 1;
                    return f.queue.remove(pos);
                }
            }
        }
        None
    }

    /// Remove and return requests past their deadline.
    pub fn expire(&mut self, now: Instant) -> Vec<Queued<T>> {
        let mut out = Vec::new();
        for cq in &mut self.classes {
            for f in cq.flows.values_mut() {
                let (keep, gone): (VecDeque<_>, VecDeque<_>) =
                    f.queue.drain(..).partition(|q| q.deadline > now);
                f.queue = keep;
                cq.len -= gone.len();
                out.extend(gone);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn later() -> Instant {
        Instant::now() + Duration::from_secs(60)
    }

    fn item(
        id: u64,
        flow: &str,
        class: TrafficClass,
        wu: f64,
        weight: f64,
        deadline: Instant,
    ) -> Item<()> {
        Item {
            id,
            flow: flow.into(),
            class,
            wu,
            weight,
            deadline,
            payload: (),
        }
    }

    /// Dispatch everything in order, returning (flow, id).
    fn drain(s: &mut Scheduler<()>) -> Vec<(String, u64)> {
        let mut out = Vec::new();
        while let Some(h) = s.candidates().into_iter().next() {
            let q = s.take(&h).unwrap();
            out.push((q.flow, q.id));
        }
        out
    }

    #[test]
    fn strict_priority_between_classes() {
        let mut s = Scheduler::new(0);
        s.enqueue(item(1, "a", TrafficClass::Payg, 1.0, 1.0, later()));
        s.enqueue(item(2, "a", TrafficClass::Spillover, 1.0, 1.0, later()));
        s.enqueue(item(3, "b", TrafficClass::Burst, 1.0, 1.0, later()));
        s.enqueue(item(4, "c", TrafficClass::Provisioned, 1.0, 1.0, later()));
        let ids: Vec<u64> = drain(&mut s).into_iter().map(|(_, id)| id).collect();
        assert_eq!(ids, [4, 3, 2, 1]);
    }

    #[test]
    fn wfq_shares_work_by_weight_not_request_count() {
        // Tenant a sends 1,000 WU requests; b sends 100 WU requests; equal weights.
        let mut s = Scheduler::new(0);
        for i in 0..10 {
            s.enqueue(item(
                i,
                "a",
                TrafficClass::Provisioned,
                1_000.0,
                1.0,
                later(),
            ));
        }
        for i in 100..200 {
            s.enqueue(item(i, "b", TrafficClass::Provisioned, 100.0, 1.0, later()));
        }
        // In the first 5,000 WU served, each tenant gets about half.
        let mut served: HashMap<String, f64> = HashMap::new();
        let mut total = 0.0;
        for (flow, _) in drain(&mut s) {
            let wu = if flow == "a" { 1_000.0 } else { 100.0 };
            if total >= 5_000.0 {
                break;
            }
            *served.entry(flow).or_default() += wu;
            total += wu;
        }
        let (a, b) = (served["a"], served["b"]);
        assert!((a - b).abs() <= 1_000.0, "a {a} b {b}");
    }

    #[test]
    fn weights_scale_the_share() {
        let mut s = Scheduler::new(0);
        for i in 0..100 {
            s.enqueue(item(
                i,
                "small",
                TrafficClass::Provisioned,
                10.0,
                1.0,
                later(),
            ));
            s.enqueue(item(
                1_000 + i,
                "big",
                TrafficClass::Provisioned,
                10.0,
                3.0,
                later(),
            ));
        }
        let first: Vec<_> = drain(&mut s).into_iter().take(40).collect();
        let big = first.iter().filter(|(f, _)| f == "big").count();
        assert_eq!(
            big, 30,
            "3:1 weights give 3:1 service while both are backlogged"
        );
    }

    #[test]
    fn idle_flow_does_not_bank_credit() {
        // b was idle while a ran; when b arrives it doesn't jump ahead of everything a queued
        // after it by more than its fair share.
        let mut s = Scheduler::new(0);
        for i in 0..5 {
            s.enqueue(item(i, "a", TrafficClass::Provisioned, 10.0, 1.0, later()));
        }
        for _ in 0..5 {
            let h = s.candidates()[0].clone();
            s.take(&h);
        }
        s.enqueue(item(10, "a", TrafficClass::Provisioned, 10.0, 1.0, later()));
        s.enqueue(item(11, "b", TrafficClass::Provisioned, 10.0, 1.0, later()));
        let order: Vec<u64> = drain(&mut s).into_iter().map(|(_, id)| id).collect();
        assert_eq!(
            order,
            [10, 11],
            "same start from the virtual time; FIFO tie-break"
        );
    }

    #[test]
    fn payg_guard_prevents_starvation() {
        let mut s = Scheduler::new(3);
        s.enqueue(item(0, "p", TrafficClass::Payg, 1.0, 1.0, later()));
        for i in 1..=10 {
            s.enqueue(item(i, "a", TrafficClass::Provisioned, 1.0, 1.0, later()));
        }
        let ids: Vec<u64> = drain(&mut s).into_iter().map(|(_, id)| id).collect();
        assert_eq!(
            ids[3], 0,
            "PAYG goes after 3 provisioned dispatches: {ids:?}"
        );
    }

    #[test]
    fn skipping_a_blocked_flow_and_removal() {
        let mut s = Scheduler::new(0);
        s.enqueue(item(1, "a", TrafficClass::Provisioned, 1.0, 1.0, later()));
        s.enqueue(item(2, "b", TrafficClass::Provisioned, 5.0, 1.0, later()));
        let c = s.candidates();
        assert_eq!(c.len(), 2);
        // Take b even though a is first (a's worker isn't available).
        assert_eq!(s.take(&c[1]).unwrap().id, 2);
        assert_eq!(s.remove(1).unwrap().id, 1);
        assert!(s.is_empty());
        assert!(s.take(&c[0]).is_none());
    }

    #[test]
    fn deadlines_expire() {
        let mut s = Scheduler::new(0);
        let now = Instant::now();
        s.enqueue(item(1, "a", TrafficClass::Provisioned, 1.0, 1.0, now));
        s.enqueue(item(2, "a", TrafficClass::Provisioned, 1.0, 1.0, later()));
        let gone = s.expire(now + Duration::from_millis(1));
        assert_eq!(gone.len(), 1);
        assert_eq!(gone[0].id, 1);
        assert_eq!(s.len_of(TrafficClass::Provisioned), 1);
    }
}
