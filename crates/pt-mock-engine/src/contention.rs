//! A simulated continuous-batching engine, so tenants really interfere (docs/05 §7).
//!
//! One scheduler task runs the batch in steps, like vLLM or TensorRT-LLM:
//!
//! - **Admission.** Waiting sequences join the batch while it has room (`max_batch`) and
//!   their prompt fits in the KV cache (`kv_capacity_tokens`), first come first served.
//! - **A step** costs `step_base + step_per_seq × decoding sequences + prefill time`. Every
//!   decoding sequence gets one token per step, so TPOT grows with the batch.
//! - **Prefill.** Without chunking, a new sequence's whole prompt is processed in one step,
//!   and every other sequence waits for it: a long prompt stalls everyone's decode. With
//!   `prefill_chunk`, at most that many prompt tokens are processed per step.
//! - **KV overflow.** Decoding grows each sequence's KV. When the cache overflows, the most
//!   recently admitted sequence is evicted and must recompute its prompt and output so far
//!   before it continues (vLLM's recompute preemption).
//!
//! Real engines are far more complex; this model keeps the three interference paths the
//! suite tests: batch size, prefill stalls, and KV pressure.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq)]
pub struct Contention {
    /// Sequences in the batch at once.
    pub max_batch: usize,
    /// Cost of a decode step with an empty batch.
    pub step_base: Duration,
    /// Extra cost per decoding sequence in the step.
    pub step_per_seq: Duration,
    /// Prefill throughput.
    pub prefill_tokens_per_ms: f64,
    /// Chunked prefill: at most this many prompt tokens per step. `None` processes whole
    /// prompts in one step.
    pub prefill_chunk: Option<u64>,
    /// KV cache size in tokens.
    pub kv_capacity_tokens: u64,
}

impl Default for Contention {
    fn default() -> Self {
        Self {
            max_batch: 256,
            step_base: Duration::from_millis(5),
            step_per_seq: Duration::from_micros(500),
            prefill_tokens_per_ms: 100.0,
            prefill_chunk: Some(512),
            kv_capacity_tokens: 64_000,
        }
    }
}

struct Submission {
    kv_prompt: u64,
    prefill: u64,
    output: u64,
    tokens: mpsc::UnboundedSender<()>,
}

struct Seq {
    /// Prompt tokens held in KV.
    prompt: u64,
    /// Prefill work left: the uncached prompt, or prompt + output after an eviction.
    prefill_left: u64,
    generated: u64,
    output: u64,
    tokens: mpsc::UnboundedSender<()>,
    admitted: u64,
}

impl Seq {
    fn kv(&self) -> u64 {
        self.prompt + self.generated
    }
}

/// Handle to the scheduler task.
#[derive(Clone)]
pub struct Batcher {
    tx: mpsc::UnboundedSender<Submission>,
    preemptions: Arc<AtomicU64>,
}

impl Batcher {
    /// Start the scheduler task. Must be called inside a Tokio runtime.
    pub fn start(config: Contention) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let preemptions = Arc::new(AtomicU64::new(0));
        tokio::spawn(run(config, rx, preemptions.clone()));
        Self { tx, preemptions }
    }

    /// Queue a request. The receiver yields one `()` per output token and closes when the
    /// output is complete. `kv_prompt` is the whole prompt; `prefill` is the uncached part
    /// that must be computed.
    pub fn submit(&self, kv_prompt: u64, prefill: u64, output: u64) -> mpsc::UnboundedReceiver<()> {
        let (tokens, rx) = mpsc::unbounded_channel();
        let _ = self.tx.send(Submission {
            kv_prompt,
            prefill,
            output: output.max(1),
            tokens,
        });
        rx
    }

    /// Sequences evicted for lack of KV and recomputed so far.
    pub fn preemptions(&self) -> u64 {
        self.preemptions.load(Ordering::Relaxed)
    }
}

async fn run(
    config: Contention,
    mut arrivals: mpsc::UnboundedReceiver<Submission>,
    preemptions: Arc<AtomicU64>,
) {
    let mut waiting: VecDeque<Seq> = VecDeque::new();
    let mut running: Vec<Seq> = Vec::new();
    let mut admitted = 0u64;
    let accept = |s: Submission| Seq {
        prompt: s.kv_prompt,
        prefill_left: s.prefill.max(1),
        generated: 0,
        output: s.output,
        tokens: s.tokens,
        admitted: 0,
    };
    loop {
        // Take arrivals; sleep until one comes when idle.
        if running.is_empty() && waiting.is_empty() {
            match arrivals.recv().await {
                Some(s) => waiting.push_back(accept(s)),
                None => return,
            }
        }
        while let Ok(s) = arrivals.try_recv() {
            waiting.push_back(accept(s));
        }

        // Admit in arrival order while the batch and the KV cache have room.
        let mut kv_used: u64 = running.iter().map(Seq::kv).sum();
        while running.len() < config.max_batch {
            let Some(next) = waiting.front() else { break };
            let fits = kv_used + next.kv() <= config.kv_capacity_tokens;
            if !fits && !running.is_empty() {
                break;
            }
            let mut seq = waiting.pop_front().expect("peeked");
            admitted += 1;
            seq.admitted = admitted;
            kv_used += seq.kv();
            running.push(seq);
        }
        if running.is_empty() {
            continue;
        }

        // One step: prefill work, then a token for every sequence that can decode.
        let decoding = running.iter().filter(|s| s.prefill_left == 0).count();
        let mut budget = config.prefill_chunk.unwrap_or(u64::MAX);
        let mut prefilled = 0u64;
        for s in running.iter_mut() {
            if s.prefill_left == 0 || budget == 0 {
                continue;
            }
            let take = s.prefill_left.min(budget);
            s.prefill_left -= take;
            budget = budget.saturating_sub(take);
            prefilled += take;
        }
        let step = config.step_base
            + config.step_per_seq * decoding as u32
            + Duration::from_secs_f64(prefilled as f64 / config.prefill_tokens_per_ms / 1_000.0);
        tokio::time::sleep(step).await;

        // Emit tokens: decoding sequences, and those whose prefill just finished (their
        // first token comes out of the prefill step).
        for s in running.iter_mut() {
            if s.prefill_left == 0 {
                s.generated += 1;
                if s.tokens.send(()).is_err() {
                    s.generated = s.output; // client gone: drop it
                }
            }
        }
        running.retain(|s| s.generated < s.output);

        // KV overflow: evict the newest sequences until the rest fit; they recompute.
        let mut kv_used: u64 = running.iter().map(Seq::kv).sum();
        while kv_used > config.kv_capacity_tokens && running.len() > 1 {
            let newest = running
                .iter()
                .enumerate()
                .max_by_key(|(_, s)| s.admitted)
                .map(|(i, _)| i)
                .expect("non-empty");
            let mut seq = running.swap_remove(newest);
            kv_used -= seq.kv();
            seq.prefill_left = seq.kv();
            preemptions.fetch_add(1, Ordering::Relaxed);
            waiting.push_front(seq);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    async fn drain(mut rx: mpsc::UnboundedReceiver<()>) -> (Duration, u64) {
        let start = Instant::now();
        let mut n = 0;
        let mut first = None;
        while rx.recv().await.is_some() {
            n += 1;
            first.get_or_insert(start.elapsed());
        }
        (first.unwrap_or_default(), n)
    }

    #[tokio::test]
    async fn every_token_arrives_and_batching_slows_steps() {
        let b = Batcher::start(Contention {
            step_base: Duration::from_millis(2),
            step_per_seq: Duration::from_millis(1),
            ..Default::default()
        });
        let (_, n) = drain(b.submit(10, 10, 5)).await;
        assert_eq!(n, 5);
        // One alone: ~3 ms per step. Eight together: ~10 ms per step.
        let t = Instant::now();
        drain(b.submit(10, 10, 10)).await;
        let alone = t.elapsed();
        let t = Instant::now();
        let all: Vec<_> = (0..8).map(|_| b.submit(10, 10, 10)).collect();
        futures::future::join_all(all.into_iter().map(drain)).await;
        assert!(t.elapsed() > alone * 2, "{:?} vs {:?}", t.elapsed(), alone);
    }

    #[tokio::test]
    async fn kv_overflow_evicts_and_recomputes() {
        let b = Batcher::start(Contention {
            kv_capacity_tokens: 1_000,
            step_base: Duration::from_millis(1),
            ..Default::default()
        });
        // Two sequences that fit at admission but outgrow the cache while decoding.
        let x = b.submit(400, 400, 150);
        let y = b.submit(400, 400, 150);
        let (a, b2) = tokio::join!(drain(x), drain(y));
        assert_eq!((a.1, b2.1), (150, 150), "all tokens still arrive");
        assert!(b.preemptions() >= 1);
    }
}
