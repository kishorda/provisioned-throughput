//! One agent session's calls (docs/09 §3): how long the chain took, how much context was
//! reused from cache, and whether any call was throttled.

use pt_core::{Outcome, TrafficClass};
use serde::Serialize;

use crate::sla::rfc3339;
use crate::stats::Percentiles;
use crate::store::StoredRecord;
use crate::usage::ClassCounts;

/// Calls listed in a report.
const MAX_CALLS: usize = 500;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SessionCall {
    pub at: String,
    pub request_id: String,
    pub class: Option<TrafficClass>,
    pub outcome: Outcome,
    pub ttft_ms: Option<f64>,
    pub total_ms: f64,
    pub input_tokens: u64,
    pub cached_tokens: u64,
    pub output_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SessionReport {
    pub session_id: String,
    pub calls: usize,
    pub first_at: String,
    pub last_at: String,
    /// Sum of gateway-measured request durations.
    pub total_ms: f64,
    pub ttft_ms: Option<Percentiles>,
    pub prompt_tokens: u64,
    pub cached_prompt_tokens: u64,
    /// Share of prompt tokens served from cache across the session.
    pub cache_reuse: Option<f64>,
    pub output_tokens: u64,
    pub throttled: u64,
    pub classes: ClassCounts,
    pub events: Vec<SessionCall>,
}

pub fn report(session_id: &str, records: &[StoredRecord]) -> Option<SessionReport> {
    let calls: Vec<&StoredRecord> = records
        .iter()
        .filter(|r| r.record.session_id.as_deref() == Some(session_id))
        .collect();
    let (first, last) = (calls.first()?, calls.last()?);
    let mut classes = ClassCounts::default();
    let mut prompt = 0;
    let mut cached = 0;
    let mut output = 0;
    let mut throttled = 0;
    let mut total_ms = 0.0;
    let mut ttft = Vec::new();
    for c in &calls {
        let r = &c.record;
        match r.class {
            Some(TrafficClass::Provisioned) => classes.provisioned += 1,
            Some(TrafficClass::Burst) => classes.burst += 1,
            Some(_) => classes.spillover += 1,
            None => {}
        }
        if matches!(r.outcome, Outcome::Rejected(_)) {
            throttled += 1;
        }
        prompt += r.tokens.uncached_prefill + r.tokens.cached_prefill;
        cached += r.tokens.cached_prefill;
        output += r.tokens.decode;
        total_ms += r.timings.total_ms;
        ttft.extend(r.timings.ttft_ms);
    }
    Some(SessionReport {
        session_id: session_id.to_string(),
        calls: calls.len(),
        first_at: rfc3339(first.at_ms),
        last_at: rfc3339(last.at_ms),
        total_ms,
        ttft_ms: Percentiles::of(ttft),
        prompt_tokens: prompt,
        cached_prompt_tokens: cached,
        cache_reuse: (prompt > 0).then(|| cached as f64 / prompt as f64),
        output_tokens: output,
        throttled,
        classes,
        events: calls
            .iter()
            .take(MAX_CALLS)
            .map(|c| SessionCall {
                at: rfc3339(c.at_ms),
                request_id: c.record.request_id.to_string(),
                class: c.record.class,
                outcome: c.record.outcome.clone(),
                ttft_ms: c.record.timings.ttft_ms,
                total_ms: c.record.timings.total_ms,
                input_tokens: c.record.tokens.uncached_prefill + c.record.tokens.cached_prefill,
                cached_tokens: c.record.tokens.cached_prefill,
                output_tokens: c.record.tokens.decode,
            })
            .collect(),
    })
}
