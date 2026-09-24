//! Monthly SLA attainment and service credits (docs/09 §4).
//!
//! - **Eligible:** in-shape, class `provisioned`, and either completed or cancelled after
//!   the first token. Everything else is excluded and counted.
//! - **Windows:** 5 minutes. A window with fewer than 100 eligible requests is merged with
//!   the next ones until it has at least 100; a short tail joins the previous window.
//! - **Met:** the window's p95 TTFT and p95 TPOT are within target. Targets depend on each
//!   request (input length, cache hits, output length), so latency is divided by the
//!   request's own target and p95 of that ratio must be at most 1.
//! - **Attainment:** met windows ÷ windows. Commitment 99.8%.
//! - **Credits:** 10 / 20 / 30 / 50% of the monthly fee below 99.8 / 99.7 / 99.6 / 99.5%.

use std::collections::BTreeMap;

use pt_core::{Outcome, Tier, TrafficClass, UsageRecord};
use serde::Serialize;

use crate::directory::ReservationInfo;
use crate::stats::percentile;
use crate::store::StoredRecord;

pub const WINDOW_MS: u64 = 5 * 60 * 1_000;
pub const MIN_WINDOW_REQUESTS: usize = 100;
pub const TARGET_ATTAINMENT_PCT: f64 = 99.8;
/// Missed windows listed in a report.
const MAX_MISSED_LISTED: usize = 100;

pub fn is_eligible(r: &UsageRecord) -> bool {
    r.in_shape
        && r.class == Some(TrafficClass::Provisioned)
        && match r.outcome {
            Outcome::Ok => true,
            Outcome::ClientCancelled => r.timings.ttft_ms.is_some(),
            _ => false,
        }
}

fn ttft_ratio(tier: Tier, r: &UsageRecord) -> Option<f64> {
    let input = r.tokens.uncached_prefill + r.tokens.cached_prefill;
    Some(r.timings.ttft_ms? / tier.ttft_target_ms(input, r.tokens.cached_prefill))
}

fn tpot_ratio(tier: Tier, r: &UsageRecord) -> Option<f64> {
    Some(r.timings.tpot_ms? / tier.tpot_target_ms(r.tokens.decode))
}

/// Credit as a percentage of the monthly fee.
pub fn credit_pct(attainment_pct: f64) -> u32 {
    match attainment_pct {
        a if a >= 99.8 => 0,
        a if a >= 99.7 => 10,
        a if a >= 99.6 => 20,
        a if a >= 99.5 => 30,
        _ => 50,
    }
}

#[derive(Debug, Clone, Default)]
struct Window {
    start_ms: u64,
    end_ms: u64,
    ttft: Vec<f64>,
    tpot: Vec<f64>,
    requests: usize,
}

impl Window {
    fn absorb(&mut self, other: Window) {
        self.start_ms = self.start_ms.min(other.start_ms);
        self.end_ms = self.end_ms.max(other.end_ms);
        self.ttft.extend(other.ttft);
        self.tpot.extend(other.tpot);
        self.requests += other.requests;
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WindowResult {
    pub start: String,
    pub end: String,
    pub requests: usize,
    /// p95 of latency ÷ target. At most 1.0 means within target.
    pub ttft_p95_ratio: Option<f64>,
    pub tpot_p95_ratio: Option<f64>,
    pub met: bool,
}

/// Evaluate SLA windows for eligible records.
pub fn windows(tier: Tier, records: &[StoredRecord]) -> Vec<WindowResult> {
    let mut raw: BTreeMap<u64, Window> = BTreeMap::new();
    for s in records.iter().filter(|s| is_eligible(&s.record)) {
        let idx = s.at_ms / WINDOW_MS;
        let w = raw.entry(idx).or_insert_with(|| Window {
            start_ms: idx * WINDOW_MS,
            end_ms: (idx + 1) * WINDOW_MS,
            ..Default::default()
        });
        w.requests += 1;
        if let Some(x) = ttft_ratio(tier, &s.record) {
            w.ttft.push(x);
        }
        if let Some(x) = tpot_ratio(tier, &s.record) {
            w.tpot.push(x);
        }
    }

    // Merge sparse windows forward until each has enough requests.
    let mut merged: Vec<Window> = Vec::new();
    let mut current: Option<Window> = None;
    for w in raw.into_values() {
        let c = current.get_or_insert_with(|| Window {
            start_ms: w.start_ms,
            ..Default::default()
        });
        c.absorb(w);
        if c.requests >= MIN_WINDOW_REQUESTS {
            merged.push(current.take().expect("just set"));
        }
    }
    if let Some(tail) = current {
        match merged.last_mut() {
            Some(last) => last.absorb(tail),
            None => merged.push(tail),
        }
    }

    merged
        .into_iter()
        .map(|mut w| {
            w.ttft.sort_by(f64::total_cmp);
            w.tpot.sort_by(f64::total_cmp);
            let ttft = percentile(&w.ttft, 0.95);
            let tpot = percentile(&w.tpot, 0.95);
            WindowResult {
                start: rfc3339(w.start_ms),
                end: rfc3339(w.end_ms),
                requests: w.requests,
                met: ttft.is_none_or(|x| x <= 1.0) && tpot.is_none_or(|x| x <= 1.0),
                ttft_p95_ratio: ttft,
                tpot_p95_ratio: tpot,
            }
        })
        .collect()
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Exclusions {
    pub out_of_shape: u64,
    /// Burst and spillover traffic, which is above the entitlement.
    pub over_entitlement: u64,
    pub rejected: u64,
    pub errors: u64,
    pub cancelled_before_first_token: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SlaReport {
    pub reservation: String,
    pub month: String,
    pub period_start: String,
    pub period_end: String,
    /// False while the month is still running.
    pub complete: bool,
    pub target_attainment_pct: f64,
    pub windows: usize,
    pub windows_met: usize,
    pub attainment_pct: f64,
    pub credit_pct: u32,
    pub credit_amount: u64,
    pub monthly_price: u64,
    pub currency: String,
    pub eligible_requests: usize,
    pub excluded: Exclusions,
    pub missed_windows: Vec<WindowResult>,
}

pub fn report(
    info: &ReservationInfo,
    records: &[StoredRecord],
    month: &str,
    period: (u64, u64),
    complete: bool,
) -> SlaReport {
    let mut excluded = Exclusions::default();
    for s in records {
        let r = &s.record;
        if is_eligible(r) {
            continue;
        }
        match (&r.outcome, r.class) {
            (Outcome::Rejected(_), _) => excluded.rejected += 1,
            (Outcome::Error(_), _) => excluded.errors += 1,
            (_, Some(TrafficClass::Burst | TrafficClass::Spillover | TrafficClass::Payg)) => {
                excluded.over_entitlement += 1
            }
            (_, _) if !r.in_shape => excluded.out_of_shape += 1,
            (Outcome::ClientCancelled, _) => excluded.cancelled_before_first_token += 1,
            _ => {}
        }
    }

    let windows = windows(info.tier, records);
    let met = windows.iter().filter(|w| w.met).count();
    let attainment_pct = if windows.is_empty() {
        100.0
    } else {
        100.0 * met as f64 / windows.len() as f64
    };
    let credit_pct = credit_pct(attainment_pct);
    SlaReport {
        reservation: info.id.clone(),
        month: month.to_string(),
        period_start: rfc3339(period.0),
        period_end: rfc3339(period.1),
        complete,
        target_attainment_pct: TARGET_ATTAINMENT_PCT,
        eligible_requests: windows.iter().map(|w| w.requests).sum(),
        windows: windows.len(),
        windows_met: met,
        attainment_pct,
        credit_pct,
        credit_amount: info.monthly_price * u64::from(credit_pct) / 100,
        monthly_price: info.monthly_price,
        currency: info.currency.clone(),
        excluded,
        missed_windows: windows
            .into_iter()
            .filter(|w| !w.met)
            .take(MAX_MISSED_LISTED)
            .collect(),
    }
}

/// `"2026-10"` → the month's UTC bounds in Unix milliseconds.
pub fn month_bounds(month: &str) -> Option<(u64, u64)> {
    let (y, m) = month.split_once('-')?;
    let (y, m): (i16, i8) = (y.parse().ok()?, m.parse().ok()?);
    let start = jiff::civil::Date::new(y, m, 1).ok()?;
    let next = start.checked_add(jiff::Span::new().months(1)).ok()?;
    let ms = |d: jiff::civil::Date| {
        d.to_zoned(jiff::tz::TimeZone::UTC)
            .ok()
            .map(|z| z.timestamp().as_millisecond().max(0) as u64)
    };
    Some((ms(start)?, ms(next)?))
}

pub fn month_of(ms: u64) -> String {
    let ts = jiff::Timestamp::from_millisecond(ms as i64).unwrap_or(jiff::Timestamp::UNIX_EPOCH);
    let d = ts.to_zoned(jiff::tz::TimeZone::UTC).date();
    format!("{:04}-{:02}", d.year(), d.month())
}

pub fn rfc3339(ms: u64) -> String {
    jiff::Timestamp::from_millisecond(ms as i64)
        .unwrap_or(jiff::Timestamp::UNIX_EPOCH)
        .to_string()
}

/// Record builders shared by this crate's unit tests.
#[cfg(test)]
pub(crate) mod tests_support {
    use pt_core::{Outcome, Timings, TokenBreakdown, TrafficClass, UsageRecord};

    use crate::store::StoredRecord;

    /// A completed, in-shape provisioned request: 1,000 prompt tokens, 200 output tokens,
    /// 100 WU, TTFT 200 ms, TPOT 20 ms.
    pub fn record(at_ms: u64) -> StoredRecord {
        record_with(at_ms, 200.0, Some(20.0))
    }

    pub fn record_with(at_ms: u64, ttft_ms: f64, tpot_ms: Option<f64>) -> StoredRecord {
        StoredRecord {
            region: "eu-west".into(),
            at_ms,
            record: UsageRecord {
                request_id: uuid::Uuid::new_v4(),
                received_at_ms: at_ms,
                tenant: "acme".into(),
                reservation: "pt-1".into(),
                deployment: "dep-1".into(),
                class: Some(TrafficClass::Provisioned),
                session_id: None,
                tokens: TokenBreakdown {
                    uncached_prefill: 1_000,
                    cached_prefill: 0,
                    decode: 200,
                },
                kv_token_seconds: 0.0,
                wu_estimated: 100.0,
                wu_actual: 100.0,
                timings: Timings {
                    queue_ms: 0.0,
                    ttft_ms: Some(ttft_ms),
                    total_ms: ttft_ms,
                    tpot_ms,
                },
                in_shape: true,
                outcome: Outcome::Ok,
                profile: "p".into(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::tests_support::record_with as record;
    use super::*;
    use pt_core::{RejectReason, Shape};

    fn info() -> ReservationInfo {
        ReservationInfo {
            id: "pt-1".into(),
            tenant: "acme".into(),
            tier: Tier::Interactive, // TTFT 800 ms, TPOT 40 ms for 200 output tokens
            cus: 10,
            entitlement_wu_s: 10_000.0,
            monthly_price: 1_000_000,
            currency: "USD".into(),
            shape: Shape {
                input_p95: 2_000,
                input_max: 8_000,
                output_p95: 500,
                context_ceiling: 16_000,
                cache_hit_ratio: 0.0,
                burst_factor: 1.0,
            },
        }
    }

    /// `n` requests spread over the 5-minute window starting at `start`.
    fn window(start: u64, n: usize, slow: usize) -> Vec<StoredRecord> {
        (0..n)
            .map(|i| {
                let ttft = if i < slow { 5_000.0 } else { 200.0 };
                record(start + i as u64 * 1_000, ttft, Some(20.0))
            })
            .collect()
    }

    #[test]
    fn credit_schedule() {
        assert_eq!(credit_pct(100.0), 0);
        assert_eq!(credit_pct(99.8), 0);
        assert_eq!(credit_pct(99.75), 10);
        assert_eq!(credit_pct(99.65), 20);
        assert_eq!(credit_pct(99.55), 30);
        assert_eq!(credit_pct(99.5), 30);
        assert_eq!(credit_pct(99.4), 50);
    }

    #[test]
    fn window_met_at_p95() {
        // 100 requests, 5 slow: p95 is still fast. 6 slow: p95 is slow.
        assert!(windows(Tier::Interactive, &window(0, 100, 5))[0].met);
        let w = &windows(Tier::Interactive, &window(0, 100, 6))[0];
        assert!(!w.met);
        assert!(w.ttft_p95_ratio.unwrap() > 1.0);
    }

    #[test]
    fn tpot_counts_too() {
        let mut recs = window(0, 100, 0);
        for r in recs.iter_mut().take(10) {
            r.record.timings.tpot_ms = Some(90.0); // target 40
        }
        assert!(!windows(Tier::Interactive, &recs)[0].met);
    }

    #[test]
    fn sparse_windows_merge_until_100() {
        // Four windows of 30 requests: the first three merge to 90, still short, then the
        // fourth brings it to 120.
        let mut recs = Vec::new();
        for w in 0..4 {
            recs.extend(window(w * WINDOW_MS, 30, 0));
        }
        let ws = windows(Tier::Interactive, &recs);
        assert_eq!(ws.len(), 1);
        assert_eq!(ws[0].requests, 120);

        // 150 + 20: the short tail joins the previous window.
        let mut recs = window(0, 150, 0);
        recs.extend(window(WINDOW_MS, 20, 0));
        let ws = windows(Tier::Interactive, &recs);
        assert_eq!(ws.len(), 1);
        assert_eq!(ws[0].requests, 170);
    }

    #[test]
    fn ineligible_requests_are_excluded_and_counted() {
        let mut recs = window(0, 100, 0);
        let mut burst = record(10, 9_000.0, None);
        burst.record.class = Some(TrafficClass::Burst);
        let mut out = record(10, 9_000.0, None);
        out.record.in_shape = false;
        let mut rejected = record(10, 0.0, None);
        rejected.record.class = None;
        rejected.record.outcome = Outcome::Rejected(RejectReason::EntitlementExhausted);
        let mut early_cancel = record(10, 0.0, None);
        early_cancel.record.outcome = Outcome::ClientCancelled;
        early_cancel.record.timings.ttft_ms = None;
        recs.extend([burst, out, rejected, early_cancel]);

        let r = report(&info(), &recs, "1970-01", (0, WINDOW_MS), true);
        assert_eq!(r.eligible_requests, 100);
        assert_eq!(
            r.excluded,
            Exclusions {
                out_of_shape: 1,
                over_entitlement: 1,
                rejected: 1,
                errors: 0,
                cancelled_before_first_token: 1
            }
        );
        assert_eq!(r.attainment_pct, 100.0);
        assert_eq!(r.credit_pct, 0);
    }

    #[test]
    fn attainment_and_credit() {
        // 1,000 windows, 3 missed: 99.7% → 10% credit.
        let mut recs = Vec::new();
        for w in 0..1_000u64 {
            recs.extend(window(w * WINDOW_MS, 100, if w < 3 { 10 } else { 0 }));
        }
        let r = report(&info(), &recs, "1970-01", (0, 1_000 * WINDOW_MS), true);
        assert_eq!((r.windows, r.windows_met), (1_000, 997));
        assert!((r.attainment_pct - 99.7).abs() < 1e-9);
        assert_eq!(r.credit_pct, 10);
        assert_eq!(r.credit_amount, 100_000);
        assert_eq!(r.missed_windows.len(), 3);
    }

    #[test]
    fn no_traffic_is_full_attainment() {
        let r = report(&info(), &[], "2026-10", (0, 1), false);
        assert_eq!((r.windows, r.attainment_pct, r.credit_pct), (0, 100.0, 0));
    }

    #[test]
    fn months() {
        let (s, e) = month_bounds("2026-10").unwrap();
        assert_eq!(rfc3339(s), "2026-10-01T00:00:00Z");
        assert_eq!(rfc3339(e), "2026-11-01T00:00:00Z");
        let (_, e) = month_bounds("2026-12").unwrap();
        assert_eq!(rfc3339(e), "2027-01-01T00:00:00Z");
        assert!(month_bounds("2026-13").is_none());
        assert!(month_bounds("oct").is_none());
        assert_eq!(month_of(s + 1), "2026-10");
    }
}
