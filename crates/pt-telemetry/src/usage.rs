//! Usage time series and summary (docs/09 §3).

use std::collections::BTreeMap;

use pt_core::{Outcome, Shape, TrafficClass};
use serde::Serialize;

use crate::directory::ReservationInfo;
use crate::sla::rfc3339;
use crate::stats::{percentile, Percentiles};
use crate::store::StoredRecord;

pub const MAX_BUCKETS: u64 = 2_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Granularity {
    Minute,
    FiveMinutes,
    Hour,
    Day,
}

impl Granularity {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "1m" => Self::Minute,
            "5m" => Self::FiveMinutes,
            "1h" => Self::Hour,
            "1d" => Self::Day,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Minute => "1m",
            Self::FiveMinutes => "5m",
            Self::Hour => "1h",
            Self::Day => "1d",
        }
    }

    pub fn millis(self) -> u64 {
        match self {
            Self::Minute => 60_000,
            Self::FiveMinutes => 300_000,
            Self::Hour => 3_600_000,
            Self::Day => 86_400_000,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ClassCounts {
    pub provisioned: u64,
    pub burst: u64,
    pub spillover: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct TokenTotals {
    pub uncached_prefill: u64,
    pub cached_prefill: u64,
    pub decode: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Stats {
    /// Served requests by class.
    pub requests: ClassCounts,
    /// Rejected requests by reason code (`entitlement_exhausted`, `queue_deadline`, ...).
    pub rejected: BTreeMap<String, u64>,
    pub errors: u64,
    pub cancelled: u64,
    pub out_of_shape: u64,
    pub tokens: TokenTotals,
    pub wu_actual: f64,
    /// WU of provisioned-class traffic, the part that counts against the entitlement.
    pub wu_provisioned: f64,
    /// `wu_provisioned ÷ (entitlement × seconds)`.
    pub utilisation: f64,
    /// Gateway-measured, completed in-shape provisioned requests.
    pub ttft_ms: Option<Percentiles>,
    pub tpot_ms: Option<Percentiles>,
    /// Share of prompt tokens served from the prefix cache.
    pub cache_hit_rate: Option<f64>,
}

fn stats(records: &[&StoredRecord], entitlement_wu_s: f64, millis: u64) -> Stats {
    let mut s = Stats::default();
    let mut ttft = Vec::new();
    let mut tpot = Vec::new();
    for rec in records {
        let r = &rec.record;
        match r.class {
            Some(TrafficClass::Provisioned) => s.requests.provisioned += 1,
            Some(TrafficClass::Burst) => s.requests.burst += 1,
            Some(TrafficClass::Spillover | TrafficClass::Payg) => s.requests.spillover += 1,
            None => {}
        }
        match &r.outcome {
            Outcome::Rejected(reason) => {
                *s.rejected.entry(reason.as_str().to_string()).or_default() += 1
            }
            Outcome::Error(_) => s.errors += 1,
            Outcome::ClientCancelled => s.cancelled += 1,
            Outcome::Ok => {}
        }
        if r.class.is_some() && !r.in_shape {
            s.out_of_shape += 1;
        }
        s.tokens.uncached_prefill += r.tokens.uncached_prefill;
        s.tokens.cached_prefill += r.tokens.cached_prefill;
        s.tokens.decode += r.tokens.decode;
        s.wu_actual += r.wu_actual;
        if r.class == Some(TrafficClass::Provisioned) {
            s.wu_provisioned += r.wu_actual;
            if r.in_shape && r.outcome == Outcome::Ok {
                ttft.extend(r.timings.ttft_ms);
                tpot.extend(r.timings.tpot_ms);
            }
        }
    }
    let capacity = entitlement_wu_s * millis as f64 / 1_000.0;
    s.utilisation = if capacity > 0.0 {
        s.wu_provisioned / capacity
    } else {
        0.0
    };
    s.ttft_ms = Percentiles::of(ttft);
    s.tpot_ms = Percentiles::of(tpot);
    let prompt = s.tokens.uncached_prefill + s.tokens.cached_prefill;
    s.cache_hit_rate = (prompt > 0).then(|| s.tokens.cached_prefill as f64 / prompt as f64);
    s
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Bucket {
    pub start: String,
    #[serde(flatten)]
    pub stats: Stats,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ShapeReport {
    pub declared: Shape,
    pub observed_input_p95: Option<u64>,
    pub observed_input_max: Option<u64>,
    pub observed_output_p95: Option<u64>,
    /// Share of served requests inside the declared shape.
    pub in_shape_fraction: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UsageReport {
    pub reservation: String,
    /// Set when the report is filtered to one deployment. Utilisation is still measured
    /// against the whole reservation's entitlement.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment: Option<String>,
    pub from: String,
    pub to: String,
    pub granularity: &'static str,
    pub entitlement_wu_per_s: f64,
    pub summary: Stats,
    pub shape: ShapeReport,
    /// Plain-language suggestions based on the period.
    pub advice: Vec<String>,
    pub series: Vec<Bucket>,
}

pub fn report(
    info: &ReservationInfo,
    records: &[StoredRecord],
    from_ms: u64,
    to_ms: u64,
    granularity: Granularity,
) -> UsageReport {
    let step = granularity.millis();
    let first = from_ms / step * step;
    let mut series = Vec::new();
    let mut i = 0;
    let mut start = first;
    while start < to_ms {
        let end = start + step;
        let mut in_bucket = Vec::new();
        while i < records.len() && records[i].at_ms < end {
            if records[i].at_ms >= start {
                in_bucket.push(&records[i]);
            }
            i += 1;
        }
        series.push(Bucket {
            start: rfc3339(start),
            stats: stats(&in_bucket, info.entitlement_wu_s, step),
        });
        start = end;
    }

    let all: Vec<&StoredRecord> = records.iter().collect();
    let summary = stats(&all, info.entitlement_wu_s, to_ms.saturating_sub(from_ms));
    let shape = shape_report(info, records);
    let advice = advice(info, &summary, &shape);
    UsageReport {
        reservation: info.id.clone(),
        deployment: None,
        from: rfc3339(from_ms),
        to: rfc3339(to_ms),
        granularity: granularity.as_str(),
        entitlement_wu_per_s: info.entitlement_wu_s,
        summary,
        shape,
        advice,
        series,
    }
}

fn shape_report(info: &ReservationInfo, records: &[StoredRecord]) -> ShapeReport {
    let served: Vec<_> = records
        .iter()
        .filter(|r| r.record.class.is_some())
        .collect();
    let mut input: Vec<f64> = served
        .iter()
        .map(|r| (r.record.tokens.uncached_prefill + r.record.tokens.cached_prefill) as f64)
        .collect();
    let mut output: Vec<f64> = served
        .iter()
        .map(|r| r.record.tokens.decode as f64)
        .collect();
    input.sort_by(f64::total_cmp);
    output.sort_by(f64::total_cmp);
    let in_shape = served.iter().filter(|r| r.record.in_shape).count();
    ShapeReport {
        declared: info.shape,
        observed_input_p95: percentile(&input, 0.95).map(|v| v as u64),
        observed_input_max: input.last().map(|v| *v as u64),
        observed_output_p95: percentile(&output, 0.95).map(|v| v as u64),
        in_shape_fraction: (!served.is_empty()).then(|| in_shape as f64 / served.len() as f64),
    }
}

fn advice(info: &ReservationInfo, s: &Stats, shape: &ShapeReport) -> Vec<String> {
    let mut out = Vec::new();
    let path = format!("/v1/provisioned-throughput/{}", info.id);
    if let Some(f) = shape.in_shape_fraction.filter(|f| *f < 0.99) {
        out.push(format!(
            "{:.1}% of requests were outside the declared shape, so they don't count towards the SLA. Update the shape with PATCH {path}.",
            100.0 * (1.0 - f)
        ));
    }
    if let Some(p) = shape
        .observed_input_p95
        .filter(|p| *p > info.shape.input_p95)
    {
        out.push(format!(
            "Observed input p95 is {p} tokens, above the declared {}.",
            info.shape.input_p95
        ));
    }
    if let Some(p) = shape
        .observed_output_p95
        .filter(|p| *p > info.shape.output_p95)
    {
        out.push(format!(
            "Observed output p95 is {p} tokens, above the declared {}.",
            info.shape.output_p95
        ));
    }
    if let Some(&n) = s.rejected.get("entitlement_exhausted") {
        out.push(format!(
            "{n} requests were rejected because the entitlement was used up. Add CUs, or enable burst or spillover in the boundary policy."
        ));
    }
    if s.utilisation > 0.9 {
        out.push(format!(
            "Provisioned utilisation averaged {:.0}%. Consider adding CUs before traffic grows.",
            100.0 * s.utilisation
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use pt_core::{RejectReason, Tier};

    fn rec(at_ms: u64, class: Option<TrafficClass>, outcome: Outcome) -> StoredRecord {
        let mut r = crate::sla::tests_support::record(at_ms);
        r.record.class = class;
        r.record.outcome = outcome;
        r
    }

    fn info() -> ReservationInfo {
        ReservationInfo {
            id: "pt-1".into(),
            tenant: "acme".into(),
            tier: Tier::Interactive,
            cus: 1,
            entitlement_wu_s: 10.0,
            monthly_price: 0,
            currency: "USD".into(),
            exclusions: vec![],
            shape: Shape {
                input_p95: 500,
                input_max: 8_000,
                output_p95: 500,
                context_ceiling: 16_000,
                cache_hit_ratio: 0.0,
                burst_factor: 1.0,
            },
        }
    }

    #[test]
    fn buckets_cover_the_range_and_count_by_class() {
        let recs = vec![
            rec(1_000, Some(TrafficClass::Provisioned), Outcome::Ok),
            rec(2_000, Some(TrafficClass::Burst), Outcome::Ok),
            rec(
                61_000,
                None,
                Outcome::Rejected(RejectReason::EntitlementExhausted),
            ),
            rec(62_000, Some(TrafficClass::Spillover), Outcome::Ok),
        ];
        let r = report(&info(), &recs, 0, 180_000, Granularity::Minute);
        assert_eq!(r.series.len(), 3);
        assert_eq!(
            r.series[0].stats.requests,
            ClassCounts {
                provisioned: 1,
                burst: 1,
                spillover: 0
            }
        );
        assert_eq!(r.series[1].stats.rejected["entitlement_exhausted"], 1);
        assert_eq!(r.series[1].stats.requests.spillover, 1);
        assert_eq!(
            r.series[2].stats,
            Stats {
                utilisation: 0.0,
                ..Default::default()
            }
        );
        assert_eq!(r.summary.requests.provisioned, 1);
        // One provisioned request of 100 WU against 10 WU/s × 60 s.
        assert!((r.series[0].stats.utilisation - 100.0 / 600.0).abs() < 1e-9);
        assert!(r
            .advice
            .iter()
            .any(|a| a.contains("rejected because the entitlement")));
        assert!(
            r.advice.iter().any(|a| a.contains("input p95")),
            "{:?}",
            r.advice
        );
    }

    #[test]
    fn cache_hit_rate_and_latency() {
        let mut a = rec(0, Some(TrafficClass::Provisioned), Outcome::Ok);
        a.record.tokens.cached_prefill = 3_000;
        a.record.tokens.uncached_prefill = 1_000;
        let r = report(&info(), &[a], 0, 60_000, Granularity::Minute);
        assert_eq!(r.summary.cache_hit_rate, Some(0.75));
        assert_eq!(r.summary.ttft_ms.unwrap().p95, 200.0);
        assert_eq!(r.shape.observed_input_max, Some(4_000));
    }

    #[test]
    fn granularity_parsing() {
        assert_eq!(Granularity::parse("5m"), Some(Granularity::FiveMinutes));
        assert_eq!(Granularity::parse("2h"), None);
    }
}
