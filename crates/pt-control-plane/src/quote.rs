//! Quote API: how many CUs a workload needs (docs/02 §5).
//!
//! The workload comes from one of:
//! - a declared `shape` plus `requests_per_minute`;
//! - a sample `trace` of individual requests;
//! - `from_reservation`: an existing reservation's recent usage from telemetry, including
//!   rejected requests, so the quote doubles as a resize recommendation.
//!
//! For each region and tier the quote prices one request with the region's calibrated
//! profile, then sizes the reservation:
//!
//! ```text
//! sustained = busiest hour's WU ÷ its length        (or rpm × WU per request)
//! peak      = busiest 10 s's WU ÷ 10                (or sustained × burst_factor)
//! recommended CUs = ceil(sustained ÷ (WU/s per CU × 0.8))
//! peak CUs        = ceil(peak ÷ WU/s per CU)
//! ```
//!
//! When peaks need more than the recommendation, the quote suggests a burst policy that
//! covers them from banked credit, and reports the CUs needed to cover them outright.

use std::collections::{BTreeMap, HashMap};

use pt_admission::{BoundaryPolicy, BurstPolicy};
use pt_core::cost::estimate_kv_token_seconds;
use pt_core::{Outcome, PerformanceProfile, PoolIsolation, Shape, Tier, WorkBreakdown};
use pt_telemetry::UsageStore;
use serde::{Deserialize, Serialize};

use crate::clock::Clock;
use crate::model::{Price, RegionShare, Sku};
use crate::planner::CapacityPlanner;
use crate::pricing;
use crate::service::{Service, ServiceError};
use crate::store::Store;
use crate::validate;

/// Sizing target: leave 20% of the entitlement for estimation error and growth.
pub const TARGET_UTILISATION: f64 = 0.8;
/// Window for sustained demand.
pub const SUSTAINED_WINDOW_MS: u64 = 3_600_000;
/// Window for peak demand.
pub const PEAK_WINDOW_MS: u64 = 10_000;
/// Shorter traces are treated as spanning this long.
pub const MIN_TRACE_SPAN_MS: u64 = 60_000;
pub const MAX_TRACE: usize = 100_000;
const DEFAULT_LOOKBACK_DAYS: u32 = 7;

/// `POST /v1/quotes`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuoteRequest {
    /// Required unless `from_reservation` is given.
    #[serde(default)]
    pub model: Option<String>,
    /// Defaults to every tier the model offers (or the reservation's tier).
    #[serde(default)]
    pub tier: Option<Tier>,
    #[serde(default)]
    pub isolation: Option<PoolIsolation>,
    /// Prices the Multi-region surcharge. Defaults to the reservation's SKU, or Regional.
    #[serde(default)]
    pub sku: Option<Sku>,
    /// Defaults to every region offering the model (or the reservation's regions).
    #[serde(default)]
    pub regions: Option<Vec<String>>,

    /// Source 1: a declared shape and a request rate.
    #[serde(default)]
    pub shape: Option<Shape>,
    #[serde(default)]
    pub requests_per_minute: Option<f64>,
    /// Defaults to half the shape's p95.
    #[serde(default)]
    pub avg_input_tokens: Option<u64>,
    /// Defaults to half the shape's p95.
    #[serde(default)]
    pub avg_output_tokens: Option<u64>,

    /// Source 2: a sample of requests.
    #[serde(default)]
    pub trace: Option<Vec<TraceEntry>>,

    /// Source 3: an existing reservation's usage over the last `lookback_days` (default 7).
    #[serde(default)]
    pub from_reservation: Option<String>,
    #[serde(default)]
    pub lookback_days: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceEntry {
    /// Milliseconds from any fixed origin.
    pub offset_ms: u64,
    pub input_tokens: u64,
    #[serde(default)]
    pub cached_tokens: u64,
    pub output_tokens: u64,
    /// Measured WU, used instead of the cost model (reservation history).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wu: Option<f64>,
}

/// Traffic to size for.
#[derive(Debug, Clone)]
enum Workload {
    Analytic {
        rpm: f64,
        input: u64,
        cached: u64,
        output: u64,
        burst_factor: f64,
    },
    Trace(Vec<TraceEntry>),
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Demand {
    pub requests_per_minute: f64,
    pub avg_input_tokens: f64,
    pub avg_output_tokens: f64,
    pub wu_per_request: f64,
    pub sustained_wu_per_s: f64,
    pub peak_wu_per_s: f64,
}

fn request_wu(
    profile: &PerformanceProfile,
    tier: Tier,
    input: u64,
    cached: u64,
    output: u64,
) -> f64 {
    let cached = cached.min(input);
    let kv = estimate_kv_token_seconds(input, output, tier.tpot_target_s());
    profile.work_units(&WorkBreakdown::new(input - cached, cached, output, kv))
}

/// Largest per-window total, divided by the window length (capped at the span).
fn busiest(points: &[(u64, f64)], window_ms: u64, span_ms: u64) -> f64 {
    let mut buckets: HashMap<u64, f64> = HashMap::new();
    for (t, v) in points {
        *buckets.entry(t / window_ms).or_default() += v;
    }
    let max = buckets.values().copied().fold(0.0, f64::max);
    max / (window_ms.min(span_ms) as f64 / 1_000.0)
}

impl Workload {
    fn demand(&self, profile: &PerformanceProfile, tier: Tier) -> Demand {
        match self {
            Workload::Analytic {
                rpm,
                input,
                cached,
                output,
                burst_factor,
            } => {
                let wu = request_wu(profile, tier, *input, *cached, *output);
                let sustained = rpm / 60.0 * wu;
                Demand {
                    requests_per_minute: *rpm,
                    avg_input_tokens: *input as f64,
                    avg_output_tokens: *output as f64,
                    wu_per_request: wu,
                    sustained_wu_per_s: sustained,
                    peak_wu_per_s: sustained * burst_factor.max(1.0),
                }
            }
            Workload::Trace(entries) => {
                let first = entries.iter().map(|e| e.offset_ms).min().unwrap_or(0);
                let last = entries.iter().map(|e| e.offset_ms).max().unwrap_or(0);
                let span = (last - first).max(MIN_TRACE_SPAN_MS);
                let points: Vec<(u64, f64)> = entries
                    .iter()
                    .map(|e| {
                        let wu = e.wu.unwrap_or_else(|| {
                            request_wu(
                                profile,
                                tier,
                                e.input_tokens,
                                e.cached_tokens,
                                e.output_tokens,
                            )
                        });
                        (e.offset_ms - first, wu)
                    })
                    .collect();
                let counts: Vec<(u64, f64)> = points.iter().map(|(t, _)| (*t, 1.0)).collect();
                let n = entries.len() as f64;
                Demand {
                    requests_per_minute: busiest(&counts, SUSTAINED_WINDOW_MS, span) * 60.0,
                    avg_input_tokens: entries.iter().map(|e| e.input_tokens as f64).sum::<f64>()
                        / n,
                    avg_output_tokens: entries.iter().map(|e| e.output_tokens as f64).sum::<f64>()
                        / n,
                    wu_per_request: points.iter().map(|(_, w)| w).sum::<f64>() / n,
                    sustained_wu_per_s: busiest(&points, SUSTAINED_WINDOW_MS, span),
                    peak_wu_per_s: busiest(&points, PEAK_WINDOW_MS, span),
                }
            }
        }
    }

    /// A shape that fits the trace, for creating a reservation from it.
    fn observed_shape(&self, model_max_context: u64) -> Option<Shape> {
        let Workload::Trace(entries) = self else {
            return None;
        };
        let served: Vec<&TraceEntry> = entries.iter().filter(|e| e.input_tokens > 0).collect();
        if served.is_empty() {
            return None;
        }
        let p95 = |mut v: Vec<u64>| {
            v.sort_unstable();
            v[((v.len() as f64 * 0.95).ceil() as usize).clamp(1, v.len()) - 1]
        };
        let input_max = served.iter().map(|e| e.input_tokens).max().unwrap_or(0);
        let context = served
            .iter()
            .map(|e| e.input_tokens + e.output_tokens)
            .max()
            .unwrap_or(0);
        let total_input: u64 = served.iter().map(|e| e.input_tokens).sum();
        let total_cached: u64 = served
            .iter()
            .map(|e| e.cached_tokens.min(e.input_tokens))
            .sum();
        // Round the ceiling up to a power of two, within the model's limit.
        let context_ceiling = context
            .max(1_024)
            .next_power_of_two()
            .min(model_max_context);
        Some(Shape {
            input_p95: p95(served.iter().map(|e| e.input_tokens).collect()),
            input_max,
            output_p95: p95(served.iter().map(|e| e.output_tokens).collect()).max(1),
            context_ceiling,
            cache_hit_ratio: if total_input > 0 {
                total_cached as f64 / total_input as f64
            } else {
                0.0
            },
            burst_factor: 1.0, // filled in from the demand
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PerCu {
    pub requests_per_minute: f64,
    pub input_tokens_per_minute: f64,
    pub output_tokens_per_minute: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Slo {
    pub ttft_p95_ms: f64,
    pub tpot_p95_ms: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RegionQuote {
    pub region: String,
    pub tier: Tier,
    pub profile: String,
    #[serde(flatten)]
    pub demand: Demand,
    pub recommended_cus: u32,
    /// CUs that cover peaks without banked burst credit.
    pub peak_cus: u32,
    /// "1 CU ≈ …" for this workload.
    pub per_cu: PerCu,
    /// At `recommended_cus`.
    pub price: Price,
    pub slo: Slo,
    /// CUs that could be reserved now. For a reservation, includes its current CUs here.
    pub available_cus: u32,
    /// The region's pools serve the workload's context length.
    pub serves_context: bool,
    pub feasible: bool,
    /// Suggested when peaks exceed the recommended entitlement.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_boundary_policy: Option<BoundaryPolicy>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Current {
    pub reservation: String,
    pub tier: Tier,
    pub cus: u32,
    pub regions: Vec<RegionShare>,
    pub lookback_days: u32,
    pub requests_seen: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Recommendation {
    pub region: String,
    pub tier: Tier,
    pub cus: u32,
    pub monthly_price: u64,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QuoteResponse {
    pub model: String,
    /// `shape`, `trace`, or `reservation`.
    pub source: &'static str,
    pub isolation: PoolIsolation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current: Option<Current>,
    /// Shape inferred from a trace or history, ready to use when creating a reservation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_shape: Option<Shape>,
    pub quotes: Vec<RegionQuote>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recommendation: Option<Recommendation>,
}

fn invalid(field: &str, message: impl Into<String>) -> ServiceError {
    ServiceError::Validation {
        field: field.into(),
        message: message.into(),
    }
}

fn size(d: &Demand, wu_per_cu: f64) -> (u32, u32, Option<BoundaryPolicy>, Vec<String>) {
    let recommended =
        ((d.sustained_wu_per_s / (wu_per_cu * TARGET_UTILISATION)).ceil() as u32).max(1);
    let peak = ((d.peak_wu_per_s / wu_per_cu).ceil() as u32).max(1);
    let mut notes = Vec::new();
    let mut policy = None;
    if peak > recommended {
        let ratio = d.peak_wu_per_s / (f64::from(recommended) * wu_per_cu);
        if ratio <= 10.0 {
            let multiple = ((ratio * 10.0).ceil() / 10.0).max(1.1);
            policy = Some(BoundaryPolicy {
                burst: Some(BurstPolicy {
                    max_rate_multiple: multiple,
                    ..Default::default()
                }),
                ..Default::default()
            });
            notes.push(format!(
                "Peaks reach {ratio:.1}× the recommended entitlement. Burst credit can cover short peaks; {peak} CUs cover them without relying on banked credit."
            ));
        } else {
            policy = Some(BoundaryPolicy {
                spillover: true,
                ..Default::default()
            });
            notes.push(format!(
                "Peaks reach {ratio:.1}× the recommended entitlement, beyond what burst credit covers. Reserve {peak} CUs, or enable spillover to pay-as-you-go for peaks."
            ));
        }
    }
    (recommended, peak, policy, notes)
}

pub async fn quote<S, P, C, U>(
    svc: &Service<S, P, C>,
    usage: &U,
    tenant: &str,
    req: QuoteRequest,
) -> Result<QuoteResponse, ServiceError>
where
    S: Store,
    P: CapacityPlanner,
    C: Clock,
    U: UsageStore,
{
    let sources = usize::from(req.shape.is_some())
        + usize::from(req.trace.is_some())
        + usize::from(req.from_reservation.is_some());
    if sources != 1 {
        return Err(invalid(
            "source",
            "Give exactly one of shape, trace, or from_reservation.",
        ));
    }
    let config = &svc.config;

    // Resolve the model, tiers, regions, and per-region workloads.
    let mut current = None;
    let mut reservation_sku = None;
    let (model_id, workloads, default_regions, default_tier, source, shape_for_context) =
        if let Some(id) = &req.from_reservation {
            let pt = svc.get(tenant, id).await?;
            if req.model.as_ref().is_some_and(|m| *m != pt.model) {
                return Err(invalid(
                    "model",
                    "model doesn't match the reservation's model.",
                ));
            }
            let days = req.lookback_days.unwrap_or(DEFAULT_LOOKBACK_DAYS);
            if !(1..=30).contains(&days) {
                return Err(invalid(
                    "lookback_days",
                    "lookback_days must be between 1 and 30.",
                ));
            }
            let now = svc.clock.now().as_millisecond().max(0) as u64;
            let records = usage
                .range(
                    tenant,
                    id,
                    now.saturating_sub(u64::from(days) * 86_400_000),
                    now,
                )
                .await;
            let mut by_region: BTreeMap<String, Vec<TraceEntry>> = BTreeMap::new();
            for s in &records {
                let r = &s.record;
                let wu = match (&r.outcome, r.class) {
                    (Outcome::Error(_), _) => continue,
                    // Rejected requests are demand the reservation couldn't serve.
                    (_, None) => r.wu_estimated,
                    _ => r.wu_actual,
                };
                by_region
                    .entry(s.region.clone())
                    .or_default()
                    .push(TraceEntry {
                        offset_ms: s.at_ms,
                        input_tokens: r.tokens.uncached_prefill + r.tokens.cached_prefill,
                        cached_tokens: r.tokens.cached_prefill,
                        output_tokens: r.tokens.decode,
                        wu: Some(wu),
                    });
            }
            if by_region.is_empty() {
                return Err(invalid(
                    "from_reservation",
                    format!("No usage in the last {days} days to base a quote on."),
                ));
            }
            reservation_sku = Some(pt.sku);
            current = Some(Current {
                reservation: pt.id.clone(),
                tier: pt.tier,
                cus: pt.cus,
                regions: pt.regions.clone(),
                lookback_days: days,
                requests_seen: records.len(),
            });
            let workloads: HashMap<String, Workload> = by_region
                .into_iter()
                .map(|(r, e)| (r, Workload::Trace(e)))
                .collect();
            let regions = pt.regions.iter().map(|r| r.region.clone()).collect();
            (
                pt.model.clone(),
                Some(workloads),
                regions,
                Some(pt.tier),
                "reservation",
                pt.shape,
            )
        } else {
            let model_id = req.model.clone().ok_or_else(|| {
                invalid(
                    "model",
                    "model is required unless from_reservation is given.",
                )
            })?;
            let model = validate::model(config, &model_id)?;
            let regions = config
                .regions_for(&model_id)
                .into_iter()
                .map(String::from)
                .collect();
            if let Some(shape) = req.shape {
                validate::shape(model, &shape)?;
                let rpm = req
                    .requests_per_minute
                    .filter(|r| r.is_finite() && *r > 0.0)
                    .ok_or_else(|| {
                        invalid(
                            "requests_per_minute",
                            "Give a positive requests_per_minute with a shape.",
                        )
                    })?;
                let input = req.avg_input_tokens.unwrap_or((shape.input_p95 / 2).max(1));
                let output = req
                    .avg_output_tokens
                    .unwrap_or((shape.output_p95 / 2).max(1));
                let cached = (input as f64 * shape.cache_hit_ratio).round() as u64;
                let w = Workload::Analytic {
                    rpm,
                    input,
                    cached,
                    output,
                    burst_factor: shape.burst_factor,
                };
                (
                    model_id,
                    Some(HashMap::from([(String::new(), w)])),
                    regions,
                    None,
                    "shape",
                    shape,
                )
            } else {
                let trace = req.trace.clone().unwrap_or_default();
                if trace.is_empty() || trace.len() > MAX_TRACE {
                    return Err(invalid(
                        "trace",
                        format!("Give between 1 and {MAX_TRACE} trace entries."),
                    ));
                }
                if trace.iter().any(|e| e.cached_tokens > e.input_tokens) {
                    return Err(invalid("trace", "cached_tokens can't exceed input_tokens."));
                }
                let w = Workload::Trace(trace);
                let shape = w
                    .observed_shape(model.max_context)
                    .expect("non-empty trace");
                (
                    model_id,
                    Some(HashMap::from([(String::new(), w)])),
                    regions,
                    None,
                    "trace",
                    shape,
                )
            }
        };
    let workloads = workloads.expect("set above");
    let model = validate::model(config, &model_id)?.clone();

    let tiers: Vec<Tier> = match req.tier.or(default_tier) {
        Some(t) => {
            validate::tier(&model, t)?;
            vec![t]
        }
        None => model.tiers.clone(),
    };
    let regions = req.regions.clone().unwrap_or(default_regions);
    if regions.is_empty() {
        return Err(invalid("regions", "Give at least one region."));
    }
    for r in &regions {
        if config.capacity_for(r, &model_id).is_none() {
            return Err(invalid(
                "regions",
                format!("{model_id} isn't offered in {r}."),
            ));
        }
    }
    let isolation = req.isolation.unwrap_or_default();
    let sku = req.sku.or(reservation_sku).unwrap_or_default();

    // The workload for a region: the region's own history, or the one shared workload.
    let workload_for = |region: &str| workloads.get(region).or_else(|| workloads.get(""));
    let observed_shape = workloads
        .values()
        .next()
        .and_then(|w| w.observed_shape(model.max_context))
        .filter(|_| source != "shape");

    let wu_per_cu = config.telemetry.wu_per_cu;
    let base = config.pricing.base_cu_price_per_month_cents;
    let mut quotes = Vec::new();
    for region in &regions {
        let Some(workload) = workload_for(region) else {
            continue; // a reservation region with no traffic in the lookback
        };
        let capacity = config.capacity_for(region, &model_id).expect("validated");
        let profile = config.profile(&capacity.profile).expect("validated config");
        let current_here = current
            .as_ref()
            .and_then(|c| c.regions.iter().find(|r| r.region == *region))
            .map_or(0, |r| r.cus);
        let available = svc
            .planner
            .available_cus(region, &model_id)
            .await
            .unwrap_or(0)
            + current_here;
        let one = [RegionShare {
            region: region.clone(),
            cus: 1,
        }];
        let serves_context = svc
            .planner
            .check_shape(&model_id, &one, &shape_for_context)
            .await
            .is_ok();

        for &tier in &tiers {
            let demand = workload.demand(profile, tier);
            let (recommended, peak, policy, mut notes) = size(&demand, wu_per_cu);
            let feasible = serves_context && available >= recommended;
            if !serves_context {
                notes.push(format!(
                    "{region} serves contexts up to {} tokens, less than this workload needs.",
                    capacity.max_context
                ));
            } else if available < recommended {
                notes.push(format!("{region} has only {available} CUs available."));
            }
            let per_request = demand.wu_per_request.max(f64::MIN_POSITIVE);
            let rpm_per_cu = wu_per_cu * 60.0 / per_request;
            let cached_share = shape_for_context.cache_hit_ratio;
            quotes.push(RegionQuote {
                region: region.clone(),
                tier,
                profile: profile.name.clone(),
                per_cu: PerCu {
                    requests_per_minute: rpm_per_cu,
                    input_tokens_per_minute: rpm_per_cu * demand.avg_input_tokens,
                    output_tokens_per_minute: rpm_per_cu * demand.avg_output_tokens,
                },
                price: pricing::price(
                    &config.pricing.currency,
                    base,
                    tier,
                    isolation,
                    sku,
                    recommended,
                ),
                slo: Slo {
                    ttft_p95_ms: tier.ttft_target_ms(
                        shape_for_context.input_p95,
                        (shape_for_context.input_p95 as f64 * cached_share) as u64,
                    ),
                    tpot_p95_ms: tier.tpot_target_ms(demand.avg_output_tokens as u64),
                },
                demand,
                recommended_cus: recommended,
                peak_cus: peak,
                available_cus: available,
                serves_context,
                feasible,
                suggested_boundary_policy: policy,
                notes,
            });
        }
    }

    let recommendation = match &current {
        Some(c) => {
            // Per region, compare the recommendation with what the reservation has now.
            quotes.iter().find(|q| q.tier == c.tier).map(|_| {
                let recommended: u32 = regions
                    .iter()
                    .filter_map(|r| quotes.iter().find(|q| q.region == *r && q.tier == c.tier))
                    .map(|q| q.recommended_cus)
                    .sum();
                let summary = match recommended.cmp(&c.cus) {
                    std::cmp::Ordering::Greater => format!(
                        "Increase from {} to {recommended} CUs. Increases apply immediately.",
                        c.cus
                    ),
                    std::cmp::Ordering::Less => format!(
                        "{recommended} CUs would cover recent traffic. Decreases take effect at renewal."
                    ),
                    std::cmp::Ordering::Equal => "The current size fits recent traffic.".to_string(),
                };
                Recommendation {
                    region: regions.join(","),
                    tier: c.tier,
                    cus: recommended,
                    monthly_price: pricing::price(
                        &config.pricing.currency,
                        base,
                        c.tier,
                        isolation,
                        sku,
                        recommended,
                    )
                    .monthly,
                    summary,
                }
            })
        }
        None => quotes
            .iter()
            .filter(|q| q.feasible)
            .min_by_key(|q| (q.price.monthly, q.tier as u8))
            .map(|q| Recommendation {
                region: q.region.clone(),
                tier: q.tier,
                cus: q.recommended_cus,
                monthly_price: q.price.monthly,
                summary: format!(
                    "{} CUs of {:?} in {} is the lowest-priced option with capacity available.",
                    q.recommended_cus, q.tier, q.region
                ),
            }),
    };

    let observed_shape = observed_shape.map(|mut s| {
        if let Some(q) = quotes.first() {
            let ratio = q.demand.peak_wu_per_s / q.demand.sustained_wu_per_s.max(f64::MIN_POSITIVE);
            s.burst_factor = (ratio * 10.0).round() / 10.0;
            s.burst_factor = s.burst_factor.clamp(1.0, 20.0);
        }
        s
    });

    Ok(QuoteResponse {
        model: model_id,
        source,
        isolation,
        current,
        observed_shape,
        quotes,
        recommendation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pt_core::cost::TierCapacity;
    use pt_core::Coefficients;

    fn profile() -> PerformanceProfile {
        PerformanceProfile {
            name: "p".into(),
            coefficients: Coefficients {
                a: 1.0,
                b: 0.1,
                c: 2.0,
                d: 0.0,
            },
            capacity_wu_per_s: TierCapacity::default(),
        }
    }

    fn entry(offset_ms: u64, input: u64, output: u64) -> TraceEntry {
        TraceEntry {
            offset_ms,
            input_tokens: input,
            cached_tokens: 0,
            output_tokens: output,
            wu: None,
        }
    }

    #[test]
    fn analytic_demand() {
        // 600 rpm of 1,000 in (half cached) + 100 out: 500 + 50 + 200 = 750 WU each.
        let w = Workload::Analytic {
            rpm: 600.0,
            input: 1_000,
            cached: 500,
            output: 100,
            burst_factor: 3.0,
        };
        let d = w.demand(&profile(), Tier::Standard);
        assert_eq!(d.wu_per_request, 750.0);
        assert_eq!(d.sustained_wu_per_s, 7_500.0);
        assert_eq!(d.peak_wu_per_s, 22_500.0);
    }

    #[test]
    fn trace_demand_uses_busiest_windows() {
        // 60 requests of 100 WU spread over a minute, plus 20 more in one 10 s burst.
        let mut t: Vec<_> = (0..60).map(|i| entry(i * 1_000, 100, 0)).collect();
        t.extend((0..20).map(|i| entry(30_000 + i * 100, 100, 0)));
        let d = Workload::Trace(t).demand(&profile(), Tier::Standard);
        assert_eq!(d.wu_per_request, 100.0);
        // 8,000 WU over the 60 s span.
        assert!(
            (d.sustained_wu_per_s - 8_000.0 / 60.0).abs() < 1e-9,
            "{d:?}"
        );
        // Busiest 10 s: 10 steady + 20 burst requests = 3,000 WU.
        assert!((d.peak_wu_per_s - 300.0).abs() < 1e-9, "{d:?}");
        assert!((d.requests_per_minute - 80.0).abs() < 1e-9);
    }

    #[test]
    fn sizing_and_burst_suggestion() {
        let d = Demand {
            requests_per_minute: 0.0,
            avg_input_tokens: 0.0,
            avg_output_tokens: 0.0,
            wu_per_request: 1.0,
            sustained_wu_per_s: 1_600.0,
            peak_wu_per_s: 4_500.0,
        };
        // 1,600 ÷ (1,000 × 0.8) = 2 CUs; peak needs 5.
        let (rec, peak, policy, notes) = size(&d, 1_000.0);
        assert_eq!((rec, peak), (2, 5));
        let burst = policy.unwrap().burst.unwrap();
        assert!((burst.max_rate_multiple - 2.3).abs() < 1e-9, "{burst:?}");
        assert!(
            notes[0].contains("2.2×") || notes[0].contains("2.3×"),
            "{notes:?}"
        );

        let flat = Demand {
            peak_wu_per_s: 1_600.0,
            ..d
        };
        let (_, _, policy, notes) = size(&flat, 1_000.0);
        assert!(policy.is_none() && notes.is_empty());

        let spiky = Demand {
            peak_wu_per_s: 40_000.0,
            ..d
        };
        let (_, _, policy, _) = size(&spiky, 1_000.0);
        assert!(policy.unwrap().spillover);

        let tiny = Demand {
            sustained_wu_per_s: 1.0,
            peak_wu_per_s: 1.0,
            ..d
        };
        assert_eq!(size(&tiny, 1_000.0).0, 1, "minimum 1 CU");
    }

    #[test]
    fn observed_shape_from_trace() {
        let mut t: Vec<_> = (0..100).map(|i| entry(i * 1_000, 1_000 + i, 50)).collect();
        t[0].cached_tokens = 1_000;
        t.push(entry(200_000, 20_000, 500));
        let s = Workload::Trace(t).observed_shape(131_072).unwrap();
        assert_eq!(s.input_max, 20_000);
        assert_eq!(s.context_ceiling, 32_768);
        assert_eq!(s.output_p95, 50);
        assert!(s.input_p95 >= 1_095 && s.input_p95 <= 1_100, "{s:?}");
        assert!(s.cache_hit_ratio > 0.0);
    }
}
