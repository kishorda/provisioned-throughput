//! Monthly invoices (docs/12 §7, ADR-018).
//!
//! One invoice per tenant per calendar month (UTC), in arrears:
//!
//! - **Reservation fee.** Each reservation's monthly price, prorated by the second over
//!   the time it was billable in the month. The rate timeline comes from its events:
//!   `Activated`, `CapacityIncreased`, `ChangeApplied`, and `Renewed` set the monthly price;
//!   `Ended` and `Cancelled` stop it. A full month bills exactly the monthly price.
//! - **Spillover.** Tokens of spillover requests that ran (`Ok` or cancelled by the
//!   client), billed as regular PAYG traffic at the model's PAYG list price per million
//!   tokens. Burst is free.
//! - **SLA credit.** Once the month has ended, the SLA report's credit percentage applied
//!   to that reservation's fee for the month.
//!
//! The current month (and the previous one, until finalised) is a draft computed on
//! demand. After `finalize_grace_hours`, [`finalize_due`] stores the invoice; stored
//! invoices never change.

use jiff::{SignedDuration, Timestamp};
use pt_core::{Outcome, Tier, TrafficClass};
use pt_telemetry::store::StoredRecord;
use pt_telemetry::{Directory, UsageStore};
use serde::{Deserialize, Serialize};

use crate::clock::Clock;
use crate::config::PaygPrice;
use crate::model::{total_cus, EventKind, ProvisionedThroughput, State};
use crate::planner::CapacityPlanner;
use crate::service::{Service, ServiceError};
use crate::store::{Store, StoreError};
use crate::telemetry::CpTelemetry;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvoiceStatus {
    /// Still changing: the month is running, or within its finalisation grace period.
    Draft,
    /// Stored and immutable.
    Final,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LineKind {
    ReservationFee,
    Spillover,
    SlaCredit,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InvoiceLine {
    pub kind: LineKind,
    pub reservation: String,
    pub reservation_name: String,
    pub description: String,
    /// The part of the month this line covers.
    pub from: Timestamp,
    pub to: Timestamp,
    pub quantity: f64,
    /// `month` for fees, `1M tokens` for spillover, `percent` for credits.
    pub unit: String,
    /// Minor units per `unit`.
    pub unit_price: i64,
    /// Minor units. Negative for credits.
    pub amount: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Invoice {
    /// `inv-{tenant}-{period}`.
    pub id: String,
    pub tenant: String,
    /// `2026-10`.
    pub period: String,
    pub period_start: Timestamp,
    pub period_end: Timestamp,
    pub currency: String,
    pub status: InvoiceStatus,
    pub lines: Vec<InvoiceLine>,
    /// Fees and spillover.
    pub subtotal: i64,
    /// SLA credits (zero or negative).
    pub credits: i64,
    pub total: i64,
    pub generated_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finalized_at: Option<Timestamp>,
}

impl Invoice {
    fn totals(&mut self) {
        self.subtotal = self
            .lines
            .iter()
            .filter(|l| l.kind != LineKind::SlaCredit)
            .map(|l| l.amount)
            .sum();
        self.credits = self
            .lines
            .iter()
            .filter(|l| l.kind == LineKind::SlaCredit)
            .map(|l| l.amount)
            .sum();
        self.total = self.subtotal + self.credits;
    }
}

/// `[start, end)` of a calendar month in UTC, from `2026-10`.
pub fn month_bounds(period: &str) -> Option<(Timestamp, Timestamp)> {
    let (y, m) = period.split_once('-')?;
    if m.len() != 2 {
        return None;
    }
    let start = jiff::civil::Date::new(y.parse().ok()?, m.parse().ok()?, 1).ok()?;
    let end = start.checked_add(jiff::Span::new().months(1)).ok()?;
    let ts = |d: jiff::civil::Date| d.to_zoned(jiff::tz::TimeZone::UTC).map(|z| z.timestamp());
    Some((ts(start).ok()?, ts(end).ok()?))
}

/// `2026-10` for a timestamp.
pub fn period_of(t: Timestamp) -> String {
    let d = t.to_zoned(jiff::tz::TimeZone::UTC).date();
    format!("{:04}-{:02}", d.year(), d.month())
}

/// The month before `period`.
pub fn previous_period(period: &str) -> Option<String> {
    let (start, _) = month_bounds(period)?;
    Some(period_of(start - SignedDuration::from_secs(1)))
}

/// A stretch of time at one rate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RateSegment {
    pub from: Timestamp,
    pub to: Timestamp,
    pub cus: u32,
    pub tier: Tier,
    pub monthly: u64,
}

/// When `pt` was billable, and at what monthly rate, up to `until`. Older events without
/// rate fields fall back to `price_of(tier, cus)`.
pub fn rate_timeline(
    pt: &ProvisionedThroughput,
    until: Timestamp,
    price_of: impl Fn(Tier, u32) -> u64,
) -> Vec<RateSegment> {
    let mut events: Vec<_> = pt.events.iter().collect();
    events.sort_by_key(|e| e.at);
    let mut out = Vec::new();
    let (mut cus, mut tier) = (pt.cus, pt.tier);
    let mut monthly = price_of(tier, cus);
    let mut open: Option<Timestamp> = None;
    let mut change =
        |at: Timestamp, open: &mut Option<Timestamp>, cus, tier, monthly, still: bool| {
            if let Some(from) = open.take() {
                if at > from {
                    out.push(RateSegment {
                        from,
                        to: at,
                        cus,
                        tier,
                        monthly,
                    });
                }
            }
            if still {
                *open = Some(at);
            }
        };
    for e in events.into_iter().filter(|e| e.at < until) {
        let active = open.is_some();
        match &e.kind {
            EventKind::Created { cus: c, monthly: m } => {
                cus = *c;
                monthly = *m;
            }
            EventKind::Activated {
                cus: c,
                tier: t,
                monthly: m,
            } => {
                change(e.at, &mut open, cus, tier, monthly, false);
                cus = c.unwrap_or(cus);
                tier = t.unwrap_or(tier);
                monthly = m.unwrap_or_else(|| price_of(tier, cus));
                open = Some(e.at);
            }
            EventKind::CapacityIncreased {
                from,
                to,
                cus: c,
                monthly: m,
                ..
            } => {
                change(e.at, &mut open, cus, tier, monthly, active);
                cus = c.unwrap_or(cus + to.saturating_sub(*from));
                monthly = m.unwrap_or_else(|| price_of(tier, cus));
            }
            EventKind::ChangeApplied {
                tier: t,
                regions,
                monthly: m,
            } => {
                change(e.at, &mut open, cus, tier, monthly, active);
                tier = *t;
                cus = total_cus(regions);
                monthly = m.unwrap_or_else(|| price_of(tier, cus));
            }
            EventKind::Renewed { monthly: m, .. } => {
                change(e.at, &mut open, cus, tier, monthly, active);
                monthly = *m;
            }
            EventKind::Ended | EventKind::Cancelled => {
                change(e.at, &mut open, cus, tier, monthly, false);
            }
            _ => {}
        }
    }
    // Still billable: up to `until`, but never past a term that has run out without an
    // `Ended` event yet (the lifecycle loop hasn't caught up).
    if let Some(from) = open {
        let stop = match pt.state {
            State::PendingCancellation => until.min(pt.term_end),
            _ => until,
        };
        if stop > from {
            out.push(RateSegment {
                from,
                to: stop,
                cus,
                tier,
                monthly,
            });
        }
    }
    out
}

/// Fee lines for one reservation in `[start, end)`.
pub fn fee_lines(
    pt: &ProvisionedThroughput,
    start: Timestamp,
    end: Timestamp,
    until: Timestamp,
    price_of: impl Fn(Tier, u32) -> u64,
) -> Vec<InvoiceLine> {
    let month = end.duration_since(start).as_secs_f64();
    rate_timeline(pt, until.min(end), price_of)
        .into_iter()
        .filter_map(|s| {
            let (from, to) = (s.from.max(start), s.to.min(end));
            if to <= from {
                return None;
            }
            let fraction = to.duration_since(from).as_secs_f64() / month;
            Some(InvoiceLine {
                kind: LineKind::ReservationFee,
                reservation: pt.id.clone(),
                reservation_name: pt.name.clone(),
                description: format!(
                    "{}: {} CU {} ({})",
                    pt.name,
                    s.cus,
                    tier_name(s.tier),
                    pt.model
                ),
                from,
                to,
                quantity: (fraction * 1e6).round() / 1e6,
                unit: "month".into(),
                unit_price: s.monthly as i64,
                amount: (s.monthly as f64 * fraction).round() as i64,
            })
        })
        .collect()
}

fn tier_name(t: Tier) -> &'static str {
    match t {
        Tier::Interactive => "Interactive",
        Tier::Agentic => "Agentic",
        Tier::Standard => "Standard",
    }
}

/// Spillover lines for one reservation from its usage records: input, cached input, and
/// output tokens at the PAYG price. Only requests that ran are billed.
pub fn spillover_lines(
    pt: &ProvisionedThroughput,
    records: &[StoredRecord],
    price: &PaygPrice,
    start: Timestamp,
    end: Timestamp,
) -> Vec<InvoiceLine> {
    let (mut input, mut cached, mut output) = (0u64, 0u64, 0u64);
    for s in records {
        let r = &s.record;
        let ran = matches!(r.outcome, Outcome::Ok | Outcome::ClientCancelled);
        if r.class == Some(TrafficClass::Spillover) && ran {
            input += r.tokens.uncached_prefill;
            cached += r.tokens.cached_prefill;
            output += r.tokens.decode;
        }
    }
    [
        ("input", input, price.input_per_mtok),
        ("cached input", cached, price.cached_input_per_mtok),
        ("output", output, price.output_per_mtok),
    ]
    .into_iter()
    .filter(|(_, tokens, _)| *tokens > 0)
    .map(|(what, tokens, per_mtok)| InvoiceLine {
        kind: LineKind::Spillover,
        reservation: pt.id.clone(),
        reservation_name: pt.name.clone(),
        description: format!("{}: spillover {what} tokens at PAYG price", pt.name),
        from: start,
        to: end,
        quantity: tokens as f64 / 1e6,
        unit: "1M tokens".into(),
        unit_price: per_mtok as i64,
        amount: (tokens as f64 * per_mtok as f64 / 1e6).round() as i64,
    })
    .collect()
}

/// The SLA credit line for a reservation whose fee for the month was `fee`.
pub fn credit_line(
    pt: &ProvisionedThroughput,
    credit_pct: u32,
    attainment_pct: f64,
    fee: i64,
    start: Timestamp,
    end: Timestamp,
) -> Option<InvoiceLine> {
    (credit_pct > 0 && fee > 0).then(|| InvoiceLine {
        kind: LineKind::SlaCredit,
        reservation: pt.id.clone(),
        reservation_name: pt.name.clone(),
        description: format!("{}: SLA credit, {attainment_pct:.2}% attainment", pt.name),
        from: start,
        to: end,
        quantity: f64::from(credit_pct),
        unit: "percent".into(),
        unit_price: fee,
        amount: -(fee * i64::from(credit_pct) / 100),
    })
}

/// Build a tenant's invoice for `period` from reservations, usage, and SLA reports.
pub async fn compute<S: Store, P: CapacityPlanner, C: Clock>(
    svc: &Service<S, P, C>,
    telemetry: &CpTelemetry<S, P, C>,
    tenant: &str,
    period: &str,
) -> Result<Invoice, ServiceError> {
    let (start, end) = month_bounds(period).ok_or_else(|| ServiceError::Validation {
        field: "period".into(),
        message: "period must look like 2026-10.".into(),
    })?;
    let now = svc.clock.now();
    let complete = now >= end;
    let config = &svc.config;
    let ms = |t: Timestamp| t.as_millisecond().max(0) as u64;
    let mut lines = Vec::new();
    for pt in svc.list(tenant, None, true).await? {
        if pt.created_at >= end {
            continue;
        }
        let price_of = |tier: Tier, cus: u32| {
            crate::pricing::price(
                &config.pricing.currency,
                config.pricing.base_cu_price_per_month_cents,
                tier,
                pt.isolation,
                pt.sku,
                cus,
            )
            .monthly
        };
        let fees = fee_lines(&pt, start, end, now, price_of);
        let fee: i64 = fees.iter().map(|l| l.amount).sum();
        lines.extend(fees);

        let records = telemetry
            .store
            .range(tenant, &pt.id, ms(start), ms(end))
            .await
            .map_err(|e| ServiceError::Unavailable(e.to_string()))?;
        if let Some(price) = config.payg_price(&pt.model) {
            lines.extend(spillover_lines(&pt, &records, price, start, end));
        }
        if complete && fee > 0 {
            let info = telemetry
                .directory
                .reservation(tenant, &pt.id)
                .await
                .map_err(|e| ServiceError::Unavailable(e.to_string()))?;
            if let Some(info) = info {
                let report =
                    pt_telemetry::sla::report(&info, &records, period, (ms(start), ms(end)), true);
                lines.extend(credit_line(
                    &pt,
                    report.credit_pct,
                    report.attainment_pct,
                    fee,
                    start,
                    end,
                ));
            }
        }
    }
    let mut invoice = Invoice {
        id: format!("inv-{tenant}-{period}"),
        tenant: tenant.to_string(),
        period: period.to_string(),
        period_start: start,
        period_end: end,
        currency: config.pricing.currency.clone(),
        status: InvoiceStatus::Draft,
        lines,
        subtotal: 0,
        credits: 0,
        total: 0,
        generated_at: now,
        finalized_at: None,
    };
    invoice.totals();
    Ok(invoice)
}

/// The invoice for `period`: the stored one if final, otherwise a draft. Drafts exist for
/// the current month and for past months not yet finalised; older unfinalised months,
/// whose usage may be pruned, and future months are `NotFound`.
pub async fn invoice<S: Store, P: CapacityPlanner, C: Clock>(
    svc: &Service<S, P, C>,
    telemetry: &CpTelemetry<S, P, C>,
    tenant: &str,
    period: &str,
) -> Result<Invoice, ServiceError> {
    if let Some(stored) = svc.store.get_invoice(tenant, period).await? {
        return Ok(stored);
    }
    let current = period_of(svc.clock.now());
    let previous = previous_period(&current).unwrap_or_default();
    if period != current && period != previous {
        return match month_bounds(period) {
            None => Err(ServiceError::Validation {
                field: "period".into(),
                message: "period must look like 2026-10.".into(),
            }),
            Some(_) => Err(ServiceError::NotFound),
        };
    }
    compute(svc, telemetry, tenant, period).await
}

/// Finalise and store every invoice whose month ended more than `finalize_grace_hours`
/// ago and isn't stored yet. Only the previous month is considered: older months were
/// finalised by earlier runs. Empty invoices aren't stored. Returns the invoices stored.
pub async fn finalize_due<S: Store, P: CapacityPlanner, C: Clock>(
    svc: &Service<S, P, C>,
    telemetry: &CpTelemetry<S, P, C>,
) -> Result<Vec<Invoice>, ServiceError> {
    let now = svc.clock.now();
    let Some(period) = previous_period(&period_of(now)) else {
        return Ok(vec![]);
    };
    let (_, end) = month_bounds(&period).expect("valid period");
    let grace = SignedDuration::from_hours(svc.config.billing.finalize_grace_hours as i64);
    if now < end + grace {
        return Ok(vec![]);
    }
    let mut stored = Vec::new();
    for tenant in svc.config.tenants.iter().map(|t| t.id.clone()) {
        if svc.store.get_invoice(&tenant, &period).await?.is_some() {
            continue;
        }
        let mut invoice = compute(svc, telemetry, &tenant, &period).await?;
        if invoice.lines.is_empty() {
            continue;
        }
        invoice.status = InvoiceStatus::Final;
        invoice.finalized_at = Some(now);
        match svc.store.insert_invoice(invoice.clone()).await {
            Ok(()) => {
                tracing::info!(%tenant, %period, total = invoice.total, "invoice finalised");
                stored.push(invoice);
            }
            // Another instance finalised it first.
            Err(StoreError::AlreadyExists(_)) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(stored)
}

/// All of a tenant's final invoices, newest first, plus drafts for the current month and
/// the previous one if it isn't final yet.
pub async fn list<S: Store, P: CapacityPlanner, C: Clock>(
    svc: &Service<S, P, C>,
    telemetry: &CpTelemetry<S, P, C>,
    tenant: &str,
) -> Result<Vec<Invoice>, ServiceError> {
    let mut out = svc.store.list_invoices(tenant).await?;
    let current = period_of(svc.clock.now());
    for period in [previous_period(&current).unwrap_or_default(), current] {
        if !out.iter().any(|i| i.period == period) {
            let draft = compute(svc, telemetry, tenant, &period).await?;
            if !draft.lines.is_empty() {
                out.push(draft);
            }
        }
    }
    out.sort_by(|a, b| b.period.cmp(&a.period));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Event, Price, RegionShare, Sku};
    use pt_core::{PoolIsolation, Shape, TermMonths};

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    fn pt(events: Vec<(&str, EventKind)>) -> ProvisionedThroughput {
        ProvisionedThroughput {
            id: "pt-1".into(),
            tenant: "acme".into(),
            name: "agents".into(),
            model: "m".into(),
            tier: Tier::Agentic,
            regions: vec![RegionShare {
                region: "eu-west".into(),
                cus: 4,
            }],
            cus: 4,
            sku: Sku::Regional,
            isolation: PoolIsolation::Shared,
            shape: Shape {
                input_p95: 1,
                input_max: 1,
                output_p95: 1,
                context_ceiling: 1,
                cache_hit_ratio: 0.0,
                burst_factor: 1.0,
            },
            boundary_policy: Default::default(),
            term_months: TermMonths::One,
            term_start: at("2026-10-11T00:00:00Z"),
            term_end: at("2026-11-11T00:00:00Z"),
            auto_renew: true,
            state: State::Active,
            pending_changes: None,
            endpoints: vec![],
            price: Price {
                currency: "USD".into(),
                per_cu_monthly: 1_000,
                monthly: 4_000,
            },
            failover_headroom: vec![],
            deployments: vec![],
            version: 1,
            created_at: at("2026-10-11T00:00:00Z"),
            updated_at: at("2026-10-11T00:00:00Z"),
            events: events
                .into_iter()
                .map(|(t, kind)| Event { at: at(t), kind })
                .collect(),
        }
    }

    fn activated(cus: u32, monthly: u64) -> EventKind {
        EventKind::Activated {
            cus: Some(cus),
            tier: Some(Tier::Agentic),
            monthly: Some(monthly),
        }
    }

    fn per_cu(_: Tier, cus: u32) -> u64 {
        1_000 * u64::from(cus)
    }

    #[test]
    fn months() {
        let (s, e) = month_bounds("2026-02").unwrap();
        assert_eq!(
            (s, e),
            (at("2026-02-01T00:00:00Z"), at("2026-03-01T00:00:00Z"))
        );
        assert_eq!(previous_period("2026-01").unwrap(), "2025-12");
        assert_eq!(period_of(at("2026-10-31T23:59:59Z")), "2026-10");
        assert!(month_bounds("2026-13").is_none());
        assert!(month_bounds("2026-1").is_none());
    }

    #[test]
    fn full_month_bills_exactly_the_monthly_price() {
        let p = pt(vec![("2026-09-11T00:00:00Z", activated(4, 4_000))]);
        let (s, e) = month_bounds("2026-10").unwrap();
        let lines = fee_lines(&p, s, e, e, per_cu);
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].amount, 4_000);
        assert_eq!(lines[0].quantity, 1.0);
    }

    #[test]
    fn mid_month_start_increase_and_end_are_prorated() {
        // October has 31 days. Active from the 11th (21 days), 4 CU until the 21st, then
        // 6 CU until it ends on the 26th.
        let p = pt(vec![
            (
                "2026-10-11T00:00:00Z",
                EventKind::Created {
                    cus: 4,
                    monthly: 4_000,
                },
            ),
            ("2026-10-11T00:00:00Z", activated(4, 4_000)),
            (
                "2026-10-21T00:00:00Z",
                EventKind::CapacityIncreased {
                    region: "eu-west".into(),
                    from: 4,
                    to: 6,
                    prorated_charge: 0,
                    cus: Some(6),
                    monthly: Some(6_000),
                },
            ),
            ("2026-10-26T00:00:00Z", EventKind::Ended),
        ]);
        let (s, e) = month_bounds("2026-10").unwrap();
        let lines = fee_lines(&p, s, e, e, per_cu);
        let amounts: Vec<i64> = lines.iter().map(|l| l.amount).collect();
        assert_eq!(
            amounts,
            [
                (4_000.0 * 10.0 / 31.0_f64).round() as i64,
                (6_000.0 * 5.0 / 31.0_f64).round() as i64
            ]
        );
        assert!(lines[1].description.contains("6 CU Agentic"));
        // November: nothing.
        let (s, e) = month_bounds("2026-11").unwrap();
        assert!(fee_lines(&p, s, e, e, per_cu).is_empty());
    }

    #[test]
    fn renewal_changes_the_rate_and_drafts_stop_at_now() {
        let p = pt(vec![
            ("2026-09-11T00:00:00Z", activated(4, 4_000)),
            (
                "2026-10-11T00:00:00Z",
                EventKind::ChangeApplied {
                    tier: Tier::Standard,
                    regions: vec![RegionShare {
                        region: "eu-west".into(),
                        cus: 2,
                    }],
                    monthly: Some(1_500),
                },
            ),
            (
                "2026-10-11T00:00:00Z",
                EventKind::Renewed {
                    term_start: at("2026-10-11T00:00:00Z"),
                    term_end: at("2026-11-11T00:00:00Z"),
                    monthly: 1_500,
                },
            ),
        ]);
        let (s, e) = month_bounds("2026-10").unwrap();
        let lines = fee_lines(&p, s, e, e, per_cu);
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].unit_price, 4_000);
        assert_eq!(lines[1].unit_price, 1_500);
        assert!(lines[1].description.contains("2 CU Standard"));
        let total: i64 = lines.iter().map(|l| l.amount).sum();
        assert_eq!(
            total,
            (4_000.0 * 10.0 / 31.0 + 1_500.0 * 21.0 / 31.0_f64).round() as i64
        );

        // A draft on the 16th covers only the time so far.
        let now = at("2026-10-16T00:00:00Z");
        let draft: f64 = fee_lines(&p, s, e, now, per_cu)
            .iter()
            .map(|l| l.quantity)
            .sum();
        assert!((draft - 15.0 / 31.0).abs() < 1e-5);
    }

    #[test]
    fn cancelled_before_start_is_never_billed_and_legacy_events_use_config_prices() {
        let p = pt(vec![
            (
                "2026-10-01T00:00:00Z",
                EventKind::Created {
                    cus: 4,
                    monthly: 4_000,
                },
            ),
            ("2026-10-02T00:00:00Z", EventKind::Cancelled),
        ]);
        let (s, e) = month_bounds("2026-10").unwrap();
        assert!(fee_lines(&p, s, e, e, per_cu).is_empty());

        let legacy = pt(vec![(
            "2026-10-01T00:00:00Z",
            EventKind::Activated {
                cus: None,
                tier: None,
                monthly: None,
            },
        )]);
        assert_eq!(fee_lines(&legacy, s, e, e, per_cu)[0].amount, 4_000);
    }

    #[test]
    fn credit_is_a_share_of_the_fee() {
        let p = pt(vec![]);
        let (s, e) = month_bounds("2026-10").unwrap();
        let c = credit_line(&p, 20, 99.65, 4_000, s, e).unwrap();
        assert_eq!(c.amount, -800);
        assert!(credit_line(&p, 0, 100.0, 4_000, s, e).is_none());
        assert!(credit_line(&p, 20, 99.6, 0, s, e).is_none());
    }
}
