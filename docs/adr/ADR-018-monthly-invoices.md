# ADR-018: Calendar-month invoices in arrears, finalised and stored

- **Status:** Accepted.
- **Date:** 2026-09-24

## Context
The product decisions (docs/11 §4) fix what's charged:
- the reservation fee: tier price × CUs, plus the strict-dedicated surcharge;
- mid-term increases, billed for the rest of the term;
- spillover at the PAYG list price, with burst free;
- SLA credits of 10/20/30/50% of the month's fee.

Nothing turned these into invoices. Events recorded a lump-sum `prorated_charge` per
increase. There was no PAYG price, and SLA credits were only reported. The choices were:
- the billing period: calendar month or term anniversary;
- the spillover price basis;
- whether invoices are stored or recomputed.

## Decision
- **One invoice per tenant per calendar month (UTC), in arrears.** Each reservation's
  fee is its monthly price prorated by the second over the time it was billable in the
  month, so a full month bills exactly the monthly price. The rate timeline comes from
  events:
  - `Activated`, `CapacityIncreased`, and `ChangeApplied` now record the resulting CUs
    and monthly price;
  - `Renewed` already did;
  - `Ended` and `Cancelled` stop billing.

  Lifecycle events are stamped with the moment they took effect (term boundaries), not
  when the lifecycle loop noticed. Mid-term increases are billed month by month.
  `prorated_charge` remains as the commitment for the rest of the term.
- **Spillover at a per-model PAYG price per million tokens** (`[[payg_prices]]`: input,
  cached input, output). Only spillover requests that ran (`Ok` or client-cancelled) are
  billed. Every model must have a price.
- **SLA credit on the same invoice.** Once the month has ended, the SLA report's credit
  percentage is applied to that reservation's fee for the month, as a negative line.
- **Drafts on demand, finals stored.** The current month, and the previous month until
  finalised, are drafts computed from events and usage. `finalize_grace_hours` (48) after
  a month ends, a job stores each tenant's invoice (SQL `invoices` table, one per tenant
  and period). Stored invoices never change. Corrections would be adjustment lines on a
  later invoice, which aren't built yet. Empty invoices aren't stored.
- Telemetry retention must cover a month plus the grace period (validated in config).

## Consequences
- ✅ One invoice date per tenant, and credits land with the month they apply to.
- ✅ Invoices are reproducible from events while drafts, and immutable once final, even
  after usage is pruned.
- ⚠️ No mid-month or in-advance billing. Revenue is recognised a month later than
  billing in advance would.
- ⚠️ Multi-region failover headroom isn't priced (open PM question), so it doesn't
  appear on invoices.
- ⚠️ No taxes, currency conversion, payment collection, or adjustment lines yet. Amounts
  are in the configured currency's minor units.
- ⚠️ Reservations created before this change lack rate fields on their events, and fall
  back to current config prices.
