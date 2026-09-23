# ADR-006: Backfill reserved headroom with preemptible PAYG

- **Status:** Accepted
- **Date:** 2026-09-23

## Context
Reliability needs N+k replicas per failure domain plus hot spares. Unused reserved
capacity (off-peak, dedicated pools) is also idle. Leaving all of it idle makes PT
uneconomic.

## Decision
All headroom (the `k` spares, hot spares, and unused reserved capacity on dedicated pools)
serves **preemptible PAYG** traffic at the lowest priority. Provisioned traffic evicts it
within 1 s, through router priority and engine preemption with KVBM offload. A
"strict-dedicated" option without backfill is available from launch at 1.3× the
base (Standard) CU price, regardless of tier.
The dedicated capacity for that option is excluded from the backfill economics.

## Consequences
- ✅ Headroom pays for itself. PT pricing can stay competitive.
- ✅ Spares stay warm and continuously exercised, so failover paths are not dead code.
- ⚠️ PAYG customers see variable latency and preemption. This is documented as best-effort.
- ⚠️ Provisioned customers on backfilled dedicated pools share hardware with PAYG. The
  isolation guarantee is a scheduling guarantee, not a physical one.
