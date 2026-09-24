# ADR-014: Automatic region failover from gateway heartbeats

- **Status:** Accepted. Refines [ADR-007](ADR-007-regional-static-stability.md).
- **Date:** 2026-09-24

## Context
Docs/07 §4 describes a region-failure sequence for the Multi-region SKU: detect the
failure, steer traffic away, activate dormant failover entitlements in the paired region,
claim headroom, and return traffic gradually. Until now, operators declared incidents by
hand, and nothing held failover headroom or activated failover entitlements. Several
questions needed answers:

- **Headroom.** Is it actually reserved at sale time, or used only if spare capacity
  happens to exist? A Multi-region SLO can't hold if failover capacity isn't guaranteed.
- **Detection.** Who decides a region is down? Active probes from the control plane need
  inbound reachability to every region. Gateways already reach the control plane.
- **Activation.** Which component turns failover entitlements on, and how does it ramp
  them down?

## Decision
- **Reserve headroom at sale time.** A Multi-region reservation holds, in each region's
  failover target, the largest share that fails over to that region. One region fails at
  a time, so it's the maximum, not the sum. Create, increases, renewals, and releases all
  count shares plus headroom, all or nothing. The target is the region's configured
  `pair` if the reservation has a share there, otherwise its largest other region. All of
  a Multi-region reservation's regions must share one data-residency zone. Headroom isn't
  billed separately yet. The Multi-region price is an open PM question.
- **Detect from gateway heartbeats.** Every gateway sends
  `POST /internal/v1/heartbeats` every 5 s with its region token. It reports `serving`
  only if it has entitlements and its engine answers health checks. A region is down when
  no gateway has reported serving for `heartbeat_timeout_seconds` (30).
- **Declare and resolve automatically.** A control-plane loop opens an `automatic`
  incident for a down region, starting at its last serving heartbeat. It does this only
  while another region is serving: if every region looks down, the control plane is
  probably the one cut off. It resolves an automatic incident once the region has served
  continuously for `recovery_seconds` (60). Operators can still declare incidents, for
  example to drain a region. Only operators resolve those.
- **Activate through snapshots.** Snapshots carry each reservation's dormant failover
  shares and the current region failures (open incidents, and resolved ones still
  ramping). A gateway computes its entitlement locally from wall-clock time: own CUs plus
  failover CUs × activation. Activation is 1 while the incident is open, then falls
  linearly to 0 over `return_ramp_minutes` (10). The Quota Coordinator shares the larger
  entitlement between replicas as usual.
- **Publish steering, not DNS.** `GET /internal/v1/steering` gives region weights (0 while
  an incident is open, ramping back to 1) and per-reservation target weights. A
  GeoDNS or global load-balancer controller consumes it. Steering follows incidents, not
  raw health, so an operator-declared drain moves traffic too.

## Consequences
- ✅ Detection needs no inbound access to regions, and works through NAT and firewalls.
- ✅ Failover capacity is guaranteed for Multi-region reservations. It serves PAYG
  meanwhile (ADR-006).
- ✅ The failover entitlement ramps down without new snapshots, and every gateway replica
  in a region computes the same value.
- ⚠️ Activation needs the control plane. If it's down during a region failure, gateways
  keep their last snapshot and failover doesn't activate. Static stability (ADR-007) still
  holds for normal operation.
- ⚠️ A partition between a healthy region and the control plane looks like a region
  failure. The "cut-off" region keeps serving on its snapshot while its pair activates
  failover, so the reservation briefly has up to twice its entitlement. That's
  over-serving, never under-serving, and the headroom is already held.
- ⚠️ Health is soft state. After a control-plane restart, regions are `unknown` until
  their gateways report, and nothing is declared for them.
- ⚠️ Headroom raises the capacity cost of Multi-region reservations without raising their
  price yet.
