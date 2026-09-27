# ADR-040: Route sessions and small reservations to one gateway replica

- **Status:** Accepted, opt-in. Implements the home gateways of docs/04 §6.
- **Date:** 2026-09-26

## Context
Gateway replicas share entitlements through the Quota Coordinator. But each keeps its own
per-message token counts (ADR-028) and prefix history (ADR-030). An agent whose turns land
on different replicas is tokenized from scratch each time and gets no expected cache
hits. docs/04 §6 also planned home gateways for low-volume tenants, so a small
reservation's lease isn't split into slivers across every replica. Load balancers could
hash on a session header, but not on "reservation size".

## Decision
- **Peers.** With `[affinity]` (`self_url`, `peers`), every replica ranks the peers for a
  key by rendezvous hashing: the highest FNV-1a hash of key and peer URL. It's the same
  on every replica, and a peer joining or leaving moves only its own keys.
- **Sessions.** A request with `x-pt-session-id` belongs to the session's top-ranked peer.
- **Home gateways.** A request for a reservation of at most `home_below_cus` CUs belongs
  to one of its deployment's `home_gateways` top-ranked peers (2 by default), spread by
  request id. Larger reservations without a session are served where they land.
- **Forwarding.** A replica that isn't the owner forwards the request once, after
  authenticating it, with `x-pt-forwarded`. The receiver always serves a forwarded request
  itself. The response streams back unchanged, with `x-pt-served-by`. Admission,
  settlement, and usage all happen on the owner.
- **Failure.** If the owner can't be reached, the request is served locally, and the owner
  is skipped for 10 s.

## Consequences
- ✅ An agent's conversation is tokenized once and its cache hits are predicted, whichever
  replica its requests reach.
- ✅ A small reservation's lease concentrates on its home gateways. The others keep only
  the coordinator's small floor.
- ⚠️ A forwarded request costs one extra hop inside the region (about a millisecond).
- ⚠️ Peers are configuration (for example, a StatefulSet's pod DNS names). Discovery
  from Kubernetes endpoints isn't built.
- ⚠️ A replica that restarts gets its sessions back cold: its caches are empty.
