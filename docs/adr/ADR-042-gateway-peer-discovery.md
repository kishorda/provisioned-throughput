# ADR-042: Discover gateway peers from DNS

- **Status:** Accepted. Extends [ADR-040](ADR-040-gateway-affinity.md).
- **Date:** 2026-09-27

## Context
Gateway affinity (ADR-040) routes sessions and small reservations to one replica chosen
by rendezvous hashing over a list of peers. That list was fixed configuration, so every
scale-up or pod replacement meant editing config on every replica. Replicas that
disagree on the list route the same session to different owners.

## Decision
- **Discovery.** `[affinity] discovery_dns = "host:port"`, for example a headless Service
  `pt-gateway.pt-system.svc.cluster.local:8080`, replaces the `peers` list. Every
  `refresh_secs` (10), each replica resolves the name, and each address becomes a peer
  `http://ip:port`. A headless Service returns one address per ready pod.
- **Self.** `self_ip_env` names the variable holding the pod's IP (Kubernetes'
  `status.podIP`). The replica's own URL is `http://<ip>:<port>`, which matches how the
  other replicas see it.
- **Failure.** A replica always counts itself as a peer. A lookup that fails or returns
  nothing keeps the last known peers, so a DNS hiccup doesn't collapse routing.
- **Config rules.** `peers` and `discovery_dns` are mutually exclusive, and
  `self_ip_env` requires `discovery_dns`.

## Consequences
- ✅ Replicas agree on peers without configuration changes as the gateway scales.
  Rendezvous hashing moves only the sessions of a replica that joins or leaves.
- ⚠️ Replicas can disagree for up to one refresh interval after a change, so a request
  may take one extra hop, or be served by a replica that isn't its owner. Nothing breaks:
  a forwarded request is always served by its receiver.
- ⚠️ Only plain HTTP peers are discovered. Gateway-to-gateway traffic stays inside the
  region.
