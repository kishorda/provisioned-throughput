# CLAUDE.md

## What this project is
This is the architecture design for a **Provisioned Throughput (PT)** product for AI inference, written from a principal-architect perspective. It answers the problems raised in the PM's blog post:
https://kishoraher.wordpress.com/2026/09/23/provisioned-throughput-for-ai-inference-why-just-reserve-some-capacity-is-harder-than-it-sounds/

**Current state:** documentation only. There is no code yet. It's a git repo with no remote configured. Don't scaffold code unless asked.

## Fixed decisions (don't re-litigate without the user)
- **Sellable unit:** an abstract **Capacity Unit (CU)** = a fixed rate of **Work Units (WU)** per second at a named SLO tier (Interactive / Agentic / Standard).
  WU = `a·uncached_prefill + b·cached_prefill + c·decode·m_decode + d·KV_token_seconds`. Coefficients come from a per-(model, GPU, engine version, parallelism) `PerformanceProfile`.
- **Footprint:** multi-cluster and **multi-region** from v1. The global control plane is off the request path, and regional data planes are statically stable.
- **Stack:** Kubernetes + **NVIDIA Dynamo** (KV router, disaggregated prefill/decode, KVBM, NIXL, Planner, operator, Grove), plus KAI Scheduler. **Rust** for the gateway (Pingora or hyper + tower), Quota Coordinator (tonic + openraft), router extensions, kube-rs controllers, planner (good_lp + HiGHS), and metering.
- **Traffic classes (strict priority):** `provisioned > burst > spillover > payg`. Headroom is backfilled with preemptible PAYG.
- **SLA:** measured at the regional PT Gateway, as p95 per 5-minute window per deployment, counting in-shape provisioned traffic only.

## Layout
```
docs/
  README.md                     # index, executive summary, glossary, ADR table
  01-requirements-and-traceability.md   # blog problems P1–P20 → requirements → sections; NFRs N1–N10
  02 … 11-*.md                  # unit/cost model, system, request path, isolation, capacity,
                                # multi-region, K8s+Dynamo, metering/SLA, lifecycle, roadmap
  adr/ADR-001 … ADR-010-*.md    # Nygard format: Status, Date, Context, Decision, Consequences
```
Published summary page (private Artifact): https://claude.ai/artifact/HMSSEEU8fb8tWSrGE9NmSH
Its source HTML lived in a session scratchpad, not in this repo. To update it, republish with that URL after reading it.

## Conventions for editing docs
- Headings are numbered `## N. Title`. Cross-links use GitHub-style anchors (for example `05-isolation-and-scheduling.md#4-level-3--engine`). If you rename a heading, fix the links that point to it.
- Each design doc starts with a `> Decision record(s):` line linking its ADRs, and ends with a **"Blog problems addressed"** line listing P-numbers.
- When you add or change a design response, update the traceability matrix in `01-requirements-and-traceability.md`. Every blog problem must map to a section.
- New decisions get a new ADR (next number, one decision per file), added to the table in `docs/README.md`.
- Diagrams are Mermaid. In `sequenceDiagram` message text, avoid `;` because it acts as a statement separator.
- Spelling is British (tokenise, behaviour, utilisation). Numeric targets (tier latencies, coefficients, percentages) are placeholders until calibration runs. Keep them labelled as such.

## Verification
- **Links and anchors:** extract every relative `](path#anchor)` link from `docs/**/*.md`, and check that the target file exists and that the anchor matches a GitHub slug of a heading (outside code fences). A small inline Python script was used for this. Expect 0 broken links.
- **Mermaid:** extract the ```mermaid blocks and render each one with
  `npx -y @mermaid-js/mermaid-cli -p pp.json -i d.mmd -o d.svg`, where `pp.json` is `{"args":["--no-sandbox"]}`. Chromium's sandbox is unavailable on this machine (Ubuntu AppArmor userns restriction). Put temporary files in the session scratchpad, not in the repo.

## Product decisions (resolved 2026-09-23)
These are recorded in `docs/11-roadmap-risks-open-questions.md` §4. Treat them as fixed:
- Burst credit is free within its cap. Spillover is billed at PAYG list price.
- CU re-rating is infrequent (no fixed cadence) and passes 50% of efficiency gains to customers.
- SLA commitment is 99.8% attainment. Credits are 10% / 20% / 30% / 50% below 99.8 / 99.7 / 99.6 / 99.5. There is no out-of-shape grace margin.
- Minimum reservation is 1 CU, with 1-, 3-, or 6-month terms. Increases are allowed mid-term for the remaining term. Decreases happen only at renewal.
- Strict-dedicated (no backfill) is offered at launch.
- There is no customer-defined priority beyond `continuation` at launch.

## Open items
Still open for PM: the per-tier CU price multipliers and the strict-dedicated premium. Top technical risks: Dynamo API churn and fork maintenance, and upstream acceptance of the engine KV-budget patch.
