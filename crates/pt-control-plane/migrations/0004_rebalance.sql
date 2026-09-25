-- Share rebalancing between regions (docs/07 §3, ADR-024). The contracted split stays in
-- `regions`; `effective_regions` is what gateways enforce when rebalancing has moved it.

ALTER TABLE provisioned_throughput
    ADD COLUMN IF NOT EXISTS rebalance BOOL NOT NULL DEFAULT true;
ALTER TABLE provisioned_throughput
    ADD COLUMN IF NOT EXISTS effective_regions JSONB NOT NULL DEFAULT '[]';
