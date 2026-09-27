-- Several pools per model in a region (ADR-045). Counters move from capacity_pools, keyed
-- by (region, model), to pool_capacity, keyed by (region, pool). A pool's id defaults to
-- its model id, so every existing pool keeps its counters under that id. capacity_pools
-- stays, unused, for a rollback.
CREATE TABLE IF NOT EXISTS pool_capacity (
    region          TEXT NOT NULL,
    pool            TEXT NOT NULL,
    model           TEXT NOT NULL,
    capacity_micro  INT8 NOT NULL DEFAULT 0,
    reserved_micro  INT8 NOT NULL DEFAULT 0 CHECK (reserved_micro >= 0),
    max_context     INT8 NOT NULL,
    micro_counted   BOOL NOT NULL DEFAULT false,
    PRIMARY KEY (region, pool)
);

INSERT INTO pool_capacity
    (region, pool, model, capacity_micro, reserved_micro, max_context, micro_counted)
SELECT region, model, model, capacity_micro, reserved_micro, max_context, micro_counted
FROM capacity_pools
ON CONFLICT (region, pool) DO NOTHING;

-- Which pool serves each region share. Empty for reservations sold before this migration,
-- which live on their region's default pool.
ALTER TABLE provisioned_throughput
    ADD COLUMN IF NOT EXISTS placements JSONB NOT NULL DEFAULT '[]';
