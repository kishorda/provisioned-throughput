-- Automatic region-failure handling (docs/07 §4, ADR-014).
-- Not yet used: the service runs on MemoryStore until the SQL store is implemented.

-- Multi-region SKU: capacity held in each region to absorb another region's share.
ALTER TABLE provisioned_throughput
    ADD COLUMN IF NOT EXISTS failover_headroom JSONB NOT NULL DEFAULT '[]';  -- [{"region": "eu-central", "cus": 10}]

-- Who declared an incident. Only automatic ones are resolved automatically.
ALTER TABLE region_incidents
    ADD COLUMN IF NOT EXISTS source STRING NOT NULL DEFAULT 'operator'
    CHECK (source IN ('operator', 'automatic'));
