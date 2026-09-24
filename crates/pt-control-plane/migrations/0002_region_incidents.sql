-- Operator-declared region incidents (docs/09 §4). Used to exclude failover windows
-- (Multi-region SKU) and outage periods (Regional SKU) from SLA attainment.
-- Not yet used: the service runs on MemoryStore until the SQL store is implemented.

CREATE TABLE IF NOT EXISTS region_incidents (
    id           STRING PRIMARY KEY,
    region       STRING NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL,
    ended_at     TIMESTAMPTZ,
    description  STRING NOT NULL,
    declared_at  TIMESTAMPTZ NOT NULL,
    CHECK (ended_at IS NULL OR ended_at >= started_at),
    INDEX by_time (started_at)
);

-- At most one open incident per region.
CREATE UNIQUE INDEX IF NOT EXISTS one_open_incident_per_region
    ON region_incidents (region)
    WHERE ended_at IS NULL;
