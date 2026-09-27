-- The latest status each regional component reported, for the dashboards (ADR-043):
-- capacity controllers' pools and routers. Soft state, replaced on every report and pruned
-- when stale.
CREATE TABLE IF NOT EXISTS component_reports (
    kind         TEXT NOT NULL,
    region       TEXT NOT NULL,
    id           TEXT NOT NULL,
    body         JSONB NOT NULL,
    reported_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (kind, region, id)
);
