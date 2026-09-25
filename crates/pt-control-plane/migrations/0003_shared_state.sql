-- State shared by control-plane instances (ADR-023). Portable between CockroachDB and
-- PostgreSQL.

-- Small counters, for example the entitlement version every instance bumps.
CREATE TABLE IF NOT EXISTS control_plane_state (
    key    TEXT PRIMARY KEY,
    value  INT8 NOT NULL
);

-- Each gateway's latest heartbeat, so any instance can judge region health.
CREATE TABLE IF NOT EXISTS gateway_heartbeats (
    region           TEXT NOT NULL,
    gateway_id       TEXT NOT NULL,
    last_seen        TIMESTAMPTZ NOT NULL,
    serving          BOOL NOT NULL,
    key_id           TEXT,
    last_serving_at  TIMESTAMPTZ,
    serving_since    TIMESTAMPTZ,
    PRIMARY KEY (region, gateway_id)
);
CREATE INDEX IF NOT EXISTS gateway_heartbeats_by_age ON gateway_heartbeats (last_seen);

-- Named leases. The holder of `background` runs the lifecycle, failover, reconciliation,
-- and invoice loops.
CREATE TABLE IF NOT EXISTS leases (
    name        TEXT PRIMARY KEY,
    holder      TEXT NOT NULL,
    expires_at  TIMESTAMPTZ NOT NULL
);

-- Sellable capacity per (region, model), and how much is reserved. Reservations use a
-- conditional UPDATE, so instances can't oversell together.
CREATE TABLE IF NOT EXISTS capacity_pools (
    region       TEXT NOT NULL,
    model        TEXT NOT NULL,
    capacity     INT4 NOT NULL CHECK (capacity >= 0),
    reserved     INT4 NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    max_context  INT8 NOT NULL,
    PRIMARY KEY (region, model)
);
