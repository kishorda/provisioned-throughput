-- Control-plane schema (docs/12 §5, ADR-017).
--
-- Written in the SQL that CockroachDB (production, docs/07 §5) and PostgreSQL share, so
-- the same migrations run on both. CockroachDB-only features (REGIONAL BY ROW, row-level
-- TTL) are applied by operators per deployment, not here. Every statement is idempotent,
-- because `SqlStore::migrate` may race with another control-plane instance.

CREATE TABLE IF NOT EXISTS provisioned_throughput (
    id                 TEXT PRIMARY KEY,
    tenant             TEXT NOT NULL,
    name               TEXT NOT NULL,
    model              TEXT NOT NULL,
    tier               TEXT NOT NULL CHECK (tier IN ('interactive', 'agentic', 'standard')),
    sku                TEXT NOT NULL CHECK (sku IN ('regional', 'multi_region')),
    isolation          TEXT NOT NULL CHECK (isolation IN ('shared', 'dedicated', 'strict_dedicated')),
    regions            JSONB NOT NULL,                -- [{"region": "eu-west", "cus": 10}, ...]
    failover_headroom  JSONB NOT NULL DEFAULT '[]',   -- Multi-region SKU (ADR-014)
    cus                INT4 NOT NULL CHECK (cus >= 1),
    shape              JSONB NOT NULL,
    boundary_policy    JSONB NOT NULL,
    term_months        INT2 NOT NULL CHECK (term_months IN (1, 3, 6)),
    term_start         TIMESTAMPTZ NOT NULL,
    term_end           TIMESTAMPTZ NOT NULL,
    auto_renew         BOOL NOT NULL,
    state              TEXT NOT NULL
                       CHECK (state IN ('scheduled', 'active', 'pending_cancellation', 'ended', 'cancelled')),
    pending_changes    JSONB,
    endpoints          JSONB NOT NULL,
    price              JSONB NOT NULL,
    version            INT8 NOT NULL,                 -- optimistic concurrency; the API's ETag
    created_at         TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS pt_by_tenant ON provisioned_throughput (tenant, created_at);
CREATE INDEX IF NOT EXISTS pt_by_lifecycle ON provisioned_throughput (state, term_end)
    WHERE state IN ('scheduled', 'active', 'pending_cancellation');
-- Names are unique per tenant among live reservations only.
CREATE UNIQUE INDEX IF NOT EXISTS pt_live_name_per_tenant ON provisioned_throughput (tenant, name)
    WHERE state IN ('scheduled', 'active', 'pending_cancellation');

-- Endpoint identities sharing a reservation's entitlement. Ordinal 0 is the primary.
CREATE TABLE IF NOT EXISTS deployments (
    id          TEXT PRIMARY KEY,
    pt_id       TEXT NOT NULL REFERENCES provisioned_throughput (id),
    ordinal     INT4 NOT NULL,
    name        TEXT NOT NULL,
    max_share   FLOAT8 CHECK (max_share IS NULL OR (max_share > 0 AND max_share <= 1)),
    created_at  TIMESTAMPTZ NOT NULL,
    UNIQUE (pt_id, name)
);
CREATE INDEX IF NOT EXISTS deployments_by_reservation ON deployments (pt_id, ordinal);

-- Inference API keys. Only SHA-256 hashes: keys themselves are never stored.
CREATE TABLE IF NOT EXISTS api_keys (
    id             TEXT PRIMARY KEY,
    deployment_id  TEXT NOT NULL REFERENCES deployments (id),
    prefix         TEXT NOT NULL,
    sha256         TEXT NOT NULL UNIQUE,
    created_at     TIMESTAMPTZ NOT NULL,
    expires_at     TIMESTAMPTZ                          -- NULL for the current key
);
CREATE INDEX IF NOT EXISTS api_keys_by_deployment ON api_keys (deployment_id);
CREATE UNIQUE INDEX IF NOT EXISTS one_current_key_per_deployment ON api_keys (deployment_id)
    WHERE expires_at IS NULL;

-- Audit and billing trail. Append-only.
CREATE TABLE IF NOT EXISTS provisioned_throughput_events (
    pt_id   TEXT NOT NULL REFERENCES provisioned_throughput (id),
    seq     INT4 NOT NULL,
    at      TIMESTAMPTZ NOT NULL,
    kind    TEXT NOT NULL,
    detail  JSONB NOT NULL,
    PRIMARY KEY (pt_id, seq)
);

-- Create idempotency. Keys older than 7 days are ignored and may be reused. On
-- CockroachDB, add row-level TTL to delete them:
--   ALTER TABLE idempotency_keys SET (ttl_expiration_expression = 'created_at + INTERVAL ''7 days''');
CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant       TEXT NOT NULL,
    idem_key     TEXT NOT NULL,
    resource_id  TEXT NOT NULL,
    fingerprint  TEXT NOT NULL,                        -- SHA-256 of the request body
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant, idem_key)
);
CREATE INDEX IF NOT EXISTS idempotency_by_age ON idempotency_keys (created_at);

-- Region incidents (docs/07 §4, docs/09 §4).
CREATE TABLE IF NOT EXISTS region_incidents (
    id           TEXT PRIMARY KEY,
    region       TEXT NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL,
    ended_at     TIMESTAMPTZ,
    description  TEXT NOT NULL,
    declared_at  TIMESTAMPTZ NOT NULL,
    source       TEXT NOT NULL DEFAULT 'operator' CHECK (source IN ('operator', 'automatic')),
    CHECK (ended_at IS NULL OR ended_at >= started_at)
);
CREATE INDEX IF NOT EXISTS region_incidents_by_time ON region_incidents (started_at);
-- At most one open incident per region.
CREATE UNIQUE INDEX IF NOT EXISTS one_open_incident_per_region ON region_incidents (region)
    WHERE ended_at IS NULL;
