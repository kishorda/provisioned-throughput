-- CockroachDB schema for the control-plane store (docs/12 §5).
-- Not yet used: the service runs on MemoryStore until the SQL store is implemented.
-- The global database spans three regions (docs/07 §5); tenant rows live near the tenant.

CREATE TABLE IF NOT EXISTS provisioned_throughput (
    id               STRING PRIMARY KEY,
    tenant           STRING NOT NULL,
    name             STRING NOT NULL,
    model            STRING NOT NULL,
    tier             STRING NOT NULL CHECK (tier IN ('interactive', 'agentic', 'standard')),
    sku              STRING NOT NULL CHECK (sku IN ('regional', 'multi_region')),
    isolation        STRING NOT NULL CHECK (isolation IN ('shared', 'dedicated', 'strict_dedicated')),
    regions          JSONB NOT NULL,        -- [{"region": "eu-west", "cus": 10}, ...]
    cus              INT4 NOT NULL CHECK (cus >= 1),
    shape            JSONB NOT NULL,
    boundary_policy  JSONB NOT NULL,
    term_months      INT2 NOT NULL CHECK (term_months IN (1, 3, 6)),
    term_start       TIMESTAMPTZ NOT NULL,
    term_end         TIMESTAMPTZ NOT NULL,
    auto_renew       BOOL NOT NULL,
    state            STRING NOT NULL
                     CHECK (state IN ('scheduled', 'active', 'pending_cancellation', 'ended', 'cancelled')),
    pending_changes  JSONB,
    deployment_id    STRING NOT NULL UNIQUE,
    endpoints        JSONB NOT NULL,
    price            JSONB NOT NULL,
    api_key_sha256   STRING NOT NULL UNIQUE,  -- the key itself is never stored
    version          INT8 NOT NULL,           -- optimistic concurrency; the API's ETag
    created_at       TIMESTAMPTZ NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL,
    INDEX by_tenant (tenant, created_at),
    INDEX by_lifecycle (state, term_end) WHERE state IN ('scheduled', 'active', 'pending_cancellation')
);

-- Names are unique per tenant among live reservations only.
CREATE UNIQUE INDEX IF NOT EXISTS live_name_per_tenant
    ON provisioned_throughput (tenant, name)
    WHERE state IN ('scheduled', 'active', 'pending_cancellation');

-- Audit and billing trail. Append-only.
CREATE TABLE IF NOT EXISTS provisioned_throughput_events (
    pt_id   STRING NOT NULL REFERENCES provisioned_throughput (id),
    seq     INT4 NOT NULL,
    at      TIMESTAMPTZ NOT NULL,
    kind    STRING NOT NULL,
    detail  JSONB NOT NULL,
    PRIMARY KEY (pt_id, seq)
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant       STRING NOT NULL,
    key          STRING NOT NULL,
    resource_id  STRING NOT NULL,
    fingerprint  STRING NOT NULL,           -- SHA-256 of the request body
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant, key)
) WITH (ttl_expiration_expression = 'created_at + INTERVAL ''7 days''');
