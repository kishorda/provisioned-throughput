-- Inference API keys with rotation (docs/12 §3). Replaces the single api_key_sha256 column.
-- Not yet used: the service runs on MemoryStore until the SQL store is implemented.

CREATE TABLE IF NOT EXISTS api_keys (
    id          STRING PRIMARY KEY,
    pt_id       STRING NOT NULL REFERENCES provisioned_throughput (id),
    prefix      STRING NOT NULL,
    sha256      STRING NOT NULL UNIQUE,   -- the key itself is never stored
    created_at  TIMESTAMPTZ NOT NULL,
    expires_at  TIMESTAMPTZ,              -- NULL for the current key
    INDEX by_reservation (pt_id)
);

-- Exactly one current key per reservation.
CREATE UNIQUE INDEX IF NOT EXISTS one_current_key
    ON api_keys (pt_id)
    WHERE expires_at IS NULL;

INSERT INTO api_keys (id, pt_id, prefix, sha256, created_at, expires_at)
    SELECT 'key-' || substr(id, 4), id, '', api_key_sha256, created_at, NULL
    FROM provisioned_throughput
    ON CONFLICT DO NOTHING;

ALTER TABLE provisioned_throughput DROP COLUMN IF EXISTS api_key_sha256;
