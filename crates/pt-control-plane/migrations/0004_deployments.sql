-- Several deployments per reservation (docs/12 §3). Each has its own keys and an optional
-- cap on its share of the reservation's entitlement.
-- Not yet used: the service runs on MemoryStore until the SQL store is implemented.

CREATE TABLE IF NOT EXISTS deployments (
    id          STRING PRIMARY KEY,
    pt_id       STRING NOT NULL REFERENCES provisioned_throughput (id),
    name        STRING NOT NULL,
    max_share   FLOAT8 CHECK (max_share IS NULL OR (max_share > 0 AND max_share <= 1)),
    created_at  TIMESTAMPTZ NOT NULL,
    UNIQUE (pt_id, name),
    INDEX by_reservation (pt_id, created_at)
);

-- Existing reservations get a primary deployment named "default".
INSERT INTO deployments (id, pt_id, name, max_share, created_at)
    SELECT deployment_id, id, 'default', NULL, created_at
    FROM provisioned_throughput
    ON CONFLICT DO NOTHING;

-- Keys belong to a deployment.
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS deployment_id STRING REFERENCES deployments (id);
UPDATE api_keys k SET deployment_id = p.deployment_id
    FROM provisioned_throughput p
    WHERE k.pt_id = p.id AND k.deployment_id IS NULL;
ALTER TABLE api_keys ALTER COLUMN deployment_id SET NOT NULL;
DROP INDEX IF EXISTS api_keys@one_current_key;
CREATE UNIQUE INDEX IF NOT EXISTS one_current_key_per_deployment
    ON api_keys (deployment_id)
    WHERE expires_at IS NULL;

ALTER TABLE provisioned_throughput DROP COLUMN IF EXISTS deployment_id;
