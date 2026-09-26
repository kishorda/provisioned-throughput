-- Capacity pools counted in micro-replicas, so CUs of different tiers draw what they
-- really cost (ADR-031). The old CU columns (`capacity`, `reserved`) stay, unused, for a
-- rollback; the old code's reconcile loop corrects them within two runs.
ALTER TABLE capacity_pools ADD COLUMN IF NOT EXISTS capacity_micro INT8 NOT NULL DEFAULT 0;
ALTER TABLE capacity_pools ADD COLUMN IF NOT EXISTS reserved_micro INT8 NOT NULL DEFAULT 0;
-- False until reserved_micro has been computed from live reservations. The first instance
-- to start with this code does that once, under a row lock (SqlPlanner::restore).
ALTER TABLE capacity_pools ADD COLUMN IF NOT EXISTS micro_counted BOOL NOT NULL DEFAULT false;
