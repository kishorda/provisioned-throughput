-- Sales paused in a pool while its capacity is down, for example during an expedited drain
-- (ADR-041). Placed and renewed by the region's capacity controller; expired rows are
-- ignored and pruned.
CREATE TABLE IF NOT EXISTS sales_holds (
    region      TEXT NOT NULL,
    model       TEXT NOT NULL,
    source      TEXT NOT NULL,
    reason      TEXT NOT NULL,
    expires_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (region, model, source)
);
