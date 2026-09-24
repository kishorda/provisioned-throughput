//! Durable store over the Postgres protocol (ADR-017): CockroachDB in production
//! (docs/07 §5), PostgreSQL for development and tests.
//!
//! - **Schema** is in `migrations/`, applied by [`SqlStore::migrate`]. Every migration is
//!   idempotent and recorded in `schema_migrations`.
//! - **Rows.** A reservation is one `provisioned_throughput` row plus its `deployments`,
//!   `api_keys` (hashes only), and append-only `provisioned_throughput_events`. Nested
//!   values (regions, shape, policy, price) are JSONB.
//! - **Concurrency.** `update` is `UPDATE … WHERE id = $1 AND version = $2` in a
//!   transaction with the child rows, so a lost race changes nothing.
//! - **Errors.** Unique violations are `AlreadyExists`; everything else (unreachable,
//!   timeouts, serialization retries, undecodable rows) is `Unavailable`, which the API
//!   reports as 503 and the snapshot endpoint never turns into an empty snapshot.
//!
//! Timestamps are stored at microsecond precision. The service's `SystemClock` truncates to
//! microseconds, so a resource reads back exactly as it was written.

use std::collections::HashMap;
use std::time::Duration;

use jiff::Timestamp;
use pt_core::TermMonths;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::types::Json;
use sqlx::{Postgres, Row, Transaction};
use time::OffsetDateTime;

use crate::billing::Invoice;
use crate::model::{ApiKey, Deployment, Event, ProvisionedThroughput, RegionIncident};
use crate::store::{IdempotencyRecord, Store, StoreError};

/// Migrations in order. Never edit one that has shipped; add a new file.
const MIGRATIONS: &[(i64, &str, &str)] = &[
    (1, "initial", include_str!("../migrations/0001_initial.sql")),
    (
        2,
        "invoices",
        include_str!("../migrations/0002_invoices.sql"),
    ),
];

/// Idempotency keys older than this are ignored and may be reused.
const IDEMPOTENCY_TTL: &str = "7 days";

const LIVE_STATES: &str = "('scheduled', 'active', 'pending_cancellation')";

#[derive(Clone)]
pub struct SqlStore {
    pool: PgPool,
}

fn unavailable(e: sqlx::Error) -> StoreError {
    StoreError::Unavailable(e.to_string())
}

/// Unique violations become `AlreadyExists(what)`; anything else is `Unavailable`.
fn classify(e: sqlx::Error, what: &str) -> StoreError {
    match &e {
        sqlx::Error::Database(db) if db.code().as_deref() == Some("23505") => {
            StoreError::AlreadyExists(what.to_string())
        }
        _ => unavailable(e),
    }
}

fn corrupt(what: &str, e: impl std::fmt::Display) -> StoreError {
    StoreError::Unavailable(format!("undecodable {what}: {e}"))
}

fn to_db(t: Timestamp) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp_nanos(t.as_nanosecond())
        .expect("jiff timestamps fit in time's range")
}

fn from_db(t: OffsetDateTime) -> Result<Timestamp, StoreError> {
    Timestamp::from_nanosecond(t.unix_timestamp_nanos()).map_err(|e| corrupt("timestamp", e))
}

/// A snake_case enum as its string form.
fn enum_str<T: Serialize>(v: &T) -> String {
    match serde_json::to_value(v) {
        Ok(Value::String(s)) => s,
        other => panic!("expected a string enum, got {other:?}"),
    }
}

fn parse_enum<T: DeserializeOwned>(s: String, what: &str) -> Result<T, StoreError> {
    serde_json::from_value(Value::String(s)).map_err(|e| corrupt(what, e))
}

fn json<T: Serialize>(v: &T) -> Json<Value> {
    Json(serde_json::to_value(v).expect("model types serialise"))
}

fn from_json<T: DeserializeOwned>(v: Json<Value>, what: &str) -> Result<T, StoreError> {
    serde_json::from_value(v.0).map_err(|e| corrupt(what, e))
}

impl SqlStore {
    /// Connect with a small pool. The URL is `postgres://user[:password]@host:port/db`.
    pub async fn connect(url: &str, max_connections: u32) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
            .map_err(unavailable)?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Apply migrations that haven't run yet. Returns the versions applied now.
    pub async fn migrate(&self) -> Result<Vec<i64>, StoreError> {
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                 version INT8 PRIMARY KEY,
                 name TEXT NOT NULL,
                 applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
             )",
        )
        .execute(&self.pool)
        .await
        .map_err(unavailable)?;
        let done: Vec<i64> = sqlx::query_scalar("SELECT version FROM schema_migrations")
            .fetch_all(&self.pool)
            .await
            .map_err(unavailable)?;
        let mut applied = Vec::new();
        for (version, name, sql) in MIGRATIONS {
            if done.contains(version) {
                continue;
            }
            sqlx::raw_sql(sql)
                .execute(&self.pool)
                .await
                .map_err(unavailable)?;
            sqlx::query(
                "INSERT INTO schema_migrations (version, name) VALUES ($1, $2)
                 ON CONFLICT (version) DO NOTHING",
            )
            .bind(version)
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(unavailable)?;
            tracing::info!(version, name, "applied migration");
            applied.push(*version);
        }
        Ok(applied)
    }

    /// Load full resources for the given main rows (deployments, keys, events).
    async fn assemble(&self, rows: Vec<PgRow>) -> Result<Vec<ProvisionedThroughput>, StoreError> {
        let mut out = rows
            .into_iter()
            .map(pt_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        if out.is_empty() {
            return Ok(out);
        }
        let ids: Vec<String> = out.iter().map(|p| p.id.clone()).collect();

        let mut deployments: HashMap<String, Vec<Deployment>> = HashMap::new();
        let rows = sqlx::query(
            "SELECT id, pt_id, name, max_share, created_at FROM deployments
             WHERE pt_id = ANY($1) ORDER BY pt_id, ordinal",
        )
        .bind(&ids)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        let mut owner: HashMap<String, String> = HashMap::new();
        for r in rows {
            let pt_id: String = r.try_get("pt_id").map_err(|e| corrupt("deployment", e))?;
            let d = Deployment {
                id: r.try_get("id").map_err(|e| corrupt("deployment", e))?,
                name: r.try_get("name").map_err(|e| corrupt("deployment", e))?,
                max_share: r
                    .try_get("max_share")
                    .map_err(|e| corrupt("deployment", e))?,
                api_keys: vec![],
                created_at: from_db(
                    r.try_get("created_at")
                        .map_err(|e| corrupt("deployment", e))?,
                )?,
            };
            owner.insert(d.id.clone(), pt_id.clone());
            deployments.entry(pt_id).or_default().push(d);
        }

        let dep_ids: Vec<String> = owner.keys().cloned().collect();
        let rows = sqlx::query(
            "SELECT id, deployment_id, prefix, sha256, created_at, expires_at FROM api_keys
             WHERE deployment_id = ANY($1) ORDER BY created_at, id",
        )
        .bind(&dep_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        let mut keys: HashMap<String, Vec<ApiKey>> = HashMap::new();
        for r in rows {
            let bad = |e| corrupt("api key", e);
            let expires: Option<OffsetDateTime> = r.try_get("expires_at").map_err(bad)?;
            keys.entry(r.try_get("deployment_id").map_err(bad)?)
                .or_default()
                .push(ApiKey {
                    id: r.try_get("id").map_err(bad)?,
                    prefix: r.try_get("prefix").map_err(bad)?,
                    sha256: r.try_get("sha256").map_err(bad)?,
                    created_at: from_db(r.try_get("created_at").map_err(bad)?)?,
                    expires_at: expires.map(from_db).transpose()?,
                });
        }

        let rows = sqlx::query(
            "SELECT pt_id, at, detail FROM provisioned_throughput_events
             WHERE pt_id = ANY($1) ORDER BY pt_id, seq",
        )
        .bind(&ids)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        let mut events: HashMap<String, Vec<Event>> = HashMap::new();
        for r in rows {
            let bad = |e| corrupt("event", e);
            events
                .entry(r.try_get("pt_id").map_err(bad)?)
                .or_default()
                .push(Event {
                    at: from_db(r.try_get("at").map_err(bad)?)?,
                    kind: from_json(r.try_get("detail").map_err(bad)?, "event")?,
                });
        }

        for pt in &mut out {
            let mut deps = deployments.remove(&pt.id).unwrap_or_default();
            for d in &mut deps {
                d.api_keys = keys.remove(&d.id).unwrap_or_default();
            }
            pt.deployments = deps;
            pt.events = events.remove(&pt.id).unwrap_or_default();
        }
        Ok(out)
    }

    async fn select(
        &self,
        filter: &str,
        binds: &[&str],
    ) -> Result<Vec<ProvisionedThroughput>, StoreError> {
        let sql =
            format!("SELECT * FROM provisioned_throughput WHERE {filter} ORDER BY created_at, id");
        let mut q = sqlx::query(&sql);
        for b in binds {
            q = q.bind(*b);
        }
        let rows = q.fetch_all(&self.pool).await.map_err(unavailable)?;
        self.assemble(rows).await
    }
}

fn pt_from_row(r: PgRow) -> Result<ProvisionedThroughput, StoreError> {
    let bad = |e| corrupt("reservation", e);
    let term: i16 = r.try_get("term_months").map_err(bad)?;
    let version: i64 = r.try_get("version").map_err(bad)?;
    let pending: Option<Json<Value>> = r.try_get("pending_changes").map_err(bad)?;
    let cus: i32 = r.try_get("cus").map_err(bad)?;
    Ok(ProvisionedThroughput {
        id: r.try_get("id").map_err(bad)?,
        tenant: r.try_get("tenant").map_err(bad)?,
        name: r.try_get("name").map_err(bad)?,
        model: r.try_get("model").map_err(bad)?,
        tier: parse_enum(r.try_get("tier").map_err(bad)?, "tier")?,
        regions: from_json(r.try_get("regions").map_err(bad)?, "regions")?,
        cus: cus as u32,
        sku: parse_enum(r.try_get("sku").map_err(bad)?, "sku")?,
        isolation: parse_enum(r.try_get("isolation").map_err(bad)?, "isolation")?,
        shape: from_json(r.try_get("shape").map_err(bad)?, "shape")?,
        boundary_policy: from_json(
            r.try_get("boundary_policy").map_err(bad)?,
            "boundary_policy",
        )?,
        term_months: TermMonths::try_from(term as u8).map_err(|e| corrupt("term_months", e))?,
        term_start: from_db(r.try_get("term_start").map_err(bad)?)?,
        term_end: from_db(r.try_get("term_end").map_err(bad)?)?,
        auto_renew: r.try_get("auto_renew").map_err(bad)?,
        state: parse_enum(r.try_get("state").map_err(bad)?, "state")?,
        pending_changes: pending
            .map(|p| from_json(p, "pending_changes"))
            .transpose()?,
        endpoints: from_json(r.try_get("endpoints").map_err(bad)?, "endpoints")?,
        price: from_json(r.try_get("price").map_err(bad)?, "price")?,
        failover_headroom: from_json(
            r.try_get("failover_headroom").map_err(bad)?,
            "failover_headroom",
        )?,
        deployments: vec![],
        version: version as u64,
        created_at: from_db(r.try_get("created_at").map_err(bad)?)?,
        updated_at: from_db(r.try_get("updated_at").map_err(bad)?)?,
        events: vec![],
    })
}

/// Bind the main row's columns in this order:
/// id, tenant, name, model, tier, sku, isolation, regions, failover_headroom, cus, shape,
/// boundary_policy, term_months, term_start, term_end, auto_renew, state, pending_changes,
/// endpoints, price, version, created_at, updated_at.
macro_rules! bind_pt {
    ($q:expr, $pt:expr) => {
        $q.bind(&$pt.id)
            .bind(&$pt.tenant)
            .bind(&$pt.name)
            .bind(&$pt.model)
            .bind(enum_str(&$pt.tier))
            .bind(enum_str(&$pt.sku))
            .bind(enum_str(&$pt.isolation))
            .bind(json(&$pt.regions))
            .bind(json(&$pt.failover_headroom))
            .bind($pt.cus as i32)
            .bind(json(&$pt.shape))
            .bind(json(&$pt.boundary_policy))
            .bind(i16::from($pt.term_months.months()))
            .bind(to_db($pt.term_start))
            .bind(to_db($pt.term_end))
            .bind($pt.auto_renew)
            .bind(enum_str(&$pt.state))
            .bind($pt.pending_changes.as_ref().map(json))
            .bind(json(&$pt.endpoints))
            .bind(json(&$pt.price))
            .bind($pt.version as i64)
            .bind(to_db($pt.created_at))
            .bind(to_db($pt.updated_at))
    };
}

const COLUMNS: &str = "id, tenant, name, model, tier, sku, isolation, regions, failover_headroom, \
     cus, shape, boundary_policy, term_months, term_start, term_end, auto_renew, state, \
     pending_changes, endpoints, price, version, created_at, updated_at";

/// Replace a reservation's deployments and keys with `pt`'s.
async fn write_children(
    tx: &mut Transaction<'_, Postgres>,
    pt: &ProvisionedThroughput,
) -> Result<(), StoreError> {
    sqlx::query(
        "DELETE FROM api_keys WHERE deployment_id IN (SELECT id FROM deployments WHERE pt_id = $1)",
    )
    .bind(&pt.id)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    sqlx::query("DELETE FROM deployments WHERE pt_id = $1")
        .bind(&pt.id)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    for (ordinal, d) in pt.deployments.iter().enumerate() {
        sqlx::query(
            "INSERT INTO deployments (id, pt_id, ordinal, name, max_share, created_at)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(&d.id)
        .bind(&pt.id)
        .bind(ordinal as i32)
        .bind(&d.name)
        .bind(d.max_share)
        .bind(to_db(d.created_at))
        .execute(&mut **tx)
        .await
        .map_err(|e| classify(e, &format!("deployment {}", d.id)))?;
        for k in &d.api_keys {
            sqlx::query(
                "INSERT INTO api_keys (id, deployment_id, prefix, sha256, created_at, expires_at)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(&k.id)
            .bind(&d.id)
            .bind(&k.prefix)
            .bind(&k.sha256)
            .bind(to_db(k.created_at))
            .bind(k.expires_at.map(to_db))
            .execute(&mut **tx)
            .await
            .map_err(|e| classify(e, &format!("key {}", k.id)))?;
        }
    }
    Ok(())
}

/// Append events beyond the `stored` already written.
async fn append_events(
    tx: &mut Transaction<'_, Postgres>,
    pt: &ProvisionedThroughput,
    stored: usize,
) -> Result<(), StoreError> {
    for (seq, e) in pt.events.iter().enumerate().skip(stored) {
        let detail = json(&e.kind);
        let kind = detail.0["type"].as_str().unwrap_or("unknown").to_string();
        sqlx::query(
            "INSERT INTO provisioned_throughput_events (pt_id, seq, at, kind, detail)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&pt.id)
        .bind(seq as i32)
        .bind(to_db(e.at))
        .bind(kind)
        .bind(detail)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    }
    Ok(())
}

fn incident_from_row(r: PgRow) -> Result<RegionIncident, StoreError> {
    let bad = |e| corrupt("incident", e);
    let ended: Option<OffsetDateTime> = r.try_get("ended_at").map_err(bad)?;
    Ok(RegionIncident {
        id: r.try_get("id").map_err(bad)?,
        region: r.try_get("region").map_err(bad)?,
        started_at: from_db(r.try_get("started_at").map_err(bad)?)?,
        ended_at: ended.map(from_db).transpose()?,
        description: r.try_get("description").map_err(bad)?,
        declared_at: from_db(r.try_get("declared_at").map_err(bad)?)?,
        source: parse_enum(r.try_get("source").map_err(bad)?, "source")?,
    })
}

impl Store for SqlStore {
    async fn insert(&self, pt: ProvisionedThroughput) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let placeholders = (1..=23)
            .map(|i| format!("${i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("INSERT INTO provisioned_throughput ({COLUMNS}) VALUES ({placeholders})");
        bind_pt!(sqlx::query(&sql), pt)
            .execute(&mut *tx)
            .await
            .map_err(|e| classify(e, &pt.id))?;
        write_children(&mut tx, &pt).await?;
        append_events(&mut tx, &pt, 0).await?;
        tx.commit().await.map_err(unavailable)
    }

    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<ProvisionedThroughput>, StoreError> {
        Ok(self
            .select("tenant = $1 AND id = $2", &[tenant, id])
            .await?
            .pop())
    }

    async fn list(&self, tenant: &str) -> Result<Vec<ProvisionedThroughput>, StoreError> {
        self.select("tenant = $1", &[tenant]).await
    }

    async fn list_live(&self) -> Result<Vec<ProvisionedThroughput>, StoreError> {
        self.select(&format!("state IN {LIVE_STATES}"), &[]).await
    }

    async fn update(
        &self,
        pt: ProvisionedThroughput,
        expected_version: u64,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        // $1 is the id; $2..$23 are the other columns; $24 is the expected version.
        let sets = COLUMNS
            .split(", ")
            .enumerate()
            .skip(1)
            .map(|(i, c)| format!("{} = ${}", c.trim(), i + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let sql =
            format!("UPDATE provisioned_throughput SET {sets} WHERE id = $1 AND version = $24");
        let done = bind_pt!(sqlx::query(&sql), pt)
            .bind(expected_version as i64)
            .execute(&mut *tx)
            .await
            .map_err(|e| classify(e, &format!("a live reservation named {}", pt.name)))?;
        if done.rows_affected() == 0 {
            let exists: Option<i64> =
                sqlx::query_scalar("SELECT version FROM provisioned_throughput WHERE id = $1")
                    .bind(&pt.id)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(unavailable)?;
            return Err(match exists {
                Some(_) => StoreError::VersionConflict(pt.id),
                None => StoreError::NotFound(pt.id),
            });
        }
        write_children(&mut tx, &pt).await?;
        let stored: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM provisioned_throughput_events WHERE pt_id = $1",
        )
        .bind(&pt.id)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        append_events(&mut tx, &pt, stored as usize).await?;
        tx.commit().await.map_err(unavailable)
    }

    async fn idempotency_get(
        &self,
        tenant: &str,
        key: &str,
    ) -> Result<Option<IdempotencyRecord>, StoreError> {
        let row = sqlx::query(&format!(
            "SELECT resource_id, fingerprint FROM idempotency_keys
             WHERE tenant = $1 AND idem_key = $2 AND created_at > now() - INTERVAL '{IDEMPOTENCY_TTL}'"
        ))
        .bind(tenant)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        row.map(|r| {
            Ok(IdempotencyRecord {
                resource_id: r
                    .try_get("resource_id")
                    .map_err(|e| corrupt("idempotency", e))?,
                fingerprint: r
                    .try_get("fingerprint")
                    .map_err(|e| corrupt("idempotency", e))?,
            })
        })
        .transpose()
    }

    async fn idempotency_put(
        &self,
        tenant: &str,
        key: &str,
        record: IdempotencyRecord,
    ) -> Result<(), StoreError> {
        // Insert, or take over an expired key. A live key is left alone.
        let done = sqlx::query(&format!(
            "INSERT INTO idempotency_keys (tenant, idem_key, resource_id, fingerprint, created_at)
             VALUES ($1, $2, $3, $4, now())
             ON CONFLICT (tenant, idem_key) DO UPDATE
                 SET resource_id = excluded.resource_id,
                     fingerprint = excluded.fingerprint,
                     created_at = excluded.created_at
                 WHERE idempotency_keys.created_at <= now() - INTERVAL '{IDEMPOTENCY_TTL}'"
        ))
        .bind(tenant)
        .bind(key)
        .bind(&record.resource_id)
        .bind(&record.fingerprint)
        .execute(&self.pool)
        .await
        .map_err(unavailable)?;
        if done.rows_affected() == 0 {
            return Err(StoreError::AlreadyExists(format!("idempotency key {key}")));
        }
        Ok(())
    }

    async fn insert_incident(&self, incident: RegionIncident) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO region_incidents
                 (id, region, started_at, ended_at, description, declared_at, source)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(&incident.id)
        .bind(&incident.region)
        .bind(to_db(incident.started_at))
        .bind(incident.ended_at.map(to_db))
        .bind(&incident.description)
        .bind(to_db(incident.declared_at))
        .bind(enum_str(&incident.source))
        .execute(&self.pool)
        .await
        .map_err(|e| classify(e, &format!("an open incident for {}", incident.region)))?;
        Ok(())
    }

    async fn update_incident(&self, incident: RegionIncident) -> Result<(), StoreError> {
        let done = sqlx::query(
            "UPDATE region_incidents
             SET region = $2, started_at = $3, ended_at = $4, description = $5,
                 declared_at = $6, source = $7
             WHERE id = $1",
        )
        .bind(&incident.id)
        .bind(&incident.region)
        .bind(to_db(incident.started_at))
        .bind(incident.ended_at.map(to_db))
        .bind(&incident.description)
        .bind(to_db(incident.declared_at))
        .bind(enum_str(&incident.source))
        .execute(&self.pool)
        .await
        .map_err(|e| classify(e, &format!("an open incident for {}", incident.region)))?;
        if done.rows_affected() == 0 {
            return Err(StoreError::NotFound(incident.id));
        }
        Ok(())
    }

    async fn insert_invoice(&self, invoice: Invoice) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO invoices (id, tenant, period, total, currency, finalized_at, document)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(&invoice.id)
        .bind(&invoice.tenant)
        .bind(&invoice.period)
        .bind(invoice.total)
        .bind(&invoice.currency)
        .bind(invoice.finalized_at.map(to_db))
        .bind(json(&invoice))
        .execute(&self.pool)
        .await
        .map_err(|e| classify(e, &invoice.id))?;
        Ok(())
    }

    async fn get_invoice(&self, tenant: &str, period: &str) -> Result<Option<Invoice>, StoreError> {
        let doc: Option<Json<Value>> =
            sqlx::query_scalar("SELECT document FROM invoices WHERE tenant = $1 AND period = $2")
                .bind(tenant)
                .bind(period)
                .fetch_optional(&self.pool)
                .await
                .map_err(unavailable)?;
        doc.map(|d| from_json(d, "invoice")).transpose()
    }

    async fn list_invoices(&self, tenant: &str) -> Result<Vec<Invoice>, StoreError> {
        let docs: Vec<Json<Value>> = sqlx::query_scalar(
            "SELECT document FROM invoices WHERE tenant = $1 ORDER BY period DESC",
        )
        .bind(tenant)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        docs.into_iter().map(|d| from_json(d, "invoice")).collect()
    }

    async fn list_incidents(&self) -> Result<Vec<RegionIncident>, StoreError> {
        sqlx::query("SELECT * FROM region_incidents ORDER BY started_at, id")
            .fetch_all(&self.pool)
            .await
            .map_err(unavailable)?
            .into_iter()
            .map(incident_from_row)
            .collect()
    }
}
