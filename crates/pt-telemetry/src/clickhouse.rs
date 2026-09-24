//! Durable usage records in ClickHouse (docs/09 §5, ADR-019).
//!
//! Talks to ClickHouse's HTTP interface with `reqwest`, so there's no native driver or C
//! dependency. The schema has one table:
//!
//! - `ReplacingMergeTree`, partitioned by month and ordered by
//!   `(tenant, reservation, at_ms, request_id)`, so a reservation's time range is a narrow
//!   scan.
//! - `record` holds the full `UsageRecord` as JSON, the source of truth for reads. The
//!   other columns (tokens, WU, class, outcome) exist for SQL aggregation and debugging.
//! - Retention is a table TTL, set from `retention_days` at every startup, so `prune` is a
//!   no-op.
//!
//! **Exactly once.** `append` skips request ids already stored (a bloom-filter index keeps
//! the lookup cheap) and duplicates inside a batch. Two concurrent appends of the same id
//! can both insert, so reads take one row per `request_id` (`LIMIT 1 BY`), and background
//! merges collapse identical rows.

use std::collections::HashSet;
use std::time::Duration;

use pt_core::{Outcome, TrafficClass, UsageRecord};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::store::{IngestResult, StoredRecord, UsageError, UsageStore};

/// Where the usage table lives.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseConfig {
    /// HTTP interface, for example `http://127.0.0.1:8123`.
    pub url: String,
    #[serde(default = "default_database")]
    pub database: String,
    #[serde(default = "default_user")]
    pub user: String,
    /// Prefer `PT_CLICKHOUSE_PASSWORD` over putting it in a file.
    #[serde(default)]
    pub password: Option<String>,
    /// PEM file of the CA that signed ClickHouse's certificate, for an `https://` URL with a
    /// private CA. Public roots are always trusted.
    #[serde(default)]
    pub ca_cert: Option<String>,
    /// PEM files for mutual TLS: the client certificate (chain) and its private key.
    #[serde(default)]
    pub client_cert: Option<String>,
    #[serde(default)]
    pub client_key: Option<String>,
    /// Allow `http://` to a non-loopback host. Off by default (ADR-021).
    #[serde(default)]
    pub allow_insecure_transport: bool,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            database: default_database(),
            user: default_user(),
            password: None,
            ca_cert: None,
            client_cert: None,
            client_key: None,
            allow_insecure_transport: false,
        }
    }
}

/// Whether `url` protects the connection well enough: `https`, a loopback host, or
/// `allow_insecure`.
pub fn check_transport(url: &str, allow_insecure: bool) -> Result<(), String> {
    let parsed = reqwest::Url::parse(url).map_err(|e| format!("invalid ClickHouse URL: {e}"))?;
    let loopback = parsed.host_str().is_some_and(|h| {
        let h = h.trim_start_matches('[').trim_end_matches(']');
        h.eq_ignore_ascii_case("localhost")
            || h.parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if parsed.scheme() == "https" || loopback || allow_insecure {
        Ok(())
    } else {
        Err(format!(
            "ClickHouse at {url} isn't reached over TLS. Use an https:// URL (with ca_cert for a private CA), or set allow_insecure_transport = true"
        ))
    }
}

fn default_database() -> String {
    "pt".into()
}
fn default_user() -> String {
    "default".into()
}

const TABLE: &str = "usage_records";

pub struct ClickHouseUsageStore {
    config: ClickHouseConfig,
    /// Records older than this are deleted by the table's TTL.
    retention_days: u64,
    http: reqwest::Client,
}

fn class_str(c: Option<TrafficClass>) -> &'static str {
    match c {
        Some(c) => c.as_str(),
        None => "",
    }
}

fn outcome_str(o: &Outcome) -> &'static str {
    match o {
        Outcome::Ok => "ok",
        Outcome::Rejected(_) => "rejected",
        Outcome::ClientCancelled => "client_cancelled",
        Outcome::Error(_) => "error",
    }
}

impl ClickHouseUsageStore {
    /// Checks the transport policy and loads any CA and client certificates.
    pub fn new(config: ClickHouseConfig, retention_days: u64) -> Result<Self, UsageError> {
        check_transport(&config.url, config.allow_insecure_transport).map_err(UsageError)?;
        let read = |path: &str| {
            std::fs::read(path).map_err(|e| UsageError(format!("reading {path}: {e}")))
        };
        let mut builder = reqwest::Client::builder()
            .use_rustls_tls()
            .timeout(Duration::from_secs(30));
        if let Some(ca) = &config.ca_cert {
            let cert = reqwest::Certificate::from_pem(&read(ca)?)
                .map_err(|e| UsageError(format!("{ca}: {e}")))?;
            builder = builder.add_root_certificate(cert);
        }
        match (&config.client_cert, &config.client_key) {
            (Some(cert), Some(key)) => {
                let mut pem = read(cert)?;
                pem.push(b'\n');
                pem.extend(read(key)?);
                let identity = reqwest::Identity::from_pem(&pem)
                    .map_err(|e| UsageError(format!("client certificate: {e}")))?;
                builder = builder.identity(identity);
            }
            (None, None) => {}
            _ => {
                return Err(UsageError(
                    "set both client_cert and client_key, or neither".into(),
                ))
            }
        }
        let http = builder.build().map_err(|e| UsageError(e.to_string()))?;
        Ok(Self {
            config,
            retention_days,
            http,
        })
    }

    /// Run `sql`. `params` become `{name:Type}` query parameters, so values are never
    /// spliced into SQL. `body` is sent after the query (for INSERT … FORMAT JSONEachRow).
    async fn run(
        &self,
        sql: &str,
        params: &[(&str, String)],
        body: Option<String>,
        database: bool,
    ) -> Result<String, UsageError> {
        let mut query: Vec<(String, String)> = vec![
            ("output_format_json_quote_64bit_integers".into(), "0".into()),
            ("date_time_input_format".into(), "best_effort".into()),
        ];
        if database {
            query.push(("database".into(), self.config.database.clone()));
        }
        for (k, v) in params {
            query.push((format!("param_{k}"), v.clone()));
        }
        let (url_query, payload) = match body {
            Some(b) => {
                query.push(("query".into(), sql.to_string()));
                (query, b)
            }
            None => (query, sql.to_string()),
        };
        let password = std::env::var("PT_CLICKHOUSE_PASSWORD")
            .ok()
            .or_else(|| self.config.password.clone())
            .unwrap_or_default();
        let resp = self
            .http
            .post(format!("{}/", self.config.url.trim_end_matches('/')))
            .query(&url_query)
            .header("X-ClickHouse-User", &self.config.user)
            .header("X-ClickHouse-Key", password)
            .body(payload)
            .send()
            .await
            .map_err(|e| UsageError(format!("clickhouse: {e}")))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| UsageError(format!("clickhouse: {e}")))?;
        if !status.is_success() {
            return Err(UsageError(format!(
                "clickhouse {status}: {}",
                text.lines().next().unwrap_or("")
            )));
        }
        Ok(text)
    }

    /// Create the database and table if needed, and set the retention TTL. Idempotent.
    pub async fn migrate(&self) -> Result<(), UsageError> {
        let db = &self.config.database;
        if !db.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(UsageError(format!("invalid database name {db}")));
        }
        self.run(
            &format!("CREATE DATABASE IF NOT EXISTS {db}"),
            &[],
            None,
            false,
        )
        .await?;
        let days = self.retention_days.max(1);
        self.run(
            &format!(
                "CREATE TABLE IF NOT EXISTS {TABLE} (
                    request_id        UUID,
                    tenant            LowCardinality(String),
                    reservation       String,
                    deployment        String,
                    region            LowCardinality(String),
                    at_ms             Int64,
                    at                DateTime64(3, 'UTC') MATERIALIZED fromUnixTimestamp64Milli(at_ms, 'UTC'),
                    class             LowCardinality(String),
                    outcome           LowCardinality(String),
                    in_shape          Bool,
                    uncached_prefill  UInt64,
                    cached_prefill    UInt64,
                    decode            UInt64,
                    wu_actual         Float64,
                    record            String CODEC(ZSTD(3)),
                    INDEX request_id_bf request_id TYPE bloom_filter(0.001) GRANULARITY 4
                ) ENGINE = ReplacingMergeTree
                PARTITION BY toYYYYMM(at)
                ORDER BY (tenant, reservation, at_ms, request_id)
                TTL toDateTime(at) + INTERVAL {days} DAY"
            ),
            &[],
            None,
            true,
        )
        .await?;
        // Keep the TTL in step with configuration, without rewriting existing parts now.
        self.run(
            &format!(
                "ALTER TABLE {TABLE} MODIFY TTL toDateTime(at) + INTERVAL {days} DAY
                 SETTINGS materialize_ttl_after_modify = 0"
            ),
            &[],
            None,
            true,
        )
        .await?;
        Ok(())
    }

    /// Request ids from `ids` already stored.
    async fn existing(&self, ids: &[String]) -> Result<HashSet<String>, UsageError> {
        if ids.is_empty() {
            return Ok(HashSet::new());
        }
        let list = format!(
            "[{}]",
            ids.iter()
                .map(|i| format!("'{i}'"))
                .collect::<Vec<_>>()
                .join(",")
        );
        let out = self
            .run(
                &format!(
                    "SELECT DISTINCT toString(request_id) AS id FROM {TABLE}
                     WHERE request_id IN {{ids:Array(UUID)}} FORMAT JSONEachRow"
                ),
                &[("ids", list)],
                None,
                true,
            )
            .await?;
        out.lines()
            .filter(|l| !l.is_empty())
            .map(|l| {
                serde_json::from_str::<Value>(l)
                    .ok()
                    .and_then(|v| v["id"].as_str().map(str::to_owned))
                    .ok_or_else(|| UsageError(format!("clickhouse: bad row {l}")))
            })
            .collect()
    }
}

impl UsageStore for ClickHouseUsageStore {
    async fn append(
        &self,
        region: &str,
        records: Vec<UsageRecord>,
        now_ms: u64,
    ) -> Result<IngestResult, UsageError> {
        let ids: Vec<String> = records.iter().map(|r| r.request_id.to_string()).collect();
        let stored = self.existing(&ids).await?;
        let mut seen = HashSet::new();
        let mut result = IngestResult::default();
        let mut body = String::new();
        for r in records {
            let id = r.request_id.to_string();
            if stored.contains(&id) || !seen.insert(id.clone()) {
                result.duplicates += 1;
                continue;
            }
            let at_ms = if r.received_at_ms == 0 {
                now_ms
            } else {
                r.received_at_ms
            };
            let row = json!({
                "request_id": id,
                "tenant": r.tenant,
                "reservation": r.reservation,
                "deployment": r.deployment,
                "region": region,
                "at_ms": at_ms,
                "class": class_str(r.class),
                "outcome": outcome_str(&r.outcome),
                "in_shape": r.in_shape,
                "uncached_prefill": r.tokens.uncached_prefill,
                "cached_prefill": r.tokens.cached_prefill,
                "decode": r.tokens.decode,
                "wu_actual": r.wu_actual,
                "record": serde_json::to_string(&r).expect("usage records serialise"),
            });
            body.push_str(&row.to_string());
            body.push('\n');
            result.accepted += 1;
        }
        if result.accepted > 0 {
            self.run(
                &format!("INSERT INTO {TABLE} FORMAT JSONEachRow"),
                &[],
                Some(body),
                true,
            )
            .await?;
        }
        Ok(result)
    }

    async fn range(
        &self,
        tenant: &str,
        reservation: &str,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<StoredRecord>, UsageError> {
        let out = self
            .run(
                &format!(
                    "SELECT region, at_ms, record FROM {TABLE}
                     WHERE tenant = {{tenant:String}} AND reservation = {{reservation:String}}
                       AND at_ms >= {{from:Int64}} AND at_ms < {{to:Int64}}
                     ORDER BY at_ms, request_id
                     LIMIT 1 BY request_id
                     FORMAT JSONEachRow"
                ),
                &[
                    ("tenant", tenant.to_string()),
                    ("reservation", reservation.to_string()),
                    ("from", from_ms.to_string()),
                    ("to", to_ms.to_string()),
                ],
                None,
                true,
            )
            .await?;
        #[derive(Deserialize)]
        struct Row {
            region: String,
            at_ms: u64,
            record: String,
        }
        out.lines()
            .filter(|l| !l.is_empty())
            .map(|l| {
                let row: Row = serde_json::from_str(l)
                    .map_err(|e| UsageError(format!("clickhouse row: {e}")))?;
                let record = serde_json::from_str(&row.record)
                    .map_err(|e| UsageError(format!("stored usage record: {e}")))?;
                Ok(StoredRecord {
                    region: row.region,
                    at_ms: row.at_ms,
                    record,
                })
            })
            .collect()
    }

    /// Retention is the table TTL (`retention_days`), so there's nothing to do here.
    async fn prune(&self, _before_ms: u64) -> Result<usize, UsageError> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_policy() {
        let ok = |u: &str| check_transport(u, false).is_ok();
        assert!(ok("http://127.0.0.1:8123"), "loopback");
        assert!(ok("http://localhost:8123"));
        assert!(ok("http://[::1]:8123"));
        assert!(ok("https://clickhouse.internal:8443"));
        assert!(!ok("http://clickhouse.internal:8123"));
        assert!(!ok("http://10.0.0.7:8123"));
        assert!(
            check_transport("http://10.0.0.7:8123", true).is_ok(),
            "explicitly allowed"
        );
        assert!(check_transport("not a url", true).is_err());
        // Refused at construction, before any request.
        let insecure = ClickHouseConfig {
            url: "http://10.0.0.7:8123".into(),
            ..Default::default()
        };
        assert!(ClickHouseUsageStore::new(insecure, 35).is_err());
        let half_mtls = ClickHouseConfig {
            url: "https://ch.internal".into(),
            client_cert: Some("/nonexistent.pem".into()),
            ..Default::default()
        };
        assert!(ClickHouseUsageStore::new(half_mtls, 35).is_err());
    }
}
