//! HTTP behaviour: status codes, headers, error format, auth.

mod common;

use common::*;
use pt_control_plane::clock::ManualClock;
use pt_control_plane::{api, in_memory};
use serde_json::{json, Value};

async fn spawn() -> String {
    let svc = in_memory(config(), ManualClock::new(t0()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, api::router(svc)).await });
    url
}

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

fn body() -> Value {
    serde_json::to_value(request("agents", &[("eu-west", 10)])).unwrap()
}

fn h<'a>(r: &'a reqwest::Response, name: &str) -> &'a str {
    r.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

#[tokio::test]
async fn full_lifecycle_over_http() {
    let base = spawn().await;
    let pts = format!("{base}/v1/provisioned-throughput");

    let created = client()
        .post(&pts)
        .bearer_auth(ACME_KEY)
        .json(&body())
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    assert_eq!(h(&created, "etag"), "\"1\"");
    let location = h(&created, "location").to_string();
    let v: Value = created.json().await.unwrap();
    let id = v["id"].as_str().unwrap().to_string();
    assert_eq!(location, format!("/v1/provisioned-throughput/{id}"));
    assert!(v["api_key"].as_str().unwrap().starts_with("ptk_"));
    assert!(v.get("api_key_sha256").is_none());
    assert!(
        v["deployments"][0]["api_keys"][0].get("sha256").is_none(),
        "hashes are never returned"
    );
    assert_eq!(
        v["deployments"][0]["api_keys"][0]["prefix"]
            .as_str()
            .unwrap()
            .len(),
        12
    );
    assert_eq!(v["state"], "active");
    assert_eq!(v["price"]["monthly"], 10 * BASE * 3 / 2);

    let got = client()
        .get(format!("{base}{location}"))
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(got.status(), 200);
    let v: Value = got.json().await.unwrap();
    assert!(v.get("api_key").is_none(), "the key is only returned once");

    let listed: Value = client()
        .get(&pts)
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["data"].as_array().unwrap().len(), 1);

    // Stale If-Match
    let stale = client()
        .patch(format!("{pts}/{id}"))
        .bearer_auth(ACME_KEY)
        .header("if-match", "\"7\"")
        .json(&json!({ "regions": [{ "region": "eu-west", "cus": 12 }] }))
        .send()
        .await
        .unwrap();
    assert_eq!(stale.status(), 412);

    let patched = client()
        .patch(format!("{pts}/{id}"))
        .bearer_auth(ACME_KEY)
        .header("if-match", "\"1\"")
        .json(&json!({ "regions": [{ "region": "eu-west", "cus": 12 }] }))
        .send()
        .await
        .unwrap();
    assert_eq!(patched.status(), 200);
    assert_eq!(h(&patched, "etag"), "\"2\"");
    assert_eq!(patched.json::<Value>().await.unwrap()["cus"], 12);

    let deleted = client()
        .delete(format!("{pts}/{id}"))
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(
        deleted.status(),
        202,
        "mid-term delete takes effect at term end"
    );
    assert_eq!(
        deleted.json::<Value>().await.unwrap()["state"],
        "pending_cancellation"
    );
}

#[tokio::test]
async fn errors_are_json_with_codes() {
    let base = spawn().await;
    let pts = format!("{base}/v1/provisioned-throughput");

    let r = client().get(&pts).send().await.unwrap();
    assert_eq!(r.status(), 401);
    assert_eq!(
        r.json::<Value>().await.unwrap()["error"]["code"],
        "invalid_api_key"
    );

    let r = client()
        .post(&pts)
        .bearer_auth(ACME_KEY)
        .body("{not json")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 422);
    assert_eq!(
        r.json::<Value>().await.unwrap()["error"]["code"],
        "invalid_json"
    );

    let mut b = body();
    b["term_months"] = json!(12);
    let r = client()
        .post(&pts)
        .bearer_auth(ACME_KEY)
        .json(&b)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 422);
    assert!(r.text().await.unwrap().contains("1, 3, or 6"));

    let mut b = body();
    b["regions"] = json!([{ "region": "eu-west", "cus": 500 }]);
    let r = client()
        .post(&pts)
        .bearer_auth(ACME_KEY)
        .json(&b)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 409);
    assert_eq!(
        r.json::<Value>().await.unwrap()["error"]["code"],
        "capacity_unavailable"
    );

    let mut b = body();
    b["tier"] = json!("premium");
    let r = client()
        .post(&pts)
        .bearer_auth(ACME_KEY)
        .json(&b)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 422);

    let r = client()
        .get(format!("{pts}/pt-missing"))
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    assert_eq!(
        r.json::<Value>().await.unwrap()["error"]["type"],
        "not_found_error"
    );
}

#[tokio::test]
async fn idempotency_key_replays_with_200() {
    let base = spawn().await;
    let pts = format!("{base}/v1/provisioned-throughput");
    let send = || {
        client()
            .post(&pts)
            .bearer_auth(ACME_KEY)
            .header("idempotency-key", "order-42")
            .json(&body())
            .send()
    };
    let first = send().await.unwrap();
    assert_eq!(first.status(), 201);
    let second = send().await.unwrap();
    assert_eq!(second.status(), 200);
    let v: Value = second.json().await.unwrap();
    assert!(v.get("api_key").is_none());
}

#[tokio::test]
async fn model_catalog() {
    let base = spawn().await;
    let v: Value = client()
        .get(format!("{base}/v1/models"))
        .bearer_auth(ACME_KEY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let models = v["data"].as_array().unwrap();
    assert_eq!(models.len(), 2);
    assert_eq!(models[0]["id"], MAVERICK);
    assert_eq!(models[0]["regions"].as_array().unwrap().len(), 3);
    assert_eq!(models[1]["tiers"], json!(["interactive", "standard"]));
}

const EU_WEST_TOKEN: &str = "region-token-eu-west-dev";

async fn spawn_with_svc() -> (
    String,
    std::sync::Arc<
        pt_control_plane::Service<
            pt_control_plane::store::MemoryStore,
            pt_control_plane::planner::MemoryPlanner,
            ManualClock,
        >,
    >,
) {
    let svc = in_memory(config(), ManualClock::new(t0()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = api::router(svc.clone());
    tokio::spawn(async move { axum::serve(listener, app).await });
    (url, svc)
}

async fn fetch_snapshot(
    base: &str,
    etag: Option<&str>,
    wait: u64,
) -> (u16, Option<pt_entitlement::Snapshot>, String) {
    let mut req = client()
        .get(format!(
            "{base}/internal/v1/entitlements/eu-west?wait={wait}"
        ))
        .bearer_auth(EU_WEST_TOKEN);
    if let Some(e) = etag {
        req = req.header("if-none-match", e);
    }
    let r = req.send().await.unwrap();
    let status = r.status().as_u16();
    let tag = h(&r, "etag").to_string();
    if status != 200 {
        return (status, None, tag);
    }
    let sig = h(&r, pt_entitlement::SIGNATURE_HEADER).to_string();
    let key_id = h(&r, pt_entitlement::KEY_ID_HEADER).to_string();
    let body = r.bytes().await.unwrap();
    let signer =
        pt_entitlement::SnapshotSigner::from_hex(&config().entitlements.signing_key).unwrap();
    assert_eq!(key_id, signer.key_id(), "the signing key's id is sent");
    let (snap, _) = pt_entitlement::SnapshotVerifier::from_hex(&signer.public_key_hex())
        .unwrap()
        .verify_with(&body, &sig, Some(&key_id))
        .expect("signature verifies");
    (status, Some(snap), tag)
}

#[tokio::test]
async fn entitlement_snapshots_are_signed_and_long_poll() {
    let (base, svc) = spawn_with_svc().await;

    let (status, snap, tag) = fetch_snapshot(&base, None, 0).await;
    assert_eq!(status, 200);
    let snap = snap.unwrap();
    assert_eq!(snap.region, "eu-west");
    assert!(snap.reservations.is_empty());
    assert_eq!(tag, format!("\"{}\"", snap.version));

    // Unchanged: 304 immediately without a wait.
    assert_eq!(fetch_snapshot(&base, Some(&tag), 0).await.0, 304);

    // A long poll returns as soon as something changes.
    let waiter = tokio::spawn({
        let base = base.clone();
        let tag = tag.clone();
        async move { fetch_snapshot(&base, Some(&tag), 30).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let out = svc
        .create(
            ACME,
            None,
            request("agents", &[("eu-west", 4), ("us-east", 2)]),
        )
        .await
        .unwrap();
    let (status, snap, _) = tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
        .await
        .expect("long poll woke up")
        .unwrap();
    assert_eq!(status, 200);
    let snap = snap.unwrap();
    assert!(snap.version > 0);
    assert_eq!(snap.reservations.len(), 1);
    let r = &snap.reservations[0];
    assert_eq!(r.id, out.resource.id);
    assert_eq!(r.cus, 4, "only this region's share");
    assert_eq!(r.profile, "llama-4-maverick.b200.trtllm-1.2.tp8");
    let d = &snap.deployments[0];
    assert_eq!(d.id, out.resource.primary().id);
    assert_eq!(
        d.api_key_sha256,
        pt_entitlement::sha256_hex(out.api_key.unwrap().as_bytes())
    );
}

#[tokio::test]
async fn snapshots_include_only_serving_reservations() {
    let (base, svc) = spawn_with_svc().await;
    let mut later = request("later", &[("eu-west", 1)]);
    later.start_at = Some(t0() + jiff::SignedDuration::from_hours(24));
    svc.create(ACME, None, later).await.unwrap();
    let other_region = svc
        .create(ACME, None, request("us", &[("us-east", 1)]))
        .await
        .unwrap();
    let cancelling = svc
        .create(ACME, None, request("cancelling", &[("eu-west", 1)]))
        .await
        .unwrap();
    svc.delete(ACME, &cancelling.resource.id, None)
        .await
        .unwrap();

    let snap = fetch_snapshot(&base, None, 0).await.1.unwrap();
    let ids: Vec<_> = snap.reservations.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(
        ids,
        [cancelling.resource.id.as_str()],
        "scheduled and us-east-only are excluded"
    );
    assert_ne!(other_region.resource.id, cancelling.resource.id);
}

#[tokio::test]
async fn snapshot_endpoint_requires_the_regions_token() {
    let (base, _) = spawn_with_svc().await;
    let url = format!("{base}/internal/v1/entitlements/eu-west");
    assert_eq!(client().get(&url).send().await.unwrap().status(), 401);
    assert_eq!(
        client()
            .get(&url)
            .bearer_auth(ACME_KEY)
            .send()
            .await
            .unwrap()
            .status(),
        401,
        "tenant keys don't work"
    );
    assert_eq!(
        client()
            .get(&url)
            .bearer_auth("region-token-us-east-dev")
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
}
