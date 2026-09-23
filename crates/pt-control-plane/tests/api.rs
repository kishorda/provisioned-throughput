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
